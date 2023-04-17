use std::{
    collections::HashMap,
    io::Read,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

use chrono::DateTime;
use mr_world_core::proto_type::world::{
    omen::{Omen, OmenDirection, OmenKind, OmenSeriesItem, ScenarioOmenSeriesItem},
    oracle::oracle_server::Oracle,
    order::{OmenOrder, OmenOrderAction},
    scenario::ScenarioState,
    tick::{BasicTick, Tick, TickKind},
    util::{EmptyMessage, QueryById, RequestResponse},
};
use prost::Message as ProstMessage;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::Consumer,
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tonic::{Code, Request, Response, Status};

use crate::{
    analysis::{Analysis, AnalysisReport},
    group_ops::{
        create_or_insert_order_group, get_advanced, GroupIdToGroup, GroupResolved, OrderToGroup,
    },
    kafka_helper::{CustomContext, LoggingConsumer},
    order_matching::match_orders_with_ticks,
    types::{InstrumentOrderScenarioMap, LayeredInstrumentTrade, OverallPosition},
};

const GROUP: &str = "SIMMONS_GROUP";
const BROKER: &str = "localhost:9092";

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SimmonsState {
    pub instrument_order_pricing: Vec<InstrumentOrderScenarioMap>,
}

#[derive(Debug, Default)]
pub struct SimmonsShared {
    pub instrument_order_pricing: Vec<InstrumentOrderScenarioMap>,
    // instrument ids to (order, timestamp)
    pub pending_orders: HashMap<String, Vec<(OmenOrder, i64)>>,
    // instrument ids to ticks
    pub prices: HashMap<String, Vec<BasicTick>>,
    // triple counting map from oracle/price/order scenario id to instrument id
    pub price_order_to_instrument: HashMap<String, String>,
    // instrument id to trades
    pub open_trades: HashMap<String, Vec<LayeredInstrumentTrade>>,
    pub finished_trades: HashMap<String, Vec<LayeredInstrumentTrade>>,
    // Helper mapper from trade id to LayeredInstrumentTrade
    pub general_trades: HashMap<String, Vec<LayeredInstrumentTrade>>,
    // instrument to all positions (with initial trades)
    pub open_positions: HashMap<String, OverallPosition>,
    pub listeners: HashMap<String, JoinHandle<()>>,
    pub order_to_group: OrderToGroup,
    pub groupid_to_group: GroupIdToGroup,
    pub group_resolved: GroupResolved,
}

#[derive(Debug, Clone, Default)]
pub struct SimmonsOracle {
    pub shared: Arc<Mutex<SimmonsShared>>,
}

#[tonic::async_trait]
impl Oracle for SimmonsOracle {
    async fn update(
        &self,
        request: Request<ScenarioState>,
    ) -> Result<Response<RequestResponse>, Status> {
        let request_state = request.into_inner();
        let params = match serde_json::from_slice::<SimmonsState>(&request_state.state) {
            Ok(params) => params,
            Err(e) => {
                log::error!("Unable to parse parameter state {:#?}", e);
                return Err(tonic::Status::new(
                    Code::Internal,
                    format!("Parsing parameter failed {:#?}", e),
                ));
            }
        };

        Err(Status::new(
            tonic::Code::Internal,
            "Not implemented yet".to_string(),
        ))
    }

    async fn state(&self, request: Request<QueryById>) -> Result<Response<ScenarioState>, Status> {
        Err(Status::new(
            tonic::Code::Internal,
            "Not implemented yet".to_string(),
        ))
    }

    async fn initialize(
        &self,
        request: Request<ScenarioState>,
    ) -> Result<Response<RequestResponse>, Status> {
        let context = CustomContext;
        let consumer: LoggingConsumer = ClientConfig::new()
            .set("group.id", GROUP)
            .set("bootstrap.servers", BROKER)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "50000")
            .set("enable.auto.commit", "true")
            .set("statistics.interval.ms", "30000")
            .set("auto.offset.reset", "smallest")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .expect("Unable to create kafka consumer for recording scenarios, fatal");

        let init_state = request.into_inner();
        let scenario_id = init_state.scenario_id.clone();
        log::info!("Initialized");
        let params = match serde_json::from_slice::<SimmonsState>(&init_state.state) {
            Ok(params) => params,
            Err(e) => {
                log::error!("Unable to parse parameter state {:#?}", e);
                return Err(tonic::Status::new(
                    Code::Internal,
                    format!("Parsing parameter failed {:#?}", e),
                ));
            }
        };
        let mut topics = vec![];
        match self.shared.lock() {
            Ok(mut shared) => {
                for triplet in params.instrument_order_pricing {
                    shared.instrument_order_pricing.push(triplet.clone());
                    topics.push(triplet.scenario_price_id.clone());
                    topics.push(triplet.scenario_order_id.clone());
                    shared
                        .price_order_to_instrument
                        .insert(triplet.scenario_order_id, triplet.instrument_id.clone());
                    shared
                        .price_order_to_instrument
                        .insert(scenario_id.clone(), triplet.instrument_id.clone());
                    shared
                        .price_order_to_instrument
                        .insert(triplet.scenario_price_id, triplet.instrument_id.clone());
                }
            }
            Err(e) => {
                log::error!("Failed to lock price_order_to_instrument {:#?}", e);
            }
        }

        let cloned_scenario_id = scenario_id.clone();
        consumer.subscribe(
            topics
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .as_slice(),
        );
        let shared = self.shared.clone();
        let handle = tokio::task::spawn(async move {
            let topic_stream = consumer.stream().timeout(std::time::Duration::from_secs(5));
            log::info!("Created topics comsumers and producers");
            tokio::pin!(topic_stream);
            let scenario_id = cloned_scenario_id.clone();

            while let Some(Ok(Ok(message))) = topic_stream.next().await {
                let payload = match message.payload_view::<[u8]>() {
                    None => &[],
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        log::error!("Error while deserializing message payload: {:?}", e);
                        &[]
                    }
                };

                let mut received_timestamp: Option<i64> = None;
                let mut received_order: Option<OmenOrder> = None;
                let mut received_tick: Option<BasicTick> = None;
                match ScenarioOmenSeriesItem::decode(payload) {
                    Ok(scenario_omen) => {
                        if let Some(omen) = scenario_omen.omen {
                            received_timestamp = Some(omen.t);
                            if omen.kind == (OmenKind::Order as i32) {
                                received_order = omen.order.clone();
                            }
                            if omen.kind == (OmenKind::Tick as i32) {
                                if let Some(tick) = omen.tick {
                                    // actually basic check needed but other oracle does the
                                    // mistake
                                    received_tick = tick.basic;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("failed to decode kafka message {:#?}", e);
                    }
                }
                let mut instrument_id: Option<String> = None;
                match shared.lock() {
                    Ok(mut shared) => {
                        if let Some(id) = shared.price_order_to_instrument.get(&scenario_id.clone())
                        {
                            instrument_id = Some(id.clone());
                        }
                        if let Some(instrument_id) = instrument_id {
                            if let Some(received_timestamp) = received_timestamp {
                                if let Some(received_order) = received_order {
                                    let new_order = (received_order.clone(), received_timestamp);
                                    match shared.pending_orders.get_mut(&instrument_id) {
                                        Some(pending) => {
                                            pending.push(new_order);
                                            log::info!("Inserted new order");
                                        }
                                        None => {
                                            shared
                                                .pending_orders
                                                .insert(instrument_id.clone(), vec![new_order]);
                                            log::info!(
                                                "Created pending orders for {:#?}",
                                                instrument_id
                                            );
                                        }
                                    }
                                    if get_advanced(&received_order).is_ok() {
                                        if let Err(e) = create_or_insert_order_group(
                                            received_order,
                                            &mut shared
                                        ) {
                                            log::error!("Something went wrong while creating or inserting in order_group {:#?}", e);
                                        }
                                    }
                                }
                            }
                            if let Some(received_tick) = received_tick {
                                match shared.prices.get_mut(&instrument_id) {
                                    Some(prices) => {
                                        prices.push(received_tick);
                                        prices.sort_by(|a, b| a.t.partial_cmp(&b.t).unwrap());
                                        log::info!("Inserted ticks for {:#?}", instrument_id);
                                    }
                                    None => {
                                        shared
                                            .prices
                                            .insert(instrument_id.clone(), vec![received_tick]);
                                        log::info!("Created ticks for {:#?}", instrument_id);
                                    }
                                }
                            }
                        }
                        match_orders_with_ticks(&mut (*shared));
                    }
                    Err(e) => {
                        log::error!("Failed to lock shared {:#?}", e);
                    }
                }
            }
        });

        match self.shared.lock() {
            Ok(mut shared) => {
                shared.listeners.insert(scenario_id, handle);
            }
            Err(e) => {
                log::error!("failed to add listener {:#?}", e);
            }
        }
        Ok(tonic::Response::new(RequestResponse { success: true }))
    }

    async fn default_state(
        &self,
        request: Request<EmptyMessage>,
    ) -> Result<Response<ScenarioState>, Status> {
        match std::env::var("DEFAULT_STATE_PATH") {
            Ok(path) => match std::fs::File::open(path) {
                Ok(mut file) => {
                    let mut content = String::new();
                    file.read_to_string(&mut content);
                    match serde_json::from_str::<SimmonsState>(content.as_str()) {
                        Ok(params) => Ok(tonic::Response::new(ScenarioState {
                            id: "".to_string(),
                            ticker: vec![],
                            scenario_id: "".to_string(),
                            state: serde_json::to_string(&(params)).unwrap().into_bytes(),
                        })),
                        Err(e) => Err(Status::new(tonic::Code::Internal, format!("{:#?}", e))),
                    }
                }
                Err(e) => Err(Status::new(tonic::Code::Internal, format!("{:#?}", e))),
            },
            Err(e) => Err(Status::new(tonic::Code::Internal, format!("{:#?}", e))),
        }
    }

    async fn end(&self, request: Request<QueryById>) -> Result<Response<RequestResponse>, Status> {
        let QueryById { id } = request.into_inner();
        let mut instrument_id = None;
        match self.shared.lock() {
            Ok(mut shared) => {
                    if let Some(listener) = shared.listeners.get_mut(&id) {
                        listener.abort();
                    }
                if let Some(iid) = shared.price_order_to_instrument.get(&id) {
                    instrument_id = Some(iid.clone());
                }

            },
            Err(e) => {
                log::error!("Failed to lock price_order_to_instrument {:#?}", e);
            }
        }
        if instrument_id.is_none() {
            return Err(Status::new(
                tonic::Code::Internal,
                "Failed to find instrument to scenario".to_string(),
            ));
        }

        let instrument_id = instrument_id.unwrap();
        let mut last_price = BasicTick::default();

        let mut cloned_finished_trades = vec![];
        let mut cloned_open_trades = vec![];
        match self.shared.lock() {
            Ok(shared) => {
                if let Some(prices) = shared.prices.get(&instrument_id) {
                    if let Some(lprice) = prices.last() {
                        last_price = lprice.clone();
                    }
                }
                if let Some(finished_trades) = shared.finished_trades.get(&instrument_id) {
                    cloned_finished_trades.extend(finished_trades.clone());
                };
                if let Some(open_trades) = shared.open_trades.get(&instrument_id) {
                    cloned_open_trades.extend(open_trades.clone());
                };
            },
            Err(e) => {
                return Err(Status::new(
                    tonic::Code::Internal,
                    "Failed to lock Simmons.shared".to_string(),
                ));
            }
        }

        for finished_trade in cloned_finished_trades.iter() {
            log::info!("{:#?}", finished_trade);
        }
        let mut analysis = Analysis {
            finished_trades: cloned_finished_trades,
            open_trades: cloned_open_trades,
            last_price: last_price.c,
        };
        let report: AnalysisReport = analysis.into();
        log::info!("\n\n\n\n\n\n\n{:#?}", report);
        return Ok(Response::new(RequestResponse { success: true }));
    }
}
