use std::{sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}, MutexGuard}, collections::HashMap, io::Read};

use chrono::DateTime;
use mr_world_core::proto_type::world::{
    scenario::ScenarioState, 
    util::{RequestResponse, QueryById, EmptyMessage}, 
    omen::{ScenarioOmenSeriesItem, OmenDirection, OmenSeriesItem, Omen, OmenKind}, 
    order::{OmenOrder, OmenOrderAction}, 
    oracle::oracle_server::Oracle, 
    tick::{Tick, BasicTick, TickKind}
};
use prost::Message as ProstMessage;
use rdkafka::{producer::{FutureRecord, FutureProducer}, ClientConfig, config::RDKafkaLogLevel, consumer::Consumer, Message};
use serde::{Serialize, Deserialize};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Code};

use crate::{kafka_helper::{CustomContext, LoggingConsumer}, analysis::{Analysis, AnalysisReport}};

static COUNTER: AtomicUsize = AtomicUsize::new(1);
fn get_id() -> usize { COUNTER.fetch_add(1, Ordering::Relaxed) }

const GROUP: &str = "SIMMONS_GROUP";
const BROKER: &str = "localhost:9092";

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct OverallPosition {
    pub instrument_id: String,
    // must be sorted by date
    pub positions: Vec<Position>
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Position {
    pub instrument_id: String,
    pub trade_id: String
}

/* A simple trade
 * doesnt have an amount of units
 *nor exact prices
 */ 
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SimpleTrade {
    pub start: DateTime<chrono::Utc>,
    pub end: Option<DateTime<chrono::Utc>>,
    // percentage
    pub treturn: f64,
    // profit would be obtained via Long/Short
    pub direction: OmenDirection,
}

/**
 * An InstrumentTrade includes the units
 * as well as the exact prices that were sold
 */
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct InstrumentTrade {
    pub simple_trade: SimpleTrade,
    pub instrument: String,
    pub start_price: f64,
    pub end_price: f64,
    pub position_size: u64
}


/**
 * A layered instrument trade allows for partial selling
 * of positions, as an example
 * One buys 10 (22$ per piece) stocks and locks in
 * an initial profit of 4$, buy selling 2 stocks at 24$,
 * later on one makes a loss of 3$ at 19$ buy selling 1 stock.
 * Finally the last 7 are sold at 25$ with a profit of 21$,
 * giving an overall profit of 22$.
 */
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct LayeredInstrumentTrade {
    pub id: String,
    pub simple_trade: SimpleTrade,
    pub instrument: String,
    pub initial_price: f64,
    pub final_price: Option<f64>,
    pub position_size: u64,
    pub partial_trades: Vec<InstrumentTrade> 
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct InstrumentOrderScenarioMap {
    pub instrument_id: String,
    pub scenario_order_id: String,
    pub scenario_price_id: String
}


#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SimmonsState {
    pub instrument_order_pricing: Vec<InstrumentOrderScenarioMap>,
}

#[derive(Debug, Clone, Default)]
pub struct SimmonsOracle {
    pub instrument_order_pricing: Arc<Mutex<Vec<InstrumentOrderScenarioMap>>>,
    // instrument ids to (order, timestamp)
    pub pending_orders: Arc<Mutex<HashMap<String, Vec<(OmenOrder, i64)>>>>,
    // instrument ids to ticks
    pub prices: Arc<Mutex<HashMap<String, Vec<BasicTick>>>>,
    // triple counting map from oracle/price/order scenario id to instrument id
    pub price_order_to_instrument: Arc<Mutex<HashMap<String, String>>>,
    // instrument id to trades        
    pub open_trades: Arc<Mutex<HashMap<String, Vec<LayeredInstrumentTrade>>>>,
    pub finished_trades: Arc<Mutex<HashMap<String, Vec<LayeredInstrumentTrade>>>>,
    // Helper mapper from trade id to LayeredInstrumentTrade
    pub general_trades: Arc<Mutex<HashMap<String, Vec<LayeredInstrumentTrade>>>>,
    // instrument to all positions (with initial trades)
    pub open_positions: Arc<Mutex<HashMap<String, OverallPosition>>>,
    pub listeners: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

#[tonic::async_trait]
impl Oracle for SimmonsOracle {

    async fn update(
        &self,
        request: Request<ScenarioState> ,
    ) -> Result<Response<RequestResponse> , Status> {
        let request_state = request.into_inner();
        let params = match serde_json::from_slice::<SimmonsState>(&request_state.state) {
            Ok(params) => {
                params
            },
            Err(e) => {
                log::error!("Unable to parse parameter state {:#?}", e);
                return Err(tonic::Status::new(Code::Internal, format!("Parsing parameter failed {:#?}", e)));
            },
        };

        Err(Status::new(
            tonic::Code::Internal,
            "Not implemented yet".to_string(),
        ))
    }

    async fn state(
        &self,
        request: Request<QueryById>,
    ) -> Result<Response<ScenarioState>, Status> {
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
        let price_order_to_instrument = self.price_order_to_instrument.clone();  
        log::info!("Initialized");
        let params = match serde_json::from_slice::<SimmonsState>(&init_state.state) {
            Ok(params) => {
                params
            },
            Err(e) => {
                log::error!("Unable to parse parameter state {:#?}", e);
                return Err(tonic::Status::new(Code::Internal, format!("Parsing parameter failed {:#?}", e)));
            },
        };
        let mut topics = vec![];
        match self.price_order_to_instrument.lock() {
            Ok(mut price_order_to_instrument) => {
                match self.instrument_order_pricing.lock() {
                    Ok(mut instrument_order_pricing) => {
                        for triplet in params.instrument_order_pricing {
                            instrument_order_pricing.push(triplet.clone());
                            topics.push(triplet.scenario_price_id.clone());
                            topics.push(triplet.scenario_order_id.clone());
                            price_order_to_instrument.insert(triplet.scenario_order_id, triplet.instrument_id.clone());
                            price_order_to_instrument.insert(scenario_id.clone(), triplet.instrument_id.clone());
                            price_order_to_instrument.insert(triplet.scenario_price_id, triplet.instrument_id.clone());
                        }
                    },
                    Err(e) => {
                        log::error!("Failed to lock instrument_order_pricing {:#?}", e);
                    },
                }
            },
            Err(e) => {
                    log::error!("Failed to lock price_order_to_instrument {:#?}", e);
            }
        }

        let pending_orders = self.pending_orders.clone();
        let prices = self.prices.clone();
        let open_positions = self.open_positions.clone();
        let open_trades = self.open_trades.clone();
        let finished_trades = self.finished_trades.clone();
        let instrument_order_pricing = self.instrument_order_pricing.clone();
        let cloned_scenario_id = scenario_id.clone();

        consumer.subscribe(topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>().as_slice());
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
                                if let Some(tick) =  omen.tick {
                                    // actually basic check needed but other oracle does the
                                    // mistake
                                    received_tick = tick.basic;
                                }
                            }
                        }
                    },
                    Err(e) => {
                        log::error!("failed to decode kafka message {:#?}", e);
                    }
                }
                let mut instrument_id: Option<String> = None;
                match price_order_to_instrument.lock() {
                    Ok(price_order_to_instrument) => {
                        if let Some(id) = price_order_to_instrument.get(&scenario_id.clone()) {
                            instrument_id = Some(id.clone());
                        }
                        },
                        Err(e) => {
                            log::error!("failed to lock to find instrument id for order scenario {:#?}", e);
                        },
                        }
                        if let Some(instrument_id) = instrument_id {
                            if let Some(received_timestamp) = received_timestamp {
                                if let Some(received_order) = received_order {
                                    let new_order = (received_order, received_timestamp);
                                    match pending_orders.lock() {
                                        Ok(mut pending) => {
                                            match pending.get_mut(&instrument_id) {
                                                Some(pending) => {
                                                    pending.push(new_order);
                                                    log::info!("Inserted new order");
                                                },
                                                None => {
                                                    pending.insert(instrument_id.clone(), vec![new_order]);
                                                    log::info!("Created pending orders for {:#?}", instrument_id);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            log::error!("failed to lock pending_orders {:#?}", e);
                                        },
                                    }
                                }
                            }
                            if let Some(received_tick) = received_tick {
                                match prices.lock() {
                                    Ok(mut prices) => {
                                        match prices.get_mut(&instrument_id) {
                                            Some(prices) => {
                                                prices.push(received_tick);
                                                prices.sort_by(|a, b| a.t.partial_cmp(&b.t).unwrap());
                                                log::info!("Inserted ticks for {:#?}", instrument_id);
                                            },
                                            None => {
                                                prices.insert(instrument_id.clone(), vec![received_tick]);
                                                log::info!("Created ticks for {:#?}", instrument_id);
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        log::error!("failed to lock prices {:#?}", e);
                                    },
                                }
                            }
                        }
                match_orders_with_ticks(open_trades.clone(), finished_trades.clone(), open_positions.clone(), pending_orders.clone(), instrument_order_pricing.clone(), prices.clone());
            } 
        });

        match self.listeners.lock() {
            Ok(mut listeners) => {
                listeners.insert(scenario_id, handle);
            },
            Err(e) => {
                log::error!("failed to add listener {:#?}", e);
            },
        }
        Ok(tonic::Response::new(RequestResponse { success: true }))
    }


    async fn default_state(
        &self,
        request: Request<EmptyMessage>,
    ) -> Result<Response<ScenarioState>, Status> {
        match std::env::var("DEFAULT_STATE_PATH") {
            Ok(path) => {
                match std::fs::File::open(path) {
                    Ok(mut file) => {
                        let mut content = String::new();
                        file.read_to_string(&mut content);
                        match serde_json::from_str::<SimmonsState>(content.as_str()) {
                            Ok(params) => {
                                Ok(tonic::Response::new(ScenarioState {
                                    id: "".to_string(),
                                    ticker: vec![],
                                    scenario_id: "".to_string(),
                                    state: serde_json::to_string(&(params)).unwrap().into_bytes() 
                                }))
                            },
                            Err(e) => {
                                Err(Status::new(
                                        tonic::Code::Internal,
                                        format!("{:#?}", e),
                                ))
                            },
                        }
                    },
                    Err(e) => {
                        Err(Status::new(
                                tonic::Code::Internal,
                                format!("{:#?}", e),
                        ))
                    },
                }
            },
            Err(e) => {
                Err(Status::new(
                        tonic::Code::Internal,
                        format!("{:#?}", e),
                ))
            },
        }
    }


    async fn end(
        &self,
        request: Request<QueryById>,
    ) -> Result<Response<RequestResponse>, Status> {
        let QueryById { id } = request.into_inner();
        match self.listeners.lock() {
            Ok(mut listeners) => {
                if let Some(listener) = listeners.get_mut(&id) {
                    listener.abort();
                }
            },
            Err(e) => {
                log::error!("Failed to lock listeners to stop scenario listening {:#?}", e);
            },
        }
        let mut instrument_id = None;
        match self.price_order_to_instrument.lock() {
            Ok(price_order_to_instrument) => {
                if let Some(iid) = price_order_to_instrument.get(&id) {
                    instrument_id = Some(iid.clone());
                }
            },
            Err(e) => {
                log::error!("Failed to lock price_order_to_instrument {:#?}", e);
            },
        }
        if instrument_id.is_none() {
            return Err(Status::new(
                    tonic::Code::Internal,
                    "Failed to find instrument to scenario".to_string(),
            ));
        }

        let instrument_id = instrument_id.unwrap();
        let mut last_price = BasicTick::default();

        match self.prices.lock() {
            Ok(prices) => {
                if let Some(prices) = prices.get(&instrument_id) {
                    if let Some(lprice) = prices.last() {
                        last_price = lprice.clone();
                    }
                }
            },
            Err(e) => {
                log::error!("Failed to lock price_order_to_instrument {:#?}", e);
            },
        }

        let mut cloned_finished_trades = vec![];
        match self.finished_trades.lock() {
            Ok(finished_trades) => {
                if let Some(finished_trades) = finished_trades.get(&instrument_id) {
                    cloned_finished_trades.extend(finished_trades.clone());
                };
            },
            Err(e) => {
                log::error!("{:#?}", e);
            },
        }
        let mut cloned_open_trades = vec![];
        match self.open_trades.lock() {
            Ok(open_trades) => {
                if let Some(open_trades) = open_trades.get(&instrument_id) {
                    cloned_open_trades.extend(open_trades.clone());
                };
            },
            Err(e) => {
                log::error!("{:#?}", e);
            },
        }

        /*
        for trade in cloned_finished_trades {
            log::info!("Finished trades ran: {:#?}", trade);
        }
        for trade in cloned_open_trades {
            log::info!("Still open trades: {:#?}", trade);
        }
        */
        let mut analysis = Analysis { finished_trades: cloned_finished_trades, open_trades: cloned_open_trades, last_price: last_price.c };
        let report: AnalysisReport = analysis.into();
        log::info!("{:#?}", report);
        return Ok(Response::new(RequestResponse { success: true }))
    }
}


pub fn match_orders_with_ticks(
    open_trades: Arc<Mutex<HashMap<String, Vec<LayeredInstrumentTrade>>>>,
    finished_trades: Arc<Mutex<HashMap<String, Vec<LayeredInstrumentTrade>>>>,
    open_positions: Arc<Mutex<HashMap<String, OverallPosition>>>,
    orders: Arc<Mutex<HashMap<String, Vec<(OmenOrder, i64)>>>>, 
    instrument_order_pricing: Arc<Mutex<Vec<InstrumentOrderScenarioMap>>>,
    ticks: Arc<Mutex<HashMap<String, Vec<BasicTick>>>>
) -> Option<()> {
    let mut instruments: Option<MutexGuard<Vec<InstrumentOrderScenarioMap>>> = None;
    let mut mutex_orders: Option<MutexGuard<HashMap<String, Vec<(OmenOrder, i64)>>>> = None;
    let mut mutex_ticks: Option<MutexGuard<HashMap<String, Vec<BasicTick>>>> = None;
    let mut mutex_open_positions: Option<MutexGuard<HashMap<String, OverallPosition>>> = None;
    let mut mutex_finished_trades: Option<MutexGuard<HashMap<String, Vec<LayeredInstrumentTrade>>>> = None;
    let mut mutex_open_trades: Option<MutexGuard<HashMap<String, Vec<LayeredInstrumentTrade>>>> = None;

    match instrument_order_pricing.lock() {
        Ok(ins) => {
            instruments = Some(ins)
        },
        Err(e) => {
            log::error!("Failed to lock instrument for order matching {:?}", e);
            return None;
        },
    }
    match open_trades.lock() {
        Ok(open_trades) => {
            mutex_open_trades = Some(open_trades)
        },
        Err(e) => {
            log::error!("Failed to lock instrument for order matching {:?}", e);
            return None;
        },
    }
    match finished_trades.lock() {
        Ok(finished_trades) => {
            mutex_finished_trades = Some(finished_trades)
        },
        Err(e) => {
            log::error!("Failed to lock instrument for order matching {:?}", e);
            return None;
        },
    }
    match orders.lock() {
        Ok(orders) => {
            mutex_orders = Some(orders);
        },
        Err(e) => {
            log::error!("Failure to lock orders for order matching {:#?}", e);
            return None;
        },
    }
    match ticks.lock() {
        Ok(ticks) => {
            mutex_ticks = Some(ticks);
        },
        Err(e) => {
            log::error!("Failure to lock ticks for order matching {:#?}", e);
            return None;
        },
    }
    match open_positions.lock() {
        Ok(open_positions) => {
            mutex_open_positions = Some(open_positions);
        },
        Err(e) => {
            log::error!("Failure to lock ticks for order matching {:#?}", e);
            return None;
        },
    }
    let mut instruments = instruments?;
    let mut mutex_orders = mutex_orders?;
    let mut mutex_ticks = mutex_ticks?;
    let mut mutex_open_positions = mutex_open_positions?;
    let mut mutex_open_trades = mutex_open_trades?;
    let mut mutex_finished_trades = mutex_finished_trades?;
    for instrument in &(*instruments) {
        let mut to_remove: Vec<usize> = vec![];
        let instrument_orders = (*mutex_orders).get_mut(&instrument.instrument_id)?;
        for (index, (order, time)) in instrument_orders.clone().iter().enumerate() {
            let order_action = order.action();
            let order_limit = order.limit.unwrap();
            //let nanoseconds = substring::Substring::substring(time.to_string().as_str(), 11, 3).parse::<u32>().unwrap() * 1000000;
            //let seconds = substring::Substring::substring(time.to_string().as_str(), 1, 10).parse::<i64>().unwrap();
            let time_chrono = DateTime::<chrono::Utc>::from_utc(chrono::NaiveDateTime::from_timestamp(*time, 0), chrono::Utc);
            log::debug!("Chrono parsed {:#?}", time_chrono);
            let instrument_ticks = mutex_ticks.get(&instrument.instrument_id)?.clone();
            let mut valid_ticks = instrument_ticks.iter().filter(|tick| { tick.t >= *time });
            let trade_id = get_id().to_string();
            match order_action {
                OmenOrderAction::Buy => {
                    let order_execution_tick = valid_ticks.find(|tick: &&BasicTick| { tick.c <= order_limit })?;
                    let trade = LayeredInstrumentTrade { 
                        id: trade_id.clone(), 
                        simple_trade: SimpleTrade { 
                            start: time_chrono, 
                            end: None, 
                            //pips: std::time::Duration::new(0, 0),
                            treturn: 0.0, 
                            direction: OmenDirection::Up
                        }, 
                        instrument: order.instrument.clone(), 
                        initial_price: order_execution_tick.c, 
                        final_price: None, 
                        position_size: order.quantity as u64, 
                        partial_trades: vec![] 
                    };
                    match mutex_open_trades.get_mut(&order.instrument.clone(), ) {
                        Some(instrument_open_trades) => {
                            instrument_open_trades.push(trade)
                        },
                        None => {
                            let ts = vec![trade];
                            mutex_open_trades.insert(order.instrument.clone(), ts);
                        },
                    }

                    let new_position = Position { 
                        instrument_id: order.instrument.clone(), 
                        trade_id: trade_id.clone() 
                    };

                    match mutex_open_positions.get_mut(&instrument.instrument_id) {
                        Some(open_positions) => {
                            open_positions.positions.push(new_position)
                        },
                        None => {
                            mutex_open_positions.insert(
                                order.instrument.clone(), 
                                OverallPosition { 
                                    instrument_id: order.instrument.clone(), 
                                    positions: vec![new_position] 
                                }
                            );
                        },
                    }

                    to_remove.push(index);
                },
                OmenOrderAction::Sell => {
                    log::info!("received sell going into logic");
                    let order_execution_tick = valid_ticks.find(|tick: &&BasicTick| { tick.c >= order_limit })?;
                    let time_chrono = DateTime::<chrono::Utc>::from_utc(chrono::NaiveDateTime::from_timestamp(order_execution_tick.t, 0), chrono::Utc);
                    sell_logic(
                        &mut mutex_open_positions, 
                        &mut mutex_finished_trades, 
                        &mut mutex_open_trades,
                        time_chrono,
                        order_execution_tick.clone(),
                        order.clone()
                    );
                    to_remove.push(index);
                },
            }
        }
        for r in to_remove {
            instrument_orders.remove(r);
        }
    }
    None
}

/** 
 * This is sorta of complicated and also
 * called lot matching or cost basis method.
 * (It's a general challenge of accounting)
 * The question is often asked in the context
 * of taxes and I have to admit, that I'm confused
 * why it isn't mentioned while talking about profits.
 * Probably because most brokers fall back to FIFO.
 * Possible (not all supported) methods are
 * FIFO (sell orders are matched with the earliest Buy)
 * LIFO (sell orders are matched with the latest buy)
 * HIFO (sell orders are matched such that the loss is maxed)
 * High gain/Profit (sell orders are matched such that the profit is maxed)
 * Additionally one can turn knobs in regards to short/long term gain/loss,
 * which is equivalent to first sort orders by LIFO/FIFO and then applying
 * HIFO/High gain
 * Also specific lot by id matching exists but is only offered by highly specialized
 * brokers.
 */
pub fn sell_logic(
    mutex_open_positions: &mut MutexGuard<HashMap<String, OverallPosition>>,
    mutex_finished_trades: &mut MutexGuard<HashMap<String, Vec<LayeredInstrumentTrade>>>,
    mutex_open_trades: &mut MutexGuard<HashMap<String, Vec<LayeredInstrumentTrade>>>,
    time_chrono: chrono::DateTime<chrono::Utc>,
    order_execution_tick: BasicTick,
    order: OmenOrder
) -> Option<()> {
    let mut leftover = order.quantity as u64;
    let overall_position = mutex_open_positions.get_mut(&order.instrument.clone())?;
    let mut to_remove: Vec<usize> = vec![];
    for (index, position) in overall_position.positions.iter_mut().enumerate() {
        let open_trade_id = position.trade_id.clone();
        let trades = mutex_open_trades.get_mut(&order.instrument).unwrap();
        let mut specific_trade_index = trades.iter_mut().position(|tr| tr.id == open_trade_id).unwrap();
        let mut specific_trade = &mut trades[specific_trade_index];
        match specific_trade.position_size.cmp(&leftover) {
            std::cmp::Ordering::Less|std::cmp::Ordering::Equal => {
                log::info!("Matched order");
                let partial_trade = InstrumentTrade { 
                    simple_trade: SimpleTrade { 
                        start: specific_trade.simple_trade.start, 
                        end: Some(time_chrono), 
                        treturn: (order_execution_tick.c - specific_trade.initial_price ) / specific_trade.initial_price, 
                        direction: OmenDirection::Up
                    }, 
                    instrument: order.instrument.clone(), 
                    start_price: specific_trade.initial_price, 
                    end_price: order_execution_tick.c, 
                    position_size: leftover
                };
                leftover -= specific_trade.position_size;
                specific_trade.partial_trades.push(partial_trade);
                specific_trade.final_price = Some(order_execution_tick.c);
                specific_trade.simple_trade.end = Some(time_chrono);
                // calculating overall profit via partial trades
                let mut total_return = 0f64;
                for partial_trade in specific_trade.partial_trades.iter() {
                    total_return += (partial_trade.end_price - partial_trade.start_price)*(partial_trade.position_size as f64);
                }

                specific_trade.simple_trade.treturn = 1f64 + (total_return/(specific_trade.initial_price*(specific_trade.position_size as f64)));
                // short would be the other way arounud
                //specific_trade.simple_trade.treturn = order_execution_tick.c / specific_trade.initial_price;
                log::info!("Finalized trade {:#?}", specific_trade);
                match mutex_finished_trades.get_mut(&order.instrument) {
                    Some(f_trades) => {
                        f_trades.push(specific_trade.clone());
                    },
                    None => {
                        mutex_finished_trades.insert(order.instrument.clone(), vec![specific_trade.clone()]);
                    }
                }
                trades.remove(specific_trade_index);
                to_remove.push(index);
            }
            std::cmp::Ordering::Greater => {
                log::info!("Partially matched order");
                let partial_trade = InstrumentTrade { 
                    simple_trade: SimpleTrade { 
                        start: specific_trade.simple_trade.start, 
                        end: Some(time_chrono), 
                        treturn: (order_execution_tick.c - specific_trade.initial_price) / specific_trade.initial_price, 
                        direction: OmenDirection::Up
                    }, 
                    instrument: order.instrument.clone(), 
                    start_price: specific_trade.initial_price, 
                    end_price: order_execution_tick.c, 
                    position_size: leftover
                };
                specific_trade.position_size -= leftover;
                log::info!("partially finalized trade {:#?}", specific_trade);
                specific_trade.partial_trades.push(partial_trade);
                leftover = 0;
            }
        }
    }
    for r in to_remove {
        overall_position.positions.remove(r);
    }
    if leftover > 0 {
        log::error!("No shorting supported at the moment, 
            still trying to sell {:#?} units of {:#?}, 
            but there are no positions left", leftover, order.instrument.clone());
    }
    None
}
