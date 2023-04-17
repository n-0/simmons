use std::{error::Error, fmt::Display};

use chrono::DateTime;
use mr_world_core::proto_type::world::omen::OmenDirection;
use serde::{Serialize, Deserialize};


#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct InstrumentOrderScenarioMap {
    pub instrument_id: String,
    pub scenario_order_id: String,
    pub scenario_price_id: String
}


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

pub type ExecutedOrders = std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>;

#[derive(Debug, Clone, PartialEq)]
pub enum SimmonsErrorKind {
    NoAdvanced,
    Custom,
    InnerError
}

#[derive(Debug, Clone, PartialEq)]
pub struct SimmonsError {
    pub inner_error: Option<String>,
    pub custom_description: String,
    pub kind: SimmonsErrorKind,
}

impl SimmonsError {
    pub fn new<E: Error>(e: E) -> Self {
        let description = e.description().to_string();
        SimmonsError { 
            inner_error: Some(description), 
            custom_description: "".to_string(),
            kind: SimmonsErrorKind::InnerError
        }
    }

    pub fn custom_new(e: &str) -> Self {
        SimmonsError { 
            inner_error: None, 
            custom_description: e.to_string(),
            kind: SimmonsErrorKind::Custom
        }
    }

    pub fn new_kind(e: &str, kind: SimmonsErrorKind) -> Self {
        SimmonsError { 
            inner_error: None, 
            custom_description: e.to_string(),
            kind
        }
    }
}

impl Display for SimmonsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        //write!(f, self.inner_error.as_str())
        if let Some(inner_error) = self.inner_error.as_ref() {
            f.write_str(&inner_error);
        }
        f.write_str(&self.custom_description)
    }
}
