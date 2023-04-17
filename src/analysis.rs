use std::time::SystemTime;
use serde::{Deserialize, Serialize};

use crate::types::LayeredInstrumentTrade;

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Analysis {
    pub finished_trades: Vec<LayeredInstrumentTrade>,
    pub open_trades: Vec<LayeredInstrumentTrade>,
    pub last_price: f64
}

#[derive(Debug, Clone, PartialEq)]
pub struct AnalysisReport {
    pub total_return: f64,
    pub total_number_trades: i64,
    pub total_number_pips: ExtendedDurationInfo, 
    pub total_number_winning_trades: i64,
    pub total_number_loosing_trades: i64,
    pub acc_win: f64,
    pub acc_loss: f64,
    pub largest_winning_trade: f64,
    pub largest_loosing_trade: f64,
    pub average_winning_trade: f64,
    pub average_losing_trade: f64,
    pub average_trade: f64,
    pub ratio_avg_win_loss: f64,
    pub max_position_hold: ExtendedDurationInfo, 
    pub min_position_hold: ExtendedDurationInfo,
    pub max_position_loosing_hold: ExtendedDurationInfo,
    pub max_position_winning_hold: ExtendedDurationInfo,
    pub number_open_trades: i64,
    pub initial_value_open_trades: f64,
    pub current_value_open_trades: f64,
    pub total_return_open_trades: f64,
    pub max_open_position_hold: ExtendedDurationInfo,
    pub min_open_position_hold: ExtendedDurationInfo,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExtendedDurationInfo {
    pub days: i64,
    pub hours: i64,
    pub minutes: i64,
    pub seconds: i64,
    pub milliseconds: i64
}

impl From<chrono::Duration> for ExtendedDurationInfo {
    fn from(duration: chrono::Duration) -> Self {
        ExtendedDurationInfo { 
            days: duration.num_days(), 
            hours: duration.num_hours(),
            minutes: duration.num_minutes(),
            seconds: duration.num_seconds(),
            milliseconds: duration.num_milliseconds()
        }
    }
}

impl From<Analysis> for AnalysisReport {
    fn from(mut analysis: Analysis) -> Self {
        analysis.preparation();
        AnalysisReport { 
            total_return: analysis.total_return(),
            total_number_trades: analysis.total_number_trades(),
            total_number_pips: analysis.total_number_of_pips().unwrap_or(chrono::Duration::milliseconds(0)).into(),
            total_number_winning_trades: analysis.winning_trades().len() as i64,
            total_number_loosing_trades: analysis.loosing_trades().len() as i64,
            acc_win: analysis.acc_win(),
            acc_loss: analysis.acc_loss(),
            largest_winning_trade: analysis.largest_winning_trade(),
            largest_loosing_trade: analysis.largest_loosing_trade(),
            average_winning_trade: analysis.average_winning_trade(),
            average_losing_trade: analysis.average_losing_trade(),
            average_trade: analysis.average_trade(),
            ratio_avg_win_loss: analysis.ratio_avg_win_loss(),
            max_position_hold: analysis.max_position_hold().into(),
            min_position_hold: analysis.min_position_hold().into(),
            max_position_winning_hold: analysis.max_cons_winner().into(),
            max_position_loosing_hold: analysis.max_cons_looser().into(),
            number_open_trades: analysis.total_number_open_trades() as i64,
            initial_value_open_trades: analysis.total_initial_value_open_trades(),
            current_value_open_trades: analysis.total_current_value_open_trades(),
            total_return_open_trades: analysis.total_return_open_trades(),
            max_open_position_hold: analysis.max_open_position_hold().into(),
            min_open_position_hold: analysis.min_open_position_hold().into(),
        }
    }
}

impl Analysis {
    pub fn preparation(&mut self) {
        self.sort_trades_by_execution();
    }

    pub fn total_return(&self) -> f64 {
         self.finished_trades.iter().map(|trade| trade.simple_trade.treturn - 1.0).sum::<f64>() + 1.0
    }

    pub fn total_number_trades(&self) -> i64 {
        self.finished_trades.len() as i64
    }

    pub fn total_number_of_pips(&self) -> Option<chrono::Duration> {
        let start = self.finished_trades.first()?;
        let end = self.finished_trades.last()?;
        let end = end.simple_trade.end?;
        Some(end - start.simple_trade.start)
    }

    pub fn sort_trades_by_execution(&mut self) {
        self.finished_trades.sort_by(|trade1, trade2| trade1.simple_trade.start.cmp(&trade2.simple_trade.end.unwrap()));
    }

    pub fn winning_trades(&self) -> Vec<LayeredInstrumentTrade> {
        self.finished_trades
            .iter()
            .filter(|trade| trade.simple_trade.treturn > 1.0)
            .map(|trade| trade.to_owned())
            .collect()
    }

    pub fn acc_loss(&self) -> f64 {
        self.loosing_trades()
            .iter()
            .map(|trade| trade.simple_trade.treturn)
            .sum()
    }

    pub fn acc_win(&self) -> f64 {
        self.winning_trades()
            .iter()
            .map(|trade| trade.simple_trade.treturn)
            .sum()
    }

    pub fn loosing_trades(&self) -> Vec<LayeredInstrumentTrade> {
        self.finished_trades
            .iter()
            .filter(|trade| trade.simple_trade.treturn < 1.0)
            .map(|trade| trade.to_owned())
            .collect()
    }

    pub fn largest_winning_trade(&self) -> f64 {
        self.finished_trades
            .iter()
            .map(|trade| trade.simple_trade.treturn)
            .fold(0. / 0., f64::max)
    }

    pub fn largest_loosing_trade(&self) -> f64 {
        self.finished_trades
            .iter()
            .map(|trade| trade.simple_trade.treturn)
            .fold(0. / 0., f64::min)
    }

    pub fn average_winning_trade(&self) -> f64 {
        self.acc_win() / (self.total_number_trades() as f64)
    }

    pub fn average_losing_trade(&self) -> f64 {
        self.acc_loss() / (self.total_number_trades() as f64)
    }

    pub fn ratio_avg_win_loss(&self) -> f64 {
        self.average_winning_trade() / self.average_losing_trade()
    }

    pub fn average_trade(&self) -> f64 {
        self.finished_trades.iter().map(|trade| trade.simple_trade.treturn).sum::<f64>() / (self.total_number_trades() as f64)
    }

    pub fn pip_lens(&self) -> Vec<chrono::Duration> {
        self.finished_trades.iter().map(|trade| trade.simple_trade.end.unwrap() - trade.simple_trade.start).collect()
    }

    pub fn pips_loss(&self) -> Vec<chrono::Duration> {
        self.loosing_trades().iter().map(|trade| trade.simple_trade.end.unwrap() - trade.simple_trade.start).collect::<Vec<chrono::Duration>>()
    }

    pub fn pips_win(&self) -> Vec<chrono::Duration> {
        self.winning_trades().iter().map(|trade| trade.simple_trade.end.unwrap() - trade.simple_trade.start).collect::<Vec<chrono::Duration>>()
    }

    /*
    pub fn average_pips_loss(&self) -> f64 {
        self.pips_loss().len() / self.total_number_of_pips()
    }

    pub fn average_pips_win(&self) -> f64 {
        self.pips_win() as f64 / self.total_number_of_pips() as f64
    }
    */

    pub fn max_cons_winner(&self) -> chrono::Duration {
        self.pips_win()
            .iter()
            .max()
            .unwrap_or(&chrono::Duration::milliseconds(0)).to_owned()
    }


    pub fn max_cons_looser(&self) -> chrono::Duration {
        self.pips_loss()
            .iter()
            .max()
            .unwrap_or(&chrono::Duration::milliseconds(0)).to_owned()
    }

    pub fn max_position_hold(&self) -> chrono::Duration {
        self.pip_lens().iter().max().unwrap_or(&chrono::Duration::milliseconds(0)).to_owned()
    }

    pub fn min_position_hold(&self) -> chrono::Duration {
        self.pip_lens().iter().min().unwrap_or(&chrono::Duration::milliseconds(0)).to_owned()
    }

    pub fn total_number_open_trades(&self) -> usize {
        self.open_trades.len()
    }

    pub fn total_initial_value_open_trades(&self) -> f64 {
        self.open_trades.iter().fold(0.0, |acc, trade| acc + trade.initial_price*(trade.position_size as f64))
    }

    pub fn total_current_value_open_trades(&self) -> f64 {
        self.open_trades.iter().fold(0.0, |acc, trade| acc + self.last_price*(trade.position_size as f64))
    }

    pub fn total_return_open_trades(&self) -> f64 {
        self.total_current_value_open_trades() / self.total_initial_value_open_trades()
    }

    pub fn max_open_position_hold(&self) -> chrono::Duration {
        let now = chrono::offset::Utc::now();
        self.open_trades
            .iter()
            .map(|trade| now - trade.simple_trade.start)
            .max()
            .unwrap_or(chrono::Duration::milliseconds(0)).to_owned()
    }

    pub fn min_open_position_hold(&self) -> chrono::Duration {
        let now = chrono::offset::Utc::now();
        self.open_trades
            .iter()
            .map(|trade| now - trade.simple_trade.start)
            .min()
            .unwrap_or(chrono::Duration::milliseconds(0)).to_owned()
    }
}
