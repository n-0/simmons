use std::{sync::{Arc, Mutex, MutexGuard, atomic::{AtomicUsize, Ordering}}, collections::{HashMap, HashSet}};

use chrono::DateTime;
use mr_world_core::proto_type::world::{order::{OmenOrder, OmenOrderAction}, tick::BasicTick, omen::OmenDirection};
use simmons_proto::proto_type::advanced_order::AdvancedOrderKind;

use crate::{types::{LayeredInstrumentTrade, OverallPosition, SimpleTrade, Position, InstrumentTrade, InstrumentOrderScenarioMap, ExecutedOrders, SimmonsError, SimmonsErrorKind}, group_ops::{GroupIdToGroup, OrderToGroup, get_advanced, get_group_id, GroupResolved, is_advanced}, oracle::SimmonsShared};


static COUNTER: AtomicUsize = AtomicUsize::new(1);
fn get_id() -> usize { COUNTER.fetch_add(1, Ordering::Relaxed) }

pub fn is_executable(
    order: OmenOrder, 
    group_resolved: GroupResolved,
    to_remove: &mut Vec<usize>,
    index: usize
) -> Result<bool, SimmonsError> {
    let advanced = get_advanced(&order);
    if let Err(adv) = advanced {
        if adv.kind == SimmonsErrorKind::NoAdvanced {
            return Ok(true)
        }
        return Err(adv);
    }
    let advanced = advanced.unwrap();
    for advanced_order in advanced {
        let group_id = get_group_id(&advanced_order);
        let resolved = group_resolved.contains(&group_id);
        match advanced_order.kind() {
            AdvancedOrderKind::OneCancelsAll => {
                if resolved { 
                    to_remove.push(index);
                    return Ok(false)
                }
                return Ok(true)
            },
            AdvancedOrderKind::Bracket => {
                if !resolved { return Ok(false) }
            },
        }
    }
    Ok(true)
}

pub fn execution_group_update(
    order_id: String,
    order_to_group: &mut OrderToGroup, 
    groupid_to_group: &mut GroupIdToGroup,
    mut group_resolved: &mut GroupResolved,
) -> Result<(), SimmonsError> {
    let group_id = order_to_group.get(&order_id).ok_or(SimmonsError::custom_new("no group id"))?;
    let group = groupid_to_group.get(group_id).ok_or(SimmonsError::custom_new("no group for group id"))?;
    if group_resolved.contains(group_id) { return Ok(()) }
    match group.kind() {
        AdvancedOrderKind::OneCancelsAll => {
            group_resolved.insert(group_id.clone());
        },
        AdvancedOrderKind::Bracket => {
            if order_id == group.parent {
                group_resolved.insert(group_id.clone());
            }
        },
    }
    Ok(())
}

pub fn match_orders_with_ticks(shared: &mut SimmonsShared) -> Option<()> {
    for instrument in &(shared.instrument_order_pricing) {
        let mut to_remove: Vec<usize> = vec![];
        let instrument_orders = (shared.pending_orders).get_mut(&instrument.instrument_id)?;
        for (index, (order, time)) in instrument_orders.clone().iter().enumerate() {
            let is_exec = is_executable(order.clone(), shared.group_resolved.clone(), &mut to_remove, index);
            if is_exec.is_err() {
                log::error!("Failed to check executability {:#?}", is_exec);
                continue;
            }
            if Ok(false) == is_exec {
                continue;
            }
            let order_action = order.action();
            let time_chrono = DateTime::<chrono::Utc>::from_utc(chrono::NaiveDateTime::from_timestamp(*time, 0), chrono::Utc);
            let instrument_ticks = shared.prices.get(&instrument.instrument_id)?.clone();
            let mut valid_ticks = instrument_ticks.iter().filter(|tick| { tick.t >= *time });
            let trade_id = get_id().to_string();
            match order_action {
                OmenOrderAction::Buy => {
                    let order_limit = order.limit.unwrap();
                    let order_execution_tick = valid_ticks.find(|tick: &&BasicTick| { tick.c <= order_limit });
                    if order_execution_tick.is_none() {
                        continue;
                    }
                    let order_execution_tick = order_execution_tick.unwrap();
                    let trade = LayeredInstrumentTrade { 
                        id: trade_id.clone(), 
                        simple_trade: SimpleTrade { 
                            start: time_chrono, 
                            end: None, 
                            treturn: 0.0, 
                            direction: OmenDirection::Up
                        }, 
                        instrument: order.instrument.clone(), 
                        initial_price: order_execution_tick.c, 
                        final_price: None, 
                        position_size: order.quantity as u64, 
                        partial_trades: vec![] 
                    };
                    match shared.open_trades.get_mut(&order.instrument.clone(), ) {
                        Some(instrument_open_trades) => {
                            instrument_open_trades.push(trade)
                        },
                        None => {
                            let ts = vec![trade];
                            shared.open_trades.insert(order.instrument.clone(), ts);
                        },
                    }

                    let new_position = Position { 
                        instrument_id: order.instrument.clone(), 
                        trade_id: trade_id.clone() 
                    };

                    match shared.open_positions.get_mut(&instrument.instrument_id) {
                        Some(open_positions) => {
                            open_positions.positions.push(new_position)
                        },
                        None => {
                            shared.open_positions.insert(
                                order.instrument.clone(), 
                                OverallPosition { 
                                    instrument_id: order.instrument.clone(), 
                                    positions: vec![new_position] 
                                }
                            );
                        },
                    }
                    if is_advanced(order) {
                        if let Some(order_id) = order.order_id.clone() {
                            if let Err(e) = execution_group_update(order_id, &mut shared.order_to_group, &mut shared.groupid_to_group, &mut shared.group_resolved) {
                                log::error!("There was an issue updating the order group {:#?}", e)
                            }
                        }
                    }
                    to_remove.push(index);
                },
                OmenOrderAction::Sell => {
                    log::info!("received sell going into logic");
                    // TODO remove before pushing, as this only true for your current strategy
                    let mut order_execution_tick: Option<&BasicTick> = None;
                    if let Some(limit) = order.limit {
                        order_execution_tick = valid_ticks.find(|tick: &&BasicTick| { tick.c >= limit });
                    }
                    if let Some(stop) = order.stop {
                        if let Some(limit) = order.limit {
                            order_execution_tick = valid_ticks.find(|tick: &&BasicTick| { tick.c >= limit && tick.c <= stop });
                        }
                    }
                    if order_execution_tick.is_none() {
                        continue;
                    }
                    let order_execution_tick = order_execution_tick.unwrap();
                    let time_chrono = DateTime::<chrono::Utc>::from_utc(chrono::NaiveDateTime::from_timestamp(order_execution_tick.t, 0), chrono::Utc);
                    sell_logic(
                        &mut shared.open_positions, 
                        &mut shared.finished_trades, 
                        &mut shared.open_trades,
                        time_chrono,
                        order_execution_tick.clone(),
                        order.clone()
                    );
                    if is_advanced(order) {
                        if let Some(order_id) = order.order_id.clone() {
                            if let Err(e) = execution_group_update(order_id, &mut shared.order_to_group, &mut shared.groupid_to_group, &mut shared.group_resolved) {
                                log::error!("There was an issue updating the order group {:#?}", e)
                            }
                        }
                    }
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
    mutex_open_positions: &mut HashMap<String, OverallPosition>,
    mutex_finished_trades: &mut HashMap<String, Vec<LayeredInstrumentTrade>>,
    mutex_open_trades: &mut HashMap<String, Vec<LayeredInstrumentTrade>>,
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
