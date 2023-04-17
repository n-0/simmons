use std::{sync::{Arc, Mutex}, collections::{HashMap, HashSet}};

use mr_world_core::proto_type::world::order::OmenOrder;
use simmons_proto::proto_type::advanced_order::AdvancedOrder;

use crate::{types::SimmonsError, oracle::SimmonsShared};

pub type OrderToGroup = HashMap<String, String>;
pub type GroupResolved = HashSet<String>;
// Order group id to bracket with position to orders that take position
// (same position) implies it doesnt matter which one is executed first
pub type GroupIdToGroup = HashMap<String, AdvancedOrder>;

pub fn get_advanced(order: &OmenOrder) -> Result<Vec<AdvancedOrder>, SimmonsError> {
    let special = order.special.as_ref().ok_or(SimmonsError::new_kind("no advanced", crate::types::SimmonsErrorKind::NoAdvanced))?;
    let advanced_orders = serde_json::from_slice::<Vec<AdvancedOrder>>(special.as_slice()).map_err(SimmonsError::new)?;
    if advanced_orders.is_empty() {
        return Err(SimmonsError::new_kind("no advanced", crate::types::SimmonsErrorKind::NoAdvanced))
    }
    Ok(advanced_orders)
}

pub fn is_advanced(order: &OmenOrder) -> bool {
    get_advanced(order).is_ok()
}

pub fn get_group_id(advanced_order: &AdvancedOrder) -> String {
    advanced_order.group_id.clone()
}

pub fn create_or_insert_order_group(
    order: OmenOrder,
    shared: &mut SimmonsShared,
) -> Result<(), SimmonsError> {
    let advanced_orders = get_advanced(&order)?;
    let order_id = order.order_id.ok_or(SimmonsError::custom_new("No order id"))?;
    for advanced_order in advanced_orders {
        let group_id = get_group_id(&advanced_order);
        if !shared.groupid_to_group.contains_key(&group_id) {
            shared.groupid_to_group.insert(group_id.clone(), advanced_order);
        }
        shared.order_to_group.insert(order_id.clone(), group_id);
    }
    Ok(())
}
