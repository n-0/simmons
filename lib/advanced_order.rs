#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdvancedOrder {
    #[prost(enumeration="AdvancedOrderKind", tag="1")]
    pub kind: i32,
    #[prost(string, tag="2")]
    pub group_id: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub parent: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AdvancedOrderKind {
    OneCancelsAll = 0,
    Bracket = 1,
}
impl AdvancedOrderKind {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AdvancedOrderKind::OneCancelsAll => "OneCancelsAll",
            AdvancedOrderKind::Bracket => "Bracket",
        }
    }
}
