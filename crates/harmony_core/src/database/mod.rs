use redb::TableDefinition;
pub mod table_collection;
pub mod transaction;
pub trait DatabaseTable: 'static {
    type Error: Send;
    type Key: Send + 'static;
    type Value: Send + 'static;

    const NAME: &'static str;

    #[inline]
    fn definition() -> TableDefinition<'static, &'static [u8], &'static [u8]> {
        const { TableDefinition::new(Self::NAME) }
    }

    fn serialize_key(data: Self::Key) -> Result<Vec<u8>, Self::Error>;

    fn deserialize_key(data: &[u8]) -> Result<Self::Key, Self::Error>;

    fn serialize_value(data: Self::Value) -> Result<Vec<u8>, Self::Error>;

    fn deserialize_value(data: &[u8]) -> Result<Self::Value, Self::Error>;
}
