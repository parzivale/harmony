pub struct WriteTransaction(pub(crate) redb::WriteTransaction);
pub struct ReadTransaction(pub(crate) redb::ReadTransaction);

pub struct WriteTransactionResult<T> {
    pub(crate) transaction: redb::WriteTransaction,
    pub(crate) result: T,
}
pub struct ReadTransactionResult<T> {
    pub(crate) transaction: redb::ReadTransaction,
    pub(crate) result: Option<T>,
}

impl From<redb::WriteTransaction> for WriteTransaction {
    fn from(value: redb::WriteTransaction) -> Self {
        Self(value)
    }
}

impl From<redb::ReadTransaction> for ReadTransaction {
    fn from(value: redb::ReadTransaction) -> Self {
        Self(value)
    }
}

impl WriteTransaction {
    pub fn finish<T>(self, result: T) -> WriteTransactionResult<T> {
        WriteTransactionResult {
            transaction: self.0,
            result,
        }
    }
}

impl ReadTransaction {
    pub fn finish<T>(self, result: Option<T>) -> ReadTransactionResult<T> {
        ReadTransactionResult {
            transaction: self.0,
            result,
        }
    }
}
