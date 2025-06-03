pub struct WriteTransaction(pub(crate) redb::WriteTransaction);
pub struct ReadTransaction(pub(crate) redb::ReadTransaction);

pub struct WriteTransactionResult<T>(pub(crate) redb::WriteTransaction, pub(crate) T);
pub struct ReadTransactionResult<T>(pub(crate) redb::ReadTransaction, pub(crate) T);

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
        WriteTransactionResult(self.0, result)
    }
}

impl ReadTransaction {
    pub fn finish<T>(self, result: T) -> ReadTransactionResult<T> {
        ReadTransactionResult(self.0, result)
    }
}
