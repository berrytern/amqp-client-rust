#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmations{
    Disables,
    PublisherConfirms,
    RPCClientPublisherConfirms,
    RPCServerPublisherConfirms,
}
pub enum PendingCmd {
    Ack((u64, bool)),
    Nack((u64, bool)),
}