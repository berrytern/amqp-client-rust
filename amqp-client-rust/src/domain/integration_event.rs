pub struct IntegrationEvent {
    pub routing_key: String,
    _exchange_name: String,
}

impl IntegrationEvent {
    pub fn new(routing_key: &str, exchange_name: &str) -> Self {
        Self {
            routing_key: routing_key.to_string(),
            _exchange_name: exchange_name.to_string(),
        }
    }
    fn event_type(&self) -> String {
        self._exchange_name.to_string()
    }
}
