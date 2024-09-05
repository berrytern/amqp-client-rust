pub trait IntoResponse: Send + Sync {
    fn into_response(self) -> Option<Vec<u8>>;
}

impl IntoResponse for () {
    fn into_response(self) -> Option<Vec<u8>> {
        None
    }
}

impl IntoResponse for Vec<u8> {
    fn into_response(self) -> Option<Vec<u8>> {
        Some(self)
    }
}
