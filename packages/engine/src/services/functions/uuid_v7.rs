use uuid::Uuid;

pub fn uuid_v7() -> String {
    Uuid::now_v7().to_string()
}
