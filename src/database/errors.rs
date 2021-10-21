use doomstack::Doom;

#[derive(Doom)]
pub enum QueryError {
    #[doom(description("Failed to hash field"))]
    HashError,
    #[doom(description("Key collision within transaction"))]
    KeyCollision,
}

#[derive(Doom, PartialEq, Eq)]
pub enum SyncError {
    #[doom(description("Malformed `Question`"))]
    MalformedQuestion,
    #[doom(description("Malformed `Answer`"))]
    MalformedAnswer,
}
