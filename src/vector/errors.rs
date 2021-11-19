use doomstack::Doom;

#[derive(Doom)]
pub enum VectorError {
    #[doom(description("Failed to hash item"))]
    HashError,
}
