use doomstack::Doom;

#[derive(Doom)]
pub enum VectorError {
    #[doom(description("Failed to hash item"))]
    HashError,
}

#[derive(Doom)]
pub enum ProofError {
    #[doom(description("Root mismatch"))]
    RootMismatch,
    #[doom(description("Item provided is out of path"))]
    OutOfPath,
    #[doom(description("Incorrectly labled node"))]
    Mislabled,
    #[doom(description("Failed to hash item"))]
    HashError,
    #[doom(description("Item mismatch"))]
    ItemMismatch,
}
