use doomstack::Doom;

#[derive(Doom)]
pub enum MapError {
    #[doom(description("Failed to hash field"))]
    HashError,
    #[doom(description("Attempted to operate on an unknown branch"))]
    BranchUnknown,
    #[doom(description("Attempted to import incompatible map"))]
    MapIncompatible,
}

#[derive(Doom)]
pub enum TopologyError {
    #[doom(description("Children violate compactness"))]
    CompactnessViolation,
    #[doom(description("Leaf outside of its key path"))]
    PathViolation,
}

#[derive(Doom)]
pub enum DeserializeError {
    #[doom(description("Flawed topology: {}", source))]
    FlawedTopology { source: TopologyError },
}
