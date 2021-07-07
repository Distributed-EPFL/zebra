#[cfg(test)]
#[macro_use]
mod tests {
    macro_rules! wrap {
        ($item: expr) => {
            crate::database::store::Wrap::new($item).unwrap()
        };
    }

    macro_rules! get {
        ($key: expr) => {
            crate::database::interact::Operation::get(&$key).unwrap()
        };
    }

    macro_rules! set {
        ($key: expr, $value: expr) => {
            crate::database::interact::Operation::set($key, $value).unwrap()
        };
    }

    macro_rules! remove {
        ($key: expr) => {
            crate::database::interact::Operation::remove(&$key).unwrap()
        };
    }
}
