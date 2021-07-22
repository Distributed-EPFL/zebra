use crate::database::sync::ANSWER_DEPTH;

use std::ops::Add;

#[derive(Debug)]
pub(crate) enum Severity {
    Benign(usize),
    Malicious,
}

impl Severity {
    pub(crate) fn ok() -> Self {
        Severity::Benign(0)
    }

    pub(crate) fn benign() -> Self {
        Severity::Benign(1)
    }

    pub(crate) fn malicious() -> Self {
        Severity::Malicious
    }

    pub(crate) fn is_benign(&self) -> bool {
        match self {
            Severity::Benign(..) => true,
            Severity::Malicious => false,
        }
    }

    pub(crate) fn is_malicious(&self) -> bool {
        match self {
            Severity::Benign(..) => false,
            Severity::Malicious => true,
        }
    }
}

impl Add for Severity {
    type Output = Self;

    fn add(self, rho: Self) -> Self {
        match (self, rho) {
            (Severity::Benign(left), Severity::Benign(right)) => {
                let recidivity = left + right;
                if recidivity > (1 << (ANSWER_DEPTH + 1)) - 2 {
                    Severity::Malicious
                } else {
                    Severity::Benign(left + right)
                }
            }
            _ => Severity::Malicious,
        }
    }
}
