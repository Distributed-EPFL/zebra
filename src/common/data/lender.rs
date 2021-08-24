use std::mem;

pub(crate) struct Lender<Inner> {
    state: State<Inner>,
}

enum State<Inner> {
    Available(Inner),
    Lent,
}

impl<Inner> Lender<Inner> {
    pub fn new(inner: Inner) -> Self {
        Lender {
            state: State::Available(inner),
        }
    }

    pub fn take(&mut self) -> Inner {
        let mut state = State::Lent;
        mem::swap(&mut self.state, &mut state);

        match state {
            State::Available(inner) => inner,
            State::Lent => panic!("attempted to `Lender::take` more than once without `Lender::restore`")
        }
    }

    pub fn restore(&mut self, inner: Inner) {
        if let State::Lent = self.state {
            self.state = State::Available(inner);
        } else {
            panic!("attempted to `Lender::restore` more than once without `Lender::take`");
        }
    }
}

impl<Inner> Drop for Lender<Inner> {
    fn drop(&mut self) {
        if let State::Lent = self.state {
            panic!(
                "dropping `Lender` without previously `Lender::restore`ing it"
            );
        }
    }
}
