use crate::contracts;

pub struct DefaultRng {}

impl DefaultRng {
    pub fn new() -> Self {
        Self {}
    }
}

impl contracts::Rng for DefaultRng {
    #[tracing::instrument(skip_all)]
    fn random_salt(&self) -> Vec<u8> {
        let mut salt = [0u8; 16];
        rand::fill(&mut salt);
        salt.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::Rng;

    #[test]
    fn salt_is_never_empty() {
        let rng = DefaultRng::new();
        for _ in 0..100 {
            let salt = rng.random_salt();
            assert!(!salt.is_empty());
        }
    }
}
