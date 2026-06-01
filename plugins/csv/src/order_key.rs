#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) struct OrderKey(u128);

#[derive(Clone, Debug)]
pub(crate) struct OrderKeyRange {
    lower: u128,
    step: u128,
    remainder: u128,
    denominator: u128,
    rank: u128,
    count: u128,
}

impl OrderKey {
    pub(crate) fn evenly_between(
        previous: Option<Self>,
        next: Option<Self>,
        count: usize,
    ) -> OrderKeyRange {
        if count == 0 {
            return OrderKeyRange {
                lower: 0,
                step: 0,
                remainder: 0,
                denominator: 1,
                rank: 1,
                count: 0,
            };
        }

        let lower = previous.map_or(0, |index| index.0);
        let upper = next.map_or(u128::MAX, |index| index.0);
        assert!(
            lower <= upper,
            "fractional index bounds are out of order: previous={previous:?}, next={next:?}"
        );

        let gap = upper - lower;
        let count = u128::try_from(count).unwrap();
        assert!(
            count < gap,
            "TODO: fractional index space exhausted between previous={previous:?} and next={next:?}"
        );

        let denominator = count + 1;
        let step = gap / denominator;
        let remainder = gap % denominator;
        OrderKeyRange {
            lower,
            step,
            remainder,
            denominator,
            rank: 1,
            count,
        }
    }

    pub(crate) fn to_snapshot_string(self) -> String {
        format!("{:032x}", self.0)
    }

    pub(crate) fn from_snapshot_string(raw: &str) -> Result<Self, String> {
        if raw.len() != 32 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("must be a 32-character hexadecimal string".to_string());
        }
        if raw.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err("must use lowercase hexadecimal digits".to_string());
        }

        let value = u128::from_str_radix(raw, 16)
            .map_err(|error| format!("must parse as a u128 hexadecimal value: {error}"))?;
        if value == 0 || value == u128::MAX {
            return Err("must be between the reserved lower and upper sentinels".to_string());
        }

        Ok(Self(value))
    }
}

impl Iterator for OrderKeyRange {
    type Item = OrderKey;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rank > self.count {
            return None;
        }

        let rank = self.rank;
        self.rank += 1;
        Some(OrderKey(
            self.lower + self.step * rank + (self.remainder * rank) / self.denominator,
        ))
    }
}
