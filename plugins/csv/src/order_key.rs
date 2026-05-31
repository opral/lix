use crate::exports::lix::plugin::api::PluginError;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) struct OrderKey(u128);

impl OrderKey {
    pub(crate) fn between(previous: Option<Self>, next: Option<Self>) -> Result<Self, PluginError> {
        let lower = previous.map_or(0, |index| index.0);
        let upper = next.map_or(u128::MAX, |index| index.0);
        assert!(
            lower <= upper,
            "fractional index bounds are out of order: previous={previous:?}, next={next:?}"
        );
        if lower == upper {
            return Err(PluginError::InvalidInput(format!(
                "cannot generate fractional index between identical indexes: {previous:?}"
            )));
        }
        let gap = upper - lower;
        if gap <= 1 {
            return Err(PluginError::InvalidInput(format!(
                "fractional index space exhausted between previous={previous:?} and next={next:?}"
            )));
        }
        Ok(Self(lower + gap / 2))
    }

    #[cfg(test)]
    pub(crate) fn evenly_spaced(offset: usize, len: usize) -> Self {
        let step = u128::MAX / (len as u128 + 1);
        Self(step * (offset as u128 + 1))
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
