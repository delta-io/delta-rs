use crate::action::{ActionError, Add, Stats};

impl Add {
    /// Returns the composite HashMap representation of stats contained in the action if present.
    /// Since stats are defined as optional in the protocol, this may be None.
    pub fn get_stats_parsed(&self) -> Result<Option<Stats>, ActionError> {
        Ok(None)
    }
}
