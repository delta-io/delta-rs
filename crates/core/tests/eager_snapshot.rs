use std::collections::HashMap;

use deltalake_core::{
    kernel::{transaction::CommitData, EagerSnapshot},
    protocol::{DeltaOperation, SaveMode},
    test_utils::add_as_remove,
};
use deltalake_test::utils::*;
use itertools::Itertools;
