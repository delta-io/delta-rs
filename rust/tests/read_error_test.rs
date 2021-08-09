extern crate deltalake;

use std::{env, matches};

#[tokio::test]
async fn read_empty_folder() {
    let dir = env::temp_dir();
    let result = deltalake::open_table(&dir.into_os_string().into_string().unwrap()).await;

    assert!(matches!(
        result.unwrap_err(),
        deltalake::DeltaTableError::NotATable(_),
    ));

    let dir = env::temp_dir();
    let result = deltalake::open_table_with_ds(
        &dir.into_os_string().into_string().unwrap(),
        "2021-08-09T13:18:31+08:00",
    )
    .await;

    assert!(matches!(
        result.unwrap_err(),
        deltalake::DeltaTableError::NotATable(_),
    ));
}
