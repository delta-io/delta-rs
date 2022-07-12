use deltalake::Schema;
use serde_json::json;

/// Basic schema
pub fn get_xy_date_schema() -> Schema {
    serde_json::from_value(json!({
      "type": "struct",
      "fields": [
        {"name": "x", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "y", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "date", "type": "string", "nullable": false, "metadata": {}},
      ]
    }))
    .unwrap()
}

/// Schema that contains a column prefiexed with _
pub fn get_vacuum_underscore_schema() -> Schema {
    serde_json::from_value::<Schema>(json!({
      "type": "struct",
      "fields": [
        {"name": "x", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "y", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "_date", "type": "string", "nullable": false, "metadata": {}},
      ]
    }))
    .unwrap()
}
