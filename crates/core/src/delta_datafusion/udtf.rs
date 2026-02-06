use async_trait::async_trait;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{internal_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::runtime::Runtime;
use url::Url;
use crate::open_table_with_storage_options;

pub fn register_delta_table_udtf(ctx: &SessionContext, name: Option<&str>, settings: Option<&HashMap<String, String>>) {
    let prefix = name
        .or_else(|| Some("delta_table")).unwrap();

    ctx.register_udtf(
        prefix,
        Arc::new(DeltaTableUdtf {
            flavor: DeltaTableUdtfFlavor::Old,
            settings: settings.cloned(),
        }),
    );
    ctx.register_udtf(
        format!("{prefix}_next").as_str(),
        Arc::new(DeltaTableUdtf {
            flavor: DeltaTableUdtfFlavor::Next,
            settings: settings.cloned(),
        }),
    );
}

#[derive(Debug, Clone)]
pub enum DeltaTableUdtfFlavor {
    Old,
    Next
}

#[derive(Debug)]
pub struct DeltaTableUdtf {
    flavor: DeltaTableUdtfFlavor,
    settings: Option<HashMap<String, String>>,
}

#[async_trait]
impl TableFunctionImpl for DeltaTableUdtf {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return Err(DataFusionError::Execution(
                "Delta table function expects table_uri as argument".to_string(),
            ));
        }

        let mut args_string = args
            .iter()
            .map(|arg| match arg.clone() {
                Expr::Literal(ScalarValue::Utf8(Some(path)), _)
                | Expr::Literal(ScalarValue::LargeUtf8(Some(path)), _)
                | Expr::Literal(ScalarValue::Utf8View(Some(path)), _) => Ok(path),
                _ => Err(DataFusionError::Execution(format!(
                    "Unexpected argument type: {:?}",
                    arg
                ))),
            })
            .collect::<Result<VecDeque<_>>>()?;

        let path = args_string
            .pop_front()
            .expect("DeltaTableUdtf missing path");
        assert_eq!(args_string.len() % 2, 0, "DeltaTableUdtf: Can't build hashmap out of odd-sized args");

        let settings = if let Some(global_settings) = &self.settings {
            global_settings.clone()
        } else {
            HashMap::new()
        };

        let flavor = self.flavor.clone();
        let table = std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let table_uri = Url::parse(&path)
                .expect(&format!("Invalid table uri: {}", path));
            rt.block_on(async {
                let delta_table = open_table_with_storage_options(table_uri, settings)
                    .await
                    .map_err(|e| { internal_datafusion_err!("DeltaTableUdtf could not open table at {}: {}",&path,e.to_string()) })
                    .unwrap();

                match flavor {
                    DeltaTableUdtfFlavor::Old => {
                        let provider = delta_table.table_provider_old();
                        Arc::new(provider) as Arc<dyn TableProvider>
                    }
                    DeltaTableUdtfFlavor::Next => {
                        let provider = delta_table
                            .table_provider()
                            .build()
                            .await
                            .unwrap();
                        Arc::new(provider) as Arc<dyn TableProvider>
                    }
                }
            })
        })
        .join()
        .map_err(|e| internal_datafusion_err!("DeltaTableFunc error opening table"))?;

        Ok(Arc::clone(&table))
    }
}
