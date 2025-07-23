use async_trait::async_trait;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{internal_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use itertools::Itertools;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::runtime::Runtime;
use url::Url;
use crate::open_table_with_storage_options;

pub fn register_delta_table_udtf(ctx: &SessionContext, name: Option<&str>, settings: Option<&HashMap<String, String>>) {
    ctx.register_udtf(
        name.or_else(|| Some("delta_table")).unwrap(),
        Arc::new(DeltaTableUdtf {
            settings: settings.cloned(),
        }),
    );
}

#[derive(Debug)]
pub struct DeltaTableUdtf {
    settings: Option<HashMap<String, String>>,
}

#[async_trait]
impl TableFunctionImpl for DeltaTableUdtf {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() < 1 {
            return Err(DataFusionError::Execution(
                "Delta table function expects at least one argument".to_string(),
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
        assert!(
            args_string.len() % 2 == 0,
            "DeltaTableUdtf: Can't build hashmap out of odd-sized args"
        );
        let mut settings = args_string
            .iter()
            .collect::<Vec<_>>()
            .as_slice()
            .chunks(2)
            .map(|l| (l[0].clone(), l[1].clone()))
            .collect::<HashMap<_, _>>();
        if let Some(global_settings) = &self.settings {
            settings.extend(global_settings.clone());
        }

        let table = std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let table_uri = Url::parse(&path)
                .expect(&format!("Invalid table uri: {}", path));
            rt.block_on(async {
                open_table_with_storage_options(table_uri, settings)
                    .await
                    .map_err(|e| {
                        internal_datafusion_err!(
                            "DeltaTableUdtf could not open table at {}: {}",
                            &path,
                            e.to_string()
                        )
                    })
                    .unwrap()
            })
        })
        .join()
        .map_err(|e| internal_datafusion_err!("DeltaTableFunc error opening table"))?;

        Ok(Arc::new(table))
    }
}
