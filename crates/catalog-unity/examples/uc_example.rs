use datafusion::prelude::*;
use deltalake_catalog_unity::prelude::*;
use std::error::Error;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filter = tracing_subscriber::EnvFilter::builder().parse("deltalake_catalog_unity=info")?;
    let subscriber = tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let uc = UnityCatalogBuilder::from_env().build()?;

    deltalake_aws::register_handlers(None);

    let catalog = UnityCatalogProvider::try_new(Arc::new(uc), "scarman_sandbox").await?;
    let ctx = SessionContext::new();
    ctx.register_catalog("scarman_sandbox", Arc::new(catalog));

    ctx.sql(
        "select hdci.city_name, hdci.country_code, hdci.latitude, hdci.longitude from \
        scarman_sandbox.external_data.historical_hourly_imperial hhi \
        join scarman_sandbox.external_data.historical_daily_calendar_imperial hdci on hdci.country_code = hhi.country_code \
        order by city_name \
        limit 50;"
    )
        .await?
        .show()
        .await?;

    ctx.table("scarman_sandbox.external_data.historical_hourly_imperial")
        .await?
        .select(vec![
            col("city_name"),
            col("country_code"),
            col("latitude"),
            col("longitude"),
        ])?
        .show_limit(50)
        .await?;

    Ok(())
}
