use std::future::Future;
use std::sync::Arc;

use crate::ConnectorPoolOptions;
use nautilus_connector::{
    Client as ConnectorClient, Executor, MysqlExecutor, PgExecutor, SqliteExecutor,
    TransactionExecutor, TransactionOptions,
};
use nautilus_core::{Error, FindManyArgs, Value};
use nautilus_dialect::Dialect;
use nautilus_engine::{handlers, EngineState};
use nautilus_protocol::error::ERR_RECORD_NOT_FOUND;
use nautilus_protocol::{
    CountParams, CreateManyParams, CreateParams, FindManyParams, GroupByParams, RpcId, RpcRequest,
    RpcResponse, UpdateParams, PROTOCOL_VERSION, QUERY_COUNT, QUERY_CREATE, QUERY_CREATE_MANY,
    QUERY_FIND_MANY, QUERY_GROUP_BY, QUERY_UPDATE,
};
use nautilus_schema::validate_schema_source;
use serde_json::Value as JsonValue;
use tokio::sync::{mpsc, OnceCell};

pub struct Client<E: Executor> {
    inner: ConnectorClient<E>,
    database_url: Arc<String>,
    engine_state: Arc<OnceCell<Arc<EngineState>>>,
    pool_options: ConnectorPoolOptions,
    engine_reads_enabled: bool,
    transaction_id: Option<String>,
}

impl<E> Clone for Client<E>
where
    E: Executor,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            database_url: Arc::clone(&self.database_url),
            engine_state: Arc::clone(&self.engine_state),
            pool_options: self.pool_options,
            engine_reads_enabled: self.engine_reads_enabled,
            transaction_id: self.transaction_id.clone(),
        }
    }
}

impl<E> Client<E>
where
    E: Executor,
{
    pub fn new<D>(dialect: D, executor: E) -> Self
    where
        D: Dialect + Send + Sync + 'static,
    {
        Self {
            inner: ConnectorClient::new(dialect, executor),
            database_url: Arc::new(String::new()),
            engine_state: Arc::new(OnceCell::new()),
            pool_options: ConnectorPoolOptions::default(),
            engine_reads_enabled: false,
            transaction_id: None,
        }
    }

    fn from_connector(
        inner: ConnectorClient<E>,
        database_url: Arc<String>,
        engine_state: Arc<OnceCell<Arc<EngineState>>>,
        pool_options: ConnectorPoolOptions,
        engine_reads_enabled: bool,
        transaction_id: Option<String>,
    ) -> Self {
        Self {
            inner,
            database_url,
            engine_state,
            pool_options,
            engine_reads_enabled,
            transaction_id,
        }
    }

    pub fn dialect(&self) -> &(dyn Dialect + Send + Sync) {
        self.inner.dialect()
    }

    pub fn executor(&self) -> &E {
        self.inner.executor()
    }

    async fn engine_state(&self) -> nautilus_core::Result<Option<Arc<EngineState>>> {
        if !self.engine_reads_enabled || self.database_url.is_empty() {
            return Ok(None);
        }

        let database_url = Arc::clone(&self.database_url);
        let pool_options = self.pool_options;
        let state = self
            .engine_state
            .get_or_try_init(|| async move {
                let schema = parse_generated_schema()?;
                EngineState::new_with_pool_options(
                    schema,
                    (*database_url).clone(),
                    None,
                    pool_options,
                )
                    .await
                    .map(Arc::new)
                    .map_err(|e| {
                        Error::Other(format!("failed to initialize embedded engine: {}", e))
                    })
            })
            .await?;

        Ok(Some(Arc::clone(state)))
    }

    fn transaction_id(&self) -> Option<String> {
        self.transaction_id.clone()
    }
}

impl Client<PgExecutor> {
    pub async fn postgres(url: &str) -> nautilus_connector::ConnectorResult<Self> {
        Self::postgres_with_options(url, ConnectorPoolOptions::default()).await
    }

    pub async fn postgres_with_options(
        url: &str,
        pool_options: ConnectorPoolOptions,
    ) -> nautilus_connector::ConnectorResult<Self> {
        let inner = ConnectorClient::postgres_with_options(url, pool_options).await?;
        Ok(Self::from_connector(
            inner,
            Arc::new(url.to_string()),
            Arc::new(OnceCell::new()),
            pool_options,
            true,
            None,
        ))
    }

    pub async fn transaction<F, Fut, T>(
        &self,
        opts: TransactionOptions,
        f: F,
    ) -> nautilus_connector::ConnectorResult<T>
    where
        F: FnOnce(Client<TransactionExecutor>) -> Fut + Send,
        Fut: Future<Output = nautilus_connector::ConnectorResult<T>> + Send,
        T: Send + 'static,
    {
        let database_url = Arc::clone(&self.database_url);
        let engine_state = Arc::clone(&self.engine_state);
        let pool_options = self.pool_options;
        let embedded_state = self
            .engine_state()
            .await
            .map_err(connector_error_from_core)?;
        let tx_id = embedded_state
            .as_ref()
            .map(|_| uuid::Uuid::new_v4().to_string());
        let timeout = opts.timeout;
        let state_for_cleanup = embedded_state.clone();
        let tx_id_for_cleanup = tx_id.clone();

        let result = self
            .inner
            .transaction(opts, move |tx| {
                let database_url = Arc::clone(&database_url);
                let engine_state = Arc::clone(&engine_state);
                let embedded_state = embedded_state.clone();
                let tx_id = tx_id.clone();
                async move {
                    if let (Some(state), Some(id)) = (embedded_state.as_ref(), tx_id.as_ref()) {
                        state
                            .register_external_transaction(id.clone(), tx.clone(), timeout)
                            .await;
                    }

                    let wrapped = Client::from_connector(
                        tx,
                        database_url,
                        engine_state,
                        pool_options,
                        embedded_state.is_some(),
                        tx_id,
                    );
                    f(wrapped).await
                }
            })
            .await;

        if let (Some(state), Some(id)) = (state_for_cleanup.as_ref(), tx_id_for_cleanup.as_deref()) {
            state.unregister_external_transaction(id).await;
        }

        result
    }
}

impl Client<MysqlExecutor> {
    pub async fn mysql(url: &str) -> nautilus_connector::ConnectorResult<Self> {
        Self::mysql_with_options(url, ConnectorPoolOptions::default()).await
    }

    pub async fn mysql_with_options(
        url: &str,
        pool_options: ConnectorPoolOptions,
    ) -> nautilus_connector::ConnectorResult<Self> {
        let inner = ConnectorClient::mysql_with_options(url, pool_options).await?;
        Ok(Self::from_connector(
            inner,
            Arc::new(url.to_string()),
            Arc::new(OnceCell::new()),
            pool_options,
            true,
            None,
        ))
    }

    pub async fn transaction<F, Fut, T>(
        &self,
        opts: TransactionOptions,
        f: F,
    ) -> nautilus_connector::ConnectorResult<T>
    where
        F: FnOnce(Client<TransactionExecutor>) -> Fut + Send,
        Fut: Future<Output = nautilus_connector::ConnectorResult<T>> + Send,
        T: Send + 'static,
    {
        let database_url = Arc::clone(&self.database_url);
        let engine_state = Arc::clone(&self.engine_state);
        let pool_options = self.pool_options;
        let embedded_state = self
            .engine_state()
            .await
            .map_err(connector_error_from_core)?;
        let tx_id = embedded_state
            .as_ref()
            .map(|_| uuid::Uuid::new_v4().to_string());
        let timeout = opts.timeout;
        let state_for_cleanup = embedded_state.clone();
        let tx_id_for_cleanup = tx_id.clone();

        let result = self
            .inner
            .transaction(opts, move |tx| {
                let database_url = Arc::clone(&database_url);
                let engine_state = Arc::clone(&engine_state);
                let embedded_state = embedded_state.clone();
                let tx_id = tx_id.clone();
                async move {
                    if let (Some(state), Some(id)) = (embedded_state.as_ref(), tx_id.as_ref()) {
                        state
                            .register_external_transaction(id.clone(), tx.clone(), timeout)
                            .await;
                    }

                    let wrapped = Client::from_connector(
                        tx,
                        database_url,
                        engine_state,
                        pool_options,
                        embedded_state.is_some(),
                        tx_id,
                    );
                    f(wrapped).await
                }
            })
            .await;

        if let (Some(state), Some(id)) = (state_for_cleanup.as_ref(), tx_id_for_cleanup.as_deref()) {
            state.unregister_external_transaction(id).await;
        }

        result
    }
}

impl Client<SqliteExecutor> {
    pub async fn sqlite(url: &str) -> nautilus_connector::ConnectorResult<Self> {
        Self::sqlite_with_options(url, ConnectorPoolOptions::default()).await
    }

    pub async fn sqlite_with_options(
        url: &str,
        pool_options: ConnectorPoolOptions,
    ) -> nautilus_connector::ConnectorResult<Self> {
        let inner = ConnectorClient::sqlite_with_options(url, pool_options).await?;
        Ok(Self::from_connector(
            inner,
            Arc::new(url.to_string()),
            Arc::new(OnceCell::new()),
            pool_options,
            true,
            None,
        ))
    }

    pub async fn transaction<F, Fut, T>(
        &self,
        opts: TransactionOptions,
        f: F,
    ) -> nautilus_connector::ConnectorResult<T>
    where
        F: FnOnce(Client<TransactionExecutor>) -> Fut + Send,
        Fut: Future<Output = nautilus_connector::ConnectorResult<T>> + Send,
        T: Send + 'static,
    {
        let database_url = Arc::clone(&self.database_url);
        let engine_state = Arc::clone(&self.engine_state);
        let pool_options = self.pool_options;
        let embedded_state = self
            .engine_state()
            .await
            .map_err(connector_error_from_core)?;
        let tx_id = embedded_state
            .as_ref()
            .map(|_| uuid::Uuid::new_v4().to_string());
        let timeout = opts.timeout;
        let state_for_cleanup = embedded_state.clone();
        let tx_id_for_cleanup = tx_id.clone();

        let result = self
            .inner
            .transaction(opts, move |tx| {
                let database_url = Arc::clone(&database_url);
                let engine_state = Arc::clone(&engine_state);
                let embedded_state = embedded_state.clone();
                let tx_id = tx_id.clone();
                async move {
                    if let (Some(state), Some(id)) = (embedded_state.as_ref(), tx_id.as_ref()) {
                        state
                            .register_external_transaction(id.clone(), tx.clone(), timeout)
                            .await;
                    }

                    let wrapped = Client::from_connector(
                        tx,
                        database_url,
                        engine_state,
                        pool_options,
                        embedded_state.is_some(),
                        tx_id,
                    );
                    f(wrapped).await
                }
            })
            .await;

        if let (Some(state), Some(id)) = (state_for_cleanup.as_ref(), tx_id_for_cleanup.as_deref()) {
            state.unregister_external_transaction(id).await;
        }

        result
    }
}

pub(crate) async fn try_find_many_via_engine<E, M>(
    client: &Client<E>,
    model: &str,
    args: &FindManyArgs,
) -> nautilus_core::Result<Option<Vec<M>>>
where
    E: Executor,
    M: crate::FromRow,
{
    let Some(state) = client.engine_state().await? else {
        return Ok(None);
    };

    let args_json = match nautilus_core::find_many_args_to_protocol_json(args) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };

    let params = FindManyParams {
        protocol_version: PROTOCOL_VERSION,
        model: model.to_string(),
        args: match &args_json {
            JsonValue::Object(map) if map.is_empty() => None,
            _ => Some(args_json),
        },
        transaction_id: client.transaction_id(),
        chunk_size: None,
    };

    let response = execute_engine_request(
        state.as_ref(),
        QUERY_FIND_MANY,
        serde_json::to_value(params)
            .map_err(|e| Error::Other(format!("failed to serialize engine params: {}", e)))?,
    )
    .await?;

    let rows = response
        .get("data")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| Error::Other("engine findMany response is missing a data array".to_string()))?;

    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        decoded.push(M::from_row(&row_from_wire_json(row)?)?);
    }

    Ok(Some(decoded))
}

pub(crate) async fn try_count_via_engine<E>(
    client: &Client<E>,
    model: &str,
    args: Option<JsonValue>,
) -> nautilus_core::Result<Option<i64>>
where
    E: Executor,
{
    let Some(state) = client.engine_state().await? else {
        return Ok(None);
    };

    let params = CountParams {
        protocol_version: PROTOCOL_VERSION,
        model: model.to_string(),
        args,
        transaction_id: client.transaction_id(),
    };

    let response = execute_engine_request(
        state.as_ref(),
        QUERY_COUNT,
        serde_json::to_value(params)
            .map_err(|e| Error::Other(format!("failed to serialize engine count params: {}", e)))?,
    )
    .await?;

    let count = response
        .get("count")
        .and_then(JsonValue::as_i64)
        .ok_or_else(|| Error::Other("engine count response is missing a count".to_string()))?;

    Ok(Some(count))
}

pub(crate) async fn try_group_by_rows_via_engine<E>(
    client: &Client<E>,
    model: &str,
    args: JsonValue,
) -> nautilus_core::Result<Option<Vec<crate::Row>>>
where
    E: Executor,
{
    let Some(state) = client.engine_state().await? else {
        return Ok(None);
    };

    let params = GroupByParams {
        protocol_version: PROTOCOL_VERSION,
        model: model.to_string(),
        args: Some(args),
        transaction_id: client.transaction_id(),
    };

    let response = execute_engine_request(
        state.as_ref(),
        QUERY_GROUP_BY,
        serde_json::to_value(params).map_err(|e| {
            Error::Other(format!(
                "failed to serialize engine groupBy params: {}",
                e
            ))
        })?,
    )
    .await?;

    let rows = response
        .get("data")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| Error::Other("engine groupBy response is missing a data array".to_string()))?;

    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        decoded.push(row_from_wire_json(row)?);
    }

    Ok(Some(decoded))
}

pub(crate) async fn try_create_via_engine<E, M>(
    client: &Client<E>,
    model: &str,
    data: JsonValue,
) -> nautilus_core::Result<Option<M>>
where
    E: Executor,
    M: crate::FromRow,
{
    let Some(state) = client.engine_state().await? else {
        return Ok(None);
    };

    let params = CreateParams {
        protocol_version: PROTOCOL_VERSION,
        model: model.to_string(),
        data,
        transaction_id: client.transaction_id(),
        return_data: true,
    };

    let rows = execute_engine_mutation::<E, M>(
        client,
        state.as_ref(),
        QUERY_CREATE,
        serde_json::to_value(params)
            .map_err(|e| Error::Other(format!("failed to serialize engine create params: {}", e)))?,
    )
    .await?;

    Ok(rows.and_then(|mut rows| rows.drain(..).next()))
}

pub(crate) async fn try_create_many_via_engine<E, M>(
    client: &Client<E>,
    model: &str,
    data: Vec<JsonValue>,
) -> nautilus_core::Result<Option<Vec<M>>>
where
    E: Executor,
    M: crate::FromRow,
{
    let Some(state) = client.engine_state().await? else {
        return Ok(None);
    };

    let params = CreateManyParams {
        protocol_version: PROTOCOL_VERSION,
        model: model.to_string(),
        data,
        transaction_id: client.transaction_id(),
        return_data: true,
    };

    execute_engine_mutation::<E, M>(
        client,
        state.as_ref(),
        QUERY_CREATE_MANY,
        serde_json::to_value(params).map_err(|e| {
            Error::Other(format!(
                "failed to serialize engine createMany params: {}",
                e
            ))
        })?,
    )
    .await
}

pub(crate) async fn try_update_via_engine<E, M>(
    client: &Client<E>,
    model: &str,
    filter: JsonValue,
    data: JsonValue,
) -> nautilus_core::Result<Option<Vec<M>>>
where
    E: Executor,
    M: crate::FromRow,
{
    let Some(state) = client.engine_state().await? else {
        return Ok(None);
    };

    let params = UpdateParams {
        protocol_version: PROTOCOL_VERSION,
        model: model.to_string(),
        filter,
        data,
        transaction_id: client.transaction_id(),
        return_data: true,
    };

    execute_engine_mutation::<E, M>(
        client,
        state.as_ref(),
        QUERY_UPDATE,
        serde_json::to_value(params)
            .map_err(|e| Error::Other(format!("failed to serialize engine update params: {}", e)))?,
    )
    .await
}

async fn execute_engine_mutation<E, M>(
    _client: &Client<E>,
    state: &EngineState,
    method: &str,
    params: JsonValue,
) -> nautilus_core::Result<Option<Vec<M>>>
where
    E: Executor,
    M: crate::FromRow,
{
    let response = execute_engine_request(state, method, params).await?;
    let rows = response
        .get("data")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();

    let mut decoded = Vec::with_capacity(rows.len());
    for row in &rows {
        decoded.push(M::from_row(&row_from_wire_json(row)?)?);
    }

    Ok(Some(decoded))
}

async fn execute_engine_request(
    state: &EngineState,
    method: &str,
    params: JsonValue,
) -> nautilus_core::Result<JsonValue> {
    let (tx, _rx) = mpsc::channel::<RpcResponse>(1);
    let response = handlers::handle_request(
        state,
        RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(RpcId::String(format!("generated-rust-{}", method))),
            method: method.to_string(),
            params,
        },
        tx,
    )
    .await;

    if let Some(error) = response.error {
        return Err(match error.code {
            ERR_RECORD_NOT_FOUND => Error::NotFound(error.message),
            _ => Error::Other(error.message),
        });
    }

    let raw = response
        .result
        .ok_or_else(|| Error::Other(format!("engine returned no result for method {}", method)))?;

    serde_json::from_str(raw.get())
        .map_err(|e| Error::Other(format!("failed to parse engine response: {}", e)))
}

fn parse_generated_schema() -> nautilus_core::Result<nautilus_schema::ir::SchemaIr> {
    validate_schema_source(crate::SCHEMA_SOURCE)
        .map(|validated| validated.ir)
        .map_err(|e| Error::Other(format!("failed to validate embedded schema: {}", e)))
}

fn connector_error_from_core(err: Error) -> nautilus_connector::ConnectorError {
    nautilus_connector::ConnectorError::database_msg(format!(
        "failed to initialize embedded engine: {}",
        err
    ))
}

pub(crate) fn row_from_wire_json(value: &JsonValue) -> nautilus_core::Result<crate::Row> {
    let object = value.as_object().ok_or_else(|| {
        Error::Other("engine returned a row that is not a JSON object".to_string())
    })?;

    let columns = object
        .iter()
        .map(|(name, value)| (name.clone(), wire_value_to_core_value(name, value)))
        .collect();

    Ok(crate::Row::new(columns))
}

pub(crate) fn wire_value_to_core_value(name: &str, value: &JsonValue) -> Value {
    if name.ends_with("_json") {
        return Value::Json(value.clone());
    }

    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(v) => Value::Bool(*v),
        JsonValue::Number(v) => {
            if let Some(i) = v.as_i64() {
                Value::I64(i)
            } else if let Some(f) = v.as_f64() {
                Value::F64(f)
            } else {
                Value::Null
            }
        }
        JsonValue::String(v) => Value::String(v.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Json(value.clone()),
    }
}
