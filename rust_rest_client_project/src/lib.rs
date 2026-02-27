use pyo3::prelude::*;
use pyo3::types::{PyDict, PyBytes, PyList, PyString};
use pyo3::exceptions::PyRuntimeError;
use reqwest::Client;
use std::sync::{Arc, OnceLock, atomic::{AtomicBool, Ordering}};
use tokio::sync::{mpsc, Semaphore, RwLock, oneshot};
use tokio::time::{timeout, Duration, Instant};
use tokio::task::JoinSet;
use serde_json::Value;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use indexmap::IndexMap;
use chrono::Local;

// 全局连接池和客户端缓存
static CLIENT_POOL: Lazy<DashMap<String, Arc<Client>>> = Lazy::new(|| DashMap::new());

// ============================================================
// 修复1(OnceLock版): 用 OnceLock<Runtime> 替代 Box::leak，
// 使 Runtime 在进程退出时可被正常 Drop，避免 pthread_join EINVAL。
// ============================================================
static GLOBAL_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn get_runtime() -> &'static tokio::runtime::Runtime {
    GLOBAL_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(std::cmp::min(num_cpus::get(), 8))
            .max_blocking_threads(32)
            .thread_name("global-rest-client")
            .enable_all()
            .build()
            .expect("Failed to create global runtime")
    })
}

/// 请求状态枚举
#[pyclass(from_py_object)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RequestStatus {
    Ready = 0,
    Success = 1,
    Failed = 2,
    Error = 3,
}

#[pymethods]
impl RequestStatus {
    #[getter]
    fn name(&self) -> &str {
        match self {
            RequestStatus::Ready => "ready",
            RequestStatus::Success => "success",
            RequestStatus::Failed => "failed",
            RequestStatus::Error => "error",
        }
    }
}

/// 高性能请求对象
#[pyclass]
pub struct Request {
    #[pyo3(get, set)]
    method: String,
    #[pyo3(get, set)]
    path: String,
    #[pyo3(get, set)]
    params: Option<Py<PyDict>>,
    #[pyo3(get, set)]
    data: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    headers: Option<Py<PyDict>>,
    #[pyo3(get, set)]
    callback: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    on_failed: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    on_error: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    extra: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    response: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    status: RequestStatus,

    response_text: Option<String>,
    status_code: Option<u16>,
    start_time: Option<Instant>,
    priority: i32,
    timeout_ms: u64,
}

fn pyany_to_param_string(value: &Bound<PyAny>) -> Option<String> {
    if let Ok(v) = value.extract::<String>() {
        Some(v)
    } else if let Ok(i) = value.extract::<i64>() {
        Some(i.to_string())
    } else if let Ok(f) = value.extract::<f64>() {
        Some(f.to_string())
    } else if let Ok(b) = value.extract::<bool>() {
        if b { Some("True".to_string()) } else { Some("False".to_string()) }
    } else {
        value.str().ok().map(|s| s.to_string())
    }
}

fn pythonlist_to_json_string(list: &Bound<PyList>) -> PyResult<String> {
    let mut array = Vec::new();
    for item in list.iter() {
        array.push(pyany_to_json_value(&item)?);
    }
    Ok(serde_json::to_string(&Value::Array(array)).unwrap())
}

#[pymethods]
impl Request {
    #[new]
    #[pyo3(signature = (method, path, params=None, data=None, headers=None, callback=None, on_failed=None, on_error=None, extra=None))]
    fn new(
        method: String,
        path: String,
        params: Option<Py<PyDict>>,
        data: Option<Py<PyAny>>,
        headers: Option<Py<PyDict>>,
        callback: Option<Py<PyAny>>,
        on_failed: Option<Py<PyAny>>,
        on_error: Option<Py<PyAny>>,
        extra: Option<Py<PyAny>>,
    ) -> Self {
        Request {
            method, path, params, data, headers,
            callback, on_failed, on_error, extra,
            response: None,
            status: RequestStatus::Ready,
            response_text: None,
            status_code: None,
            start_time: None,
            priority: 0,
            timeout_ms: 30000,
        }
    }

    fn __str__(&self, py: Python<'_>) -> String {
        let status_code = self.status_code.unwrap_or(0);
        let response_text = self.response_text.as_deref().unwrap_or("");

        let headers_str = self.headers.as_ref()
            .map(|h| h.bind(py).repr()
                .map(|s| s.to_string())
                .unwrap_or_else(|e| {
                    PYTHON_EXECUTOR.write_log(format!("Request.__str__: headers repr 失败: {}", e));
                    "{}".to_string()
                }))
            .unwrap_or_else(|| "None".to_string());

        let params_str = self.params.as_ref()
            .map(|p| p.bind(py).repr()
                .map(|s| s.to_string())
                .unwrap_or_else(|e| {
                    PYTHON_EXECUTOR.write_log(format!("Request.__str__: params repr 失败: {}", e));
                    "{}".to_string()
                }))
            .unwrap_or_else(|| "None".to_string());

        let data_str = self.data.as_ref()
            .map(|d| d.bind(py).repr()
                .map(|s| s.to_string())
                .unwrap_or_else(|e| {
                    PYTHON_EXECUTOR.write_log(format!("Request.__str__: data repr 失败: {}", e));
                    "None".to_string()
                }))
            .unwrap_or_else(|| "None".to_string());

        format!(
            "request: {} {} {} status_code: {}\nheaders: {}\nparams: {}\ndata: {}\nresponse: {}\n",
            self.method, self.path, self.status.name(), status_code,
            headers_str, params_str, data_str, response_text
        )
    }

    #[getter]
    fn get_elapsed_ms(&self) -> u128 {
        self.start_time.map(|s| s.elapsed().as_millis()).unwrap_or(0)
    }
}

/// 配置结构体
#[derive(Clone, Debug)]
pub struct ClientConfig {
    max_connections: usize,
    max_concurrent_requests: usize,
    request_timeout_ms: u64,
    connect_timeout_ms: u64,
    pool_timeout_ms: u64,
    batch_size: usize,
    semaphore_acquire_timeout_ms: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            max_concurrent_requests: 1000,
            request_timeout_ms: 5000,
            connect_timeout_ms: 5000,
            pool_timeout_ms: 5000,
            batch_size: 50,
            semaphore_acquire_timeout_ms: 30000,
        }
    }
}

/// Python 操作任务
enum PythonTask {
    Sign {
        client: Py<RestClient>,
        request: Py<Request>,
        response_tx: oneshot::Sender<PyResult<Py<Request>>>,
    },
    Callback {
        callback: Py<PyAny>,
        data: Value,
        request: Py<Request>,
    },
    OnFailed {
        callback: Py<PyAny>,
        status_code: u16,
        request: Py<Request>,
    },
    OnError {
        callback: Py<PyAny>,
        exception_type: String,
        exception_value: String,
        request: Option<Py<Request>>,
    },
    WriteLog {
        message: String,
    },
    SaveConnectionStatus {
        gateway_name: String,
        status: bool,
    },
    UpdateRequestSuccess {
        request: Py<Request>,
        status_code: u16,
        response_text: String,
        response_headers: IndexMap<String, String>,
        response_tx: oneshot::Sender<PyResult<(Option<Py<PyAny>>, Py<Request>)>>,
    },
    UpdateRequestFailed {
        request: Py<Request>,
        status_code: u16,
        response_text: String,
        response_headers: IndexMap<String, String>,
        response_tx: oneshot::Sender<PyResult<(Option<Py<PyAny>>, bool, Py<Request>)>>,
    },
    UpdateRequestError {
        request: Py<Request>,
        response_tx: oneshot::Sender<PyResult<(Option<Py<PyAny>>, Py<Request>)>>,
    },
    HandleFailedResponse {
        request: Py<Request>,
        status_code: u16,
        gateway_name: String,
        response_text: String,
    },
    HandleErrorResponse {
        request: Py<Request>,
        error_msg: String,
        gateway_name: String,
    },
    /// 关闭信号，用于优雅退出接收循环
    Shutdown,
}

struct PythonExecutor {
    task_tx: mpsc::Sender<PythonTask>,
}

// ============================================================
// 修复2(OnceLock版): PythonExecutor 内部 runtime 不再使用 Box::leak，
// 改为直接在线程闭包内持有所有权，线程退出时自动 Drop。
// ============================================================
impl PythonExecutor {
    fn new() -> Self {
        let (task_tx, mut task_rx) = mpsc::channel::<PythonTask>(10000);
        let task_tx_for_thread = task_tx.clone();

        std::thread::spawn(move || {
            // ← 修复2: 直接持有 Runtime 所有权，不再 Box::leak
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(std::cmp::min(num_cpus::get(), 8))
                .max_blocking_threads(16)
                .thread_name("rest-client-python-executor")
                .enable_all()
                .build()
                .expect("Failed to create Python executor runtime");

            let python_pool = Arc::new(
                threadpool::ThreadPool::with_name("rest-client-python-ops".to_string(), 4)
            );

            rt.block_on(async move {
                while let Some(task) = task_rx.recv().await {
                    let pool = python_pool.clone();
                    let log_tx = task_tx_for_thread.clone();

                    match task {
                        // 收到关闭信号则退出循环
                        PythonTask::Shutdown => break,

                        PythonTask::Sign { client, request, response_tx } => {
                            pool.execute(move || {
                                let result = Python::attach(|py| -> PyResult<Py<Request>> {
                                    let result = client.bind(py).call_method1("sign", (request.bind(py),))?;
                                    Ok(result.extract::<Py<Request>>()?)
                                });
                                if response_tx.send(result).is_err() {
                                    let _ = log_tx.try_send(PythonTask::WriteLog {
                                        message: "PythonTask::Sign: 发送签名结果到通道失败".to_string()
                                    });
                                }
                            });
                        },

                        PythonTask::Callback { callback, data, request } => {
                            pool.execute(move || {
                                if let Err(e) = Python::attach(|py| -> PyResult<()> {
                                    let py_dict = json_to_pyobject(py, &data)?;
                                    callback.bind(py).call1((py_dict, request.bind(py)))?;
                                    Ok(())
                                }) {
                                    let _ = log_tx.try_send(PythonTask::WriteLog {
                                        message: format!("PythonTask::Callback 处理错误: {}", e)
                                    });
                                }
                            });
                        },

                        PythonTask::OnFailed { callback, status_code, request } => {
                            pool.execute(move || {
                                if let Err(e) = Python::attach(|py| -> PyResult<()> {
                                    callback.bind(py).call1((status_code, request.bind(py)))?;
                                    Ok(())
                                }) {
                                    let _ = log_tx.try_send(PythonTask::WriteLog {
                                        message: format!(
                                            "PythonTask::OnFailed 处理错误: {}, 状态码: {}",
                                            e, status_code
                                        )
                                    });
                                }
                            });
                        },

                        PythonTask::OnError { callback, exception_type, exception_value, request } => {
                            pool.execute(move || {
                                if let Err(e) = Python::attach(|py| -> PyResult<()> {
                                    let exc_type = py.get_type::<pyo3::exceptions::PyException>();
                                    if let Some(req) = request.as_ref() {
                                        callback.bind(py).call1((
                                            exc_type,
                                            exception_value.clone(),
                                            py.None(),
                                            req.bind(py)
                                        ))?;
                                    } else {
                                        callback.bind(py).call1((
                                            exc_type,
                                            exception_value.clone(),
                                            py.None(),
                                            py.None()
                                        ))?;
                                    }
                                    Ok(())
                                }) {
                                    let _ = log_tx.try_send(PythonTask::WriteLog {
                                        message: format!(
                                            "PythonTask::OnError 处理错误: {}, 异常类型: {}, 异常值: {}",
                                            e, exception_type, exception_value
                                        )
                                    });
                                }
                            });
                        },

                        PythonTask::WriteLog { message } => {
                            pool.execute(move || {
                                let _ = Python::attach(|py| call_write_log(py, &message));
                            });
                        },

                        PythonTask::SaveConnectionStatus { gateway_name, status } => {
                            pool.execute(move || {
                                let _ = Python::attach(|py| call_save_connection_status(py, &gateway_name, status));
                            });
                        },

                        PythonTask::UpdateRequestSuccess { request, status_code, response_text, response_headers, response_tx } => {
                            pool.execute(move || {
                                let result = Python::attach(|py| -> PyResult<(Option<Py<PyAny>>, Py<Request>)> {
                                    let mut req = request.borrow_mut(py);
                                    req.status_code = Some(status_code);
                                    req.response_text = Some(response_text.clone());
                                    req.status = RequestStatus::Success;

                                    let headers_dict = PyDict::new(py);
                                    for (k, v) in response_headers.iter() {
                                        headers_dict.set_item(k, v)?;
                                    }
                                    req.response = Some(Py::new(py, PyResponseObject {
                                        status_code,
                                        text: response_text,
                                        headers: headers_dict.unbind(),
                                    })?.into_any());

                                    let callback = req.callback.as_ref().map(|c| c.clone_ref(py));
                                    let request_clone = request.clone_ref(py);
                                    Ok((callback, request_clone))
                                });
                                let _ = response_tx.send(result);
                            });
                        },

                        PythonTask::UpdateRequestFailed { request, status_code, response_text, response_headers, response_tx } => {
                            pool.execute(move || {
                                let result = Python::attach(|py| -> PyResult<(Option<Py<PyAny>>, bool, Py<Request>)> {
                                    let mut req = request.borrow_mut(py);
                                    req.status_code = Some(status_code);
                                    req.response_text = Some(response_text.clone());
                                    req.status = RequestStatus::Failed;

                                    let headers_dict = PyDict::new(py);
                                    for (k, v) in response_headers.iter() {
                                        headers_dict.set_item(k, v)?;
                                    }
                                    req.response = Some(Py::new(py, PyResponseObject {
                                        status_code,
                                        text: response_text,
                                        headers: headers_dict.unbind(),
                                    })?.into_any());

                                    let has_on_failed = req.on_failed.is_some();
                                    let on_failed = req.on_failed.as_ref().map(|f| f.clone_ref(py));
                                    let request_clone = request.clone_ref(py);
                                    Ok((on_failed, !has_on_failed, request_clone))
                                });
                                let _ = response_tx.send(result);
                            });
                        },

                        PythonTask::UpdateRequestError { request, response_tx } => {
                            pool.execute(move || {
                                let result = Python::attach(|py| -> PyResult<(Option<Py<PyAny>>, Py<Request>)> {
                                    let mut req = request.borrow_mut(py);
                                    req.status = RequestStatus::Error;
                                    let on_error = req.on_error.as_ref().map(|e| e.clone_ref(py));
                                    let request_clone = request.clone_ref(py);
                                    Ok((on_error, request_clone))
                                });
                                let _ = response_tx.send(result);
                            });
                        },

                        PythonTask::HandleFailedResponse { request, status_code, gateway_name, response_text } => {
                            pool.execute(move || {
                                let _ = Python::attach(|py| {
                                    handle_failed_response(py, status_code, &request, &gateway_name, &response_text)
                                });
                            });
                        },

                        PythonTask::HandleErrorResponse { request, error_msg, gateway_name } => {
                            pool.execute(move || {
                                let _ = Python::attach(|py| {
                                    handle_error_response(py, &error_msg, &request, &gateway_name)
                                });
                            });
                        },
                    }
                }
                // rt 在此处 block_on 返回后自动 Drop，完成优雅关闭
            });
        });

        Self { task_tx }
    }

    fn shutdown(&self) {
        let _ = self.task_tx.try_send(PythonTask::Shutdown);
    }

    async fn sign_async(&self, client: Py<RestClient>, request: Py<Request>) -> PyResult<Py<Request>> {
        let (response_tx, response_rx) = oneshot::channel();
        if self.task_tx.send(PythonTask::Sign { client, request, response_tx }).await.is_err() {
            self.write_log("sign_async: 发送签名任务失败".to_string());
            return Err(PyRuntimeError::new_err("发送签名任务失败"));
        }
        response_rx.await.map_err(|_| {
            self.write_log("sign_async: 未收到签名响应".to_string());
            PyRuntimeError::new_err("未收到签名响应")
        })?
    }

    async fn callback_async(&self, callback: Py<PyAny>, data: Value, request: Py<Request>) {
        let _ = self.task_tx.send(PythonTask::Callback { callback, data, request }).await;
    }

    async fn on_failed_async(&self, callback: Py<PyAny>, status_code: u16, request: Py<Request>) {
        let _ = self.task_tx.send(PythonTask::OnFailed { callback, status_code, request }).await;
    }

    async fn on_error_async(&self, callback: Py<PyAny>, exception_type: String, exception_value: String, request: Option<Py<Request>>) {
        let _ = self.task_tx.send(PythonTask::OnError { callback, exception_type, exception_value, request }).await;
    }

    fn write_log(&self, message: String) {
        let _ = self.task_tx.try_send(PythonTask::WriteLog { message });
    }

    fn save_connection_status(&self, gateway_name: String, status: bool) {
        let _ = self.task_tx.try_send(PythonTask::SaveConnectionStatus { gateway_name, status });
    }

    async fn update_request_success_async(
        &self,
        request: Py<Request>,
        status_code: u16,
        response_text: String,
        response_headers: IndexMap<String, String>,
    ) -> PyResult<(Option<Py<PyAny>>, Py<Request>)> {
        let (response_tx, response_rx) = oneshot::channel();
        if self.task_tx.send(PythonTask::UpdateRequestSuccess {
            request, status_code, response_text, response_headers, response_tx
        }).await.is_err() {
            return Err(PyRuntimeError::new_err("发送更新成功状态任务失败"));
        }
        response_rx.await.map_err(|_| PyRuntimeError::new_err("未收到更新成功状态响应"))?
    }

    async fn update_request_failed_async(
        &self,
        request: Py<Request>,
        status_code: u16,
        response_text: String,
        response_headers: IndexMap<String, String>,
    ) -> PyResult<(Option<Py<PyAny>>, bool, Py<Request>)> {
        let (response_tx, response_rx) = oneshot::channel();
        if self.task_tx.send(PythonTask::UpdateRequestFailed {
            request, status_code, response_text, response_headers, response_tx
        }).await.is_err() {
            return Err(PyRuntimeError::new_err("发送更新失败状态任务失败"));
        }
        response_rx.await.map_err(|_| PyRuntimeError::new_err("未收到更新失败状态响应"))?
    }

    async fn update_request_error_async(&self, request: Py<Request>) -> PyResult<(Option<Py<PyAny>>, Py<Request>)> {
        let (response_tx, response_rx) = oneshot::channel();
        if self.task_tx.send(PythonTask::UpdateRequestError { request, response_tx }).await.is_err() {
            return Err(PyRuntimeError::new_err("发送更新错误状态任务失败"));
        }
        response_rx.await.map_err(|_| PyRuntimeError::new_err("未收到更新错误状态响应"))?
    }

    async fn handle_failed_response_async(&self, request: Py<Request>, status_code: u16, gateway_name: String, response_text: String) {
        let _ = self.task_tx.send(PythonTask::HandleFailedResponse {
            request, status_code, gateway_name, response_text
        }).await;
    }

    async fn handle_error_response_async(&self, request: Py<Request>, error_msg: String, gateway_name: String) {
        let _ = self.task_tx.send(PythonTask::HandleErrorResponse { request, error_msg, gateway_name }).await;
    }
}

// 实现 Drop，优雅关闭通道
impl Drop for PythonExecutor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// 统一的请求数据提取函数
fn extract_request_data_with_gil(
    py: Python,
    request: &Py<Request>,
    url_base: &str,
    gateway_name: &str,
) -> (String, String, Vec<(String, String)>, Vec<(String, String)>, Option<String>, bool) {
    let req = request.borrow(py);
    let url = format!("{}{}", url_base, req.path);
    let req_method = req.method.clone();

    let headers_data: Vec<(String, String)> = if let Some(h) = &req.headers {
        h.bind(py).iter()
            .filter_map(|(k, v)| {
                let key_str = match k.extract::<String>() {
                    Ok(s) => s,
                    Err(e) => {
                        PYTHON_EXECUTOR.write_log(format!(
                            "交易接口：{}，提取 header key 失败: {}",
                            gateway_name, e
                        ));
                        return None;
                    }
                };
                let val_str = if let Ok(s) = v.extract::<String>() { s }
                    else if let Ok(i) = v.extract::<i64>() { i.to_string() }
                    else if let Ok(f) = v.extract::<f64>() { f.to_string() }
                    else if let Ok(b) = v.extract::<bool>() { b.to_string() }
                    else {
                        match v.str() {
                            Ok(s) => s.to_string(),
                            Err(e) => {
                                PYTHON_EXECUTOR.write_log(format!(
                                    "交易接口：{}，提取 header value 失败: key='{}', 错误: {}",
                                    gateway_name, key_str, e
                                ));
                                return None;
                            }
                        }
                    };
                Some((key_str, val_str))
            })
            .collect()
    } else {
        vec![]
    };

    let query_params: Vec<(String, String)> = if let Some(p) = &req.params {
        let obj = p.bind(py);
        if let Ok(dict) = obj.cast::<PyDict>() {
            if dict.len() > 0 {
                let mut params = Vec::new();
                for (k, v) in dict.iter() {
                    let key = match k.extract::<String>() {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    if let Ok(list) = v.cast::<PyList>() {
                        for item in list.iter() {
                            if let Some(s) = pyany_to_param_string(&item) {
                                params.push((key.clone(), s));
                            }
                        }
                    } else if let Some(s) = pyany_to_param_string(&v) {
                        params.push((key, s));
                    }
                }
                params
            } else { vec![] }
        } else { vec![] }
    } else { vec![] };

    let (body_data, is_jsonrpc) = if let Some(d) = &req.data {
        let obj = d.bind(py);
        if let Ok(s) = obj.extract::<String>() {
            if !s.is_empty() {
                let rpc = s.contains("jsonrpc");
                (Some(s), rpc)
            } else { (None, false) }
        } else if let Ok(dict) = obj.cast::<PyDict>() {
            if dict.len() > 0 {
                let rpc = dict.contains("jsonrpc").unwrap_or(false);
                (pythondict_to_json_string(dict).ok(), rpc)
            } else { (None, false) }
        } else if let Ok(list) = obj.cast::<PyList>() {
            if list.len() > 0 {
                let json_str = pythonlist_to_json_string(list).ok();
                let rpc = json_str.as_ref().map(|s| s.contains("jsonrpc")).unwrap_or(false);
                (json_str, rpc)
            } else { (None, false) }
        } else if let Ok(bytes) = obj.cast::<PyBytes>() {
            let b = bytes.as_bytes();
            if !b.is_empty() {
                let s = String::from_utf8_lossy(b).to_string();
                let rpc = s.contains("jsonrpc");
                (Some(s), rpc)
            } else { (None, false) }
        } else {
            let s = obj.str().ok().map(|s| s.to_string());
            let rpc = s.as_ref().map(|s| s.contains("jsonrpc")).unwrap_or(false);
            (s, rpc)
        }
    } else { (None, false) };

    (url, req_method, headers_data, query_params, body_data, is_jsonrpc)
}

/// 在异步上下文中提取请求数据
fn extract_request_data(
    signed_request: &Py<Request>,
    url_base: &str,
    gateway_name: &str,
) -> (String, String, Vec<(String, String)>, Vec<(String, String)>, Option<String>, bool) {
    Python::attach(|py| extract_request_data_with_gil(py, signed_request, url_base, gateway_name))
}

// 全局 Python 执行器
static PYTHON_EXECUTOR: Lazy<PythonExecutor> = Lazy::new(|| PythonExecutor::new());

/// 高性能REST客户端
#[pyclass(subclass)]
pub struct RestClient {
    url_base: String,
    gateway_name: String,
    active: Arc<AtomicBool>,
    sender: Option<mpsc::UnboundedSender<Arc<RwLock<Py<Request>>>>>,
    config: ClientConfig,
    semaphore: Arc<Semaphore>,
    // ← 修复1: 类型保持 &'static Runtime，由 get_runtime() 返回，语义不变
    runtime: &'static tokio::runtime::Runtime,
    client_key: String,
    proxies: Option<IndexMap<String, String>>,
    self_py: Option<Py<RestClient>>,
}

#[pymethods]
impl RestClient {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<pyo3::types::PyTuple>, _kwargs: Option<&Bound<PyDict>>) -> PyResult<Self> {
        let config = ClientConfig::default();
        // ← 修复1: 使用 get_runtime() 替代 *GLOBAL_RUNTIME
        let runtime = get_runtime();
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_requests));
        PYTHON_EXECUTOR.write_log(format!(
            "Semaphore初始化完成，最大并发: {}",
            semaphore.available_permits()
        ));
        Ok(RestClient {
            url_base: String::new(),
            gateway_name: String::new(),
            active: Arc::new(AtomicBool::new(false)),
            sender: None,
            semaphore,
            config,
            runtime,
            client_key: String::new(),
            proxies: None,
            self_py: None,
        })
    }

    #[pyo3(signature = (url_base, proxy_host="", proxy_port=0, gateway_name=""))]
    fn init(
        &mut self,
        url_base: String,
        proxy_host: &str,
        proxy_port: u16,
        gateway_name: &str,
    ) -> PyResult<()> {
        self.url_base = url_base.clone();
        self.gateway_name = gateway_name.to_string();

        if !proxy_host.is_empty() && proxy_port > 0 {
            let proxy = format!("http://{}:{}", proxy_host, proxy_port);
            let mut proxies = IndexMap::new();
            proxies.insert("http".to_string(), proxy.clone());
            proxies.insert("https".to_string(), proxy);
            self.proxies = Some(proxies);
        }

        if self.gateway_name.is_empty() {
            return Err(PyRuntimeError::new_err(
                "请到交易接口REST API connect函数里面的self.init函数中添加gateway_name参数"
            ));
        }

        self.client_key = if !proxy_host.is_empty() && proxy_port > 0 {
            format!("{}|{}:{}", gateway_name, proxy_host, proxy_port)
        } else {
            gateway_name.to_string()
        };

        let client = self.runtime.block_on(create_simple_client(
            if !proxy_host.is_empty() && proxy_port > 0 {
                Some(format!("http://{}:{}", proxy_host, proxy_port))
            } else {
                None
            },
            &self.config,
            gateway_name,
        ))?;

        CLIENT_POOL.insert(self.client_key.clone(), Arc::new(client));
        Ok(())
    }

    fn start(slf: &Bound<'_, Self>) -> PyResult<()> {
        let mut self_mut = slf.borrow_mut();
        let gateway_name = self_mut.gateway_name.clone();

        if self_mut.active.load(Ordering::SeqCst) {
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，REST客户端已在运行中，跳过启动", gateway_name
            ));
            return Ok(());
        }

        self_mut.active.store(true, Ordering::SeqCst);
        let (sender, receiver) = mpsc::unbounded_channel();
        self_mut.sender = Some(sender);

        let url_base = self_mut.url_base.clone();
        let client_key = self_mut.client_key.clone();
        let active = Arc::clone(&self_mut.active);
        let semaphore = Arc::clone(&self_mut.semaphore);
        let config = self_mut.config.clone();
        let runtime = self_mut.runtime;

        let py = slf.py();
        let rest_client_py = slf.clone().unbind();
        self_mut.self_py = Some(rest_client_py.clone_ref(py));

        runtime.spawn(async move {
            run_async_worker(
                receiver, gateway_name, client_key,
                active, semaphore, config, url_base, rest_client_py,
            ).await;
        });

        Ok(())
    }

    fn stop(&mut self) -> PyResult<()> {
        self.active.store(false, Ordering::SeqCst);
        self.sender = None;
        Ok(())
    }

    fn join(&mut self) -> PyResult<()> {
        Ok(())
    }

    #[pyo3(signature = (method, path, callback, params=None, data=None, headers=None, on_failed=None, on_error=None, extra=None))]
    fn add_request(
        slf: &Bound<'_, Self>,
        method: String,
        path: String,
        callback: Py<PyAny>,
        params: Option<Py<PyDict>>,
        data: Option<Py<PyAny>>,
        headers: Option<Py<PyDict>>,
        on_failed: Option<Py<PyAny>>,
        on_error: Option<Py<PyAny>>,
        extra: Option<Py<PyAny>>,
    ) -> PyResult<Py<Request>> {
        let py = slf.py();
        let request = Py::new(py, Request {
            method, path, params, data, headers,
            callback: Some(callback),
            on_failed, on_error, extra,
            response: None,
            status: RequestStatus::Ready,
            response_text: None,
            status_code: None,
            start_time: Some(Instant::now()),
            priority: 0,
            timeout_ms: 30000,
        })?;

        let self_ref = slf.borrow();
        if let Some(sender) = &self_ref.sender {
            let request_arc = Arc::new(RwLock::new(request.clone_ref(py)));
            sender.send(request_arc).map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to send request: {}", e))
            })?;
        }
        Ok(request)
    }

    fn sign<'py>(&self, request: Bound<'py, Request>) -> PyResult<Bound<'py, Request>> {
        Ok(request)
    }

    #[pyo3(signature = (method, path, params=None, data=None, headers=None))]
    fn request(
        slf: &Bound<'_, Self>,
        method: String,
        path: String,
        params: Option<Py<PyDict>>,
        data: Option<Py<PyAny>>,
        headers: Option<Py<PyDict>>,
    ) -> PyResult<Py<PyResponseObject>> {
        let py = slf.py();
        let self_ref = slf.borrow();

        if !self_ref.active.load(Ordering::SeqCst) {
            return Err(PyRuntimeError::new_err("RestClient not started. Call start() first."));
        }

        let url_base = self_ref.url_base.clone();
        let gateway_name = self_ref.gateway_name.clone();
        let client_key = self_ref.client_key.clone();
        let config = self_ref.config.clone();
        let runtime = self_ref.runtime;
        let semaphore = self_ref.semaphore.clone();

        let rest_client_py = self_ref.self_py.as_ref()
            .map(|s| s.clone_ref(py))
            .unwrap_or_else(|| slf.clone().unbind());

        let request = Py::new(py, Request {
            method, path, params, data, headers,
            callback: None, on_failed: None, on_error: None, extra: None,
            response: None,
            status: RequestStatus::Ready,
            response_text: None,
            status_code: None,
            start_time: Some(Instant::now()),
            priority: 0,
            timeout_ms: config.request_timeout_ms,
        })?;

        drop(self_ref);

        let handle = runtime.handle().clone();
        py.detach(move || {
            handle.block_on(async {
                let _permit = match timeout(
                    Duration::from_millis(config.semaphore_acquire_timeout_ms),
                    semaphore.acquire_owned()
                ).await {
                    Ok(Ok(p)) => p,
                    Ok(Err(_)) => return Err(PyRuntimeError::new_err("Semaphore closed")),
                    Err(_) => return Err(PyRuntimeError::new_err("获取信号量超时")),
                };

                let signed_request = PYTHON_EXECUTOR
                    .sign_async(rest_client_py, request).await?;

                let client = match CLIENT_POOL.get(&client_key) {
                    Some(c) => c.clone(),
                    None => {
                        PYTHON_EXECUTOR.write_log(format!(
                            "交易接口：{}，HTTP client not found，重启交易子进程", gateway_name
                        ));
                        PYTHON_EXECUTOR.save_connection_status(gateway_name.clone(), false);
                        return Err(PyRuntimeError::new_err("HTTP client not found"));
                    }
                };

                let (url, req_method, headers_data, query_params, body_data, is_jsonrpc) =
                    extract_request_data(&signed_request, &url_base, &gateway_name);

                let result = timeout(
                    Duration::from_millis(config.request_timeout_ms),
                    execute_request_with_data(&client, &req_method, &url, headers_data, query_params, body_data, is_jsonrpc, &gateway_name)
                ).await;

                match result {
                    Ok(Ok((status_code, response_text, _json_body, response_headers))) => {
                        Python::attach(|py| {
                            let headers_dict = PyDict::new(py);
                            for (k, v) in response_headers.iter() {
                                headers_dict.set_item(k, v)?;
                            }
                            Py::new(py, PyResponseObject { status_code, text: response_text, headers: headers_dict.unbind() })
                        })
                    }
                    Ok(Err(e)) => {
                        let msg = format!("REST API连接失败：{}", e);
                        PYTHON_EXECUTOR.write_log(format!("交易接口：{}，{}，重启交易子进程", gateway_name, msg));
                        PYTHON_EXECUTOR.save_connection_status(gateway_name.clone(), false);
                        Err(PyRuntimeError::new_err(msg))
                    }
                    Err(_) => {
                        PYTHON_EXECUTOR.write_log(format!("交易接口：{}，请求超时，重启交易子进程", gateway_name));
                        PYTHON_EXECUTOR.save_connection_status(gateway_name.clone(), false);
                        Err(PyRuntimeError::new_err("请求超时"))
                    }
                }
            })
        })
    }

    fn get_config(&self) -> String {
        format!("{:?}", self.config)
    }

    #[pyo3(signature = (max_concurrent_requests=None, request_timeout_ms=None))]
    fn update_config(
        &mut self,
        max_concurrent_requests: Option<usize>,
        request_timeout_ms: Option<u64>,
    ) -> PyResult<()> {
        if let Some(max_conc) = max_concurrent_requests {
            self.config.max_concurrent_requests = max_conc;
            self.semaphore = Arc::new(Semaphore::new(max_conc));
        }
        if let Some(t) = request_timeout_ms {
            self.config.request_timeout_ms = t;
        }
        Ok(())
    }

    #[getter]
    fn get_gateway_name(&self) -> &str { &self.gateway_name }
    #[setter]
    fn set_gateway_name(&mut self, name: String) { self.gateway_name = name; }
    #[getter]
    fn get_url_base(&self) -> &str { &self.url_base }
    #[setter]
    fn set_url_base(&mut self, url: String) { self.url_base = url; }
    #[getter]
    fn get_active(&self) -> bool { self.active.load(Ordering::SeqCst) }

    fn make_full_url(&self, path: &str) -> String {
        format!("{}{}", self.url_base, path)
    }

    fn on_failed(&self, py: Python, status_code: u16, request: &Bound<Request>) -> PyResult<()> {
        let req = request.borrow();
        if let Some(response_text) = &req.response_text {
            match serde_json::from_str::<Value>(response_text) {
                Ok(data) => {
                    if let Some(msg) = data.get("msg").and_then(|v| v.as_str()) {
                        let filter_msg = ["Endpoint request timeout. ", "No need to change position side."];
                        if filter_msg.contains(&msg) {
                            return Ok(());
                        }
                    }
                }
                Err(_) => {
                    call_write_log(py, &format!(
                        "交易接口：{}，REST API解码json数据出错，错误代码：{}，\n请求路径：{}，\n收到数据：{}",
                        self.gateway_name, status_code, req.path, response_text
                    ))?;
                    return Ok(());
                }
            }
        }
        call_write_log(py, &format!(
            "交易接口：{}，REST API请求失败代码：{}，请求路径：{}，完整请求：{}",
            self.gateway_name, status_code, req.path, req.__str__(py)
        ))?;
        Ok(())
    }

    fn on_error(
        &self, py: Python,
        exception_type: &str, exception_value: &str,
        request: Option<&Bound<Request>>,
    ) -> PyResult<()> {
        let text = self.exception_detail(py, exception_type, exception_value, request);
        call_write_log(py, &text)?;
        Ok(())
    }

    fn exception_detail(
        &self, py: Python,
        exception_type: &str, exception_value: &str,
        request: Option<&Bound<Request>>,
    ) -> String {
        let now = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f");
        let mut text = format!("[{}]: Unhandled RestClient Error：{}\n", now, exception_type);
        if let Some(req) = request {
            text.push_str(&format!("request:{}\n", req.borrow().__str__(py)));
        }
        text.push_str(&format!("Exception trace: \n{}\n", exception_value));
        text
    }
}

async fn create_simple_client(
    proxy: Option<String>,
    config: &ClientConfig,
    gateway_name: &str,
) -> PyResult<Client> {
    let mut builder = Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
        .pool_idle_timeout(Some(Duration::from_millis(config.pool_timeout_ms)))
        .pool_max_idle_per_host(10);

    if let Some(proxy_url) = proxy {
        match reqwest::Proxy::all(&proxy_url) {
            Ok(p) => { builder = builder.proxy(p); }
            Err(e) => {
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，代理配置失败: {}，继续不使用代理", gateway_name, e
                ));
            }
        }
    }

    builder.build().map_err(|e| {
        PYTHON_EXECUTOR.write_log(format!("交易接口：{}，HTTP客户端创建失败: {}", gateway_name, e));
        PyRuntimeError::new_err(format!("Failed to build HTTP client: {}", e))
    })
}

async fn run_async_worker(
    mut receiver: mpsc::UnboundedReceiver<Arc<RwLock<Py<Request>>>>,
    gateway_name: String,
    client_key: String,
    active: Arc<AtomicBool>,
    semaphore: Arc<Semaphore>,
    config: ClientConfig,
    url_base: String,
    rest_client: Py<RestClient>,
) {
    let mut batch: Vec<Arc<RwLock<Py<Request>>>> = Vec::with_capacity(100);
    let mut last_batch_time = Instant::now();
    const BATCH_TIMEOUT: Duration = Duration::from_millis(10);

    let mut task_set = JoinSet::new();
    const MAX_INFLIGHT: usize = 64;

    while active.load(Ordering::SeqCst) {
        let should_process = if batch.is_empty() {
            match timeout(Duration::from_millis(100), receiver.recv()).await {
                Ok(Some(req)) => { batch.push(req); last_batch_time = Instant::now(); false }
                Ok(None) => {
                    PYTHON_EXECUTOR.write_log(format!("交易接口：{}，接收通道已关闭，停止worker", gateway_name));
                    break;
                }
                Err(_) => continue,
            }
        } else {
            match timeout(BATCH_TIMEOUT, receiver.recv()).await {
                Ok(Some(req)) => { batch.push(req); batch.len() >= config.batch_size }
                Ok(None) => {
                    PYTHON_EXECUTOR.write_log(format!("交易接口：{}，接收通道已关闭（批处理中），停止worker", gateway_name));
                    break;
                }
                Err(_) => true,
            }
        };

        if (should_process || last_batch_time.elapsed() >= BATCH_TIMEOUT) && !batch.is_empty() {
            let priorities: Vec<i32> = tokio::task::block_in_place(|| {
                Python::attach(|py| {
                    batch.iter().map(|req_arc| {
                        req_arc.try_read().map(|g| g.borrow(py).priority).unwrap_or(0)
                    }).collect()
                })
            });

            let mut indexed: Vec<(i32, Arc<RwLock<Py<Request>>>)> = priorities
                .into_iter().zip(batch.drain(..)).collect();
            indexed.sort_by(|(pa, _), (pb, _)| pb.cmp(pa));
            batch = indexed.into_iter().map(|(_, req)| req).collect();

            while task_set.len() >= MAX_INFLIGHT {
                if let Some(result) = task_set.join_next().await {
                    if let Err(e) = result {
                        PYTHON_EXECUTOR.write_log(format!(
                            "交易接口：{}，任务异常退出: {}", gateway_name, e
                        ));
                    }
                }
            }

            if let Some(client) = CLIENT_POOL.get(&client_key) {
                let client = client.clone();

                for request_arc in batch.drain(..) {
                    let client_clone = client.clone();
                    let gw = gateway_name.clone();
                    let ub = url_base.clone();
                    let cfg = config.clone();
                    let sem = semaphore.clone();

                    let rc_clone = tokio::task::block_in_place(|| {
                        Python::attach(|py| rest_client.clone_ref(py))
                    });

                    task_set.spawn(async move {
                        let permit = match sem.acquire_owned().await {
                            Ok(p) => p,
                            Err(e) => {
                                PYTHON_EXECUTOR.write_log(format!(
                                    "交易接口：{}，信号量已关闭，丢弃请求: {}", gw, e
                                ));
                                return;
                            }
                        };
                        let _permit = permit;

                        if let Err(e) = process_request_async(
                            request_arc, &client_clone, &gw, &ub, &cfg, rc_clone
                        ).await {
                            PYTHON_EXECUTOR.write_log(format!(
                                "交易所{}，异步request进程出错：{}", gw, e
                            ));
                        }
                    });
                }
            } else {
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，HTTP客户端未找到，key: {}，丢弃{}个请求",
                    gateway_name, client_key, batch.len()
                ));
                batch.clear();
            }

            last_batch_time = Instant::now();
        }
    }

    // 优雅关闭：等待所有 in-flight 任务完成
    while let Some(result) = task_set.join_next().await {
        if let Err(e) = result {
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，关闭时任务异常: {}", gateway_name, e
            ));
        }
    }

    PYTHON_EXECUTOR.write_log(format!("交易接口：{}，异步worker已退出", gateway_name));
}

async fn process_request_async(
    request_arc: Arc<RwLock<Py<Request>>>,
    client: &Client,
    gateway_name: &str,
    url_base: &str,
    config: &ClientConfig,
    rest_client: Py<RestClient>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // 1. 签名
    let signed_request = {
        let request_guard = request_arc.read().await;
        let request_py = tokio::task::block_in_place(|| {
            Python::attach(|py| request_guard.clone_ref(py))
        });
        drop(request_guard);

        match PYTHON_EXECUTOR.sign_async(rest_client, request_py).await {
            Ok(signed) => signed,
            Err(e) => {
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，签名失败：{}", gateway_name, e
                ));
                return Err(format!("Sign failed: {}", e).into());
            }
        }
    };

    {
        let mut guard = request_arc.write().await;
        *guard = signed_request;
    }

    // 2. 获取超时
    let timeout_duration = {
        let guard = request_arc.read().await;
        let ms = tokio::task::block_in_place(|| Python::attach(|py| guard.borrow(py).timeout_ms));
        Duration::from_millis(ms)
    };

    let result = timeout(
        timeout_duration,
        execute_request_async_internal(client, &request_arc, gateway_name, url_base, config)
    ).await;

    match result {
        Ok(Ok((status_code, response_text, json_body, response_headers))) => {
            if status_code == 502 {
                let path = {
                    let guard = request_arc.read().await;
                    tokio::task::block_in_place(|| Python::attach(|py| guard.borrow(py).path.clone()))
                };
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，REST API请求失败，请求地址：{}{}，错误代码：{}，错误信息：{}",
                    gateway_name, url_base, path, status_code, response_text
                ));
                PYTHON_EXECUTOR.save_connection_status(gateway_name.to_string(), false);
                return Ok(());
            }

            let request_clone = {
                let guard = request_arc.read().await;
                tokio::task::block_in_place(|| Python::attach(|py| guard.clone_ref(py)))
            };

            if status_code / 100 == 2 {
                let (callback_opt, req_py) = PYTHON_EXECUTOR
                    .update_request_success_async(request_clone, status_code, response_text, response_headers)
                    .await?;
                if let Some(cb) = callback_opt {
                    PYTHON_EXECUTOR.callback_async(cb, json_body, req_py).await;
                }
            } else {
                let (on_failed_opt, should_handle_failed, req_py) = PYTHON_EXECUTOR
                    .update_request_failed_async(request_clone, status_code, response_text.clone(), response_headers)
                    .await?;
                if let Some(on_failed) = on_failed_opt {
                    PYTHON_EXECUTOR.on_failed_async(on_failed, status_code, req_py).await;
                } else if should_handle_failed {
                    PYTHON_EXECUTOR.handle_failed_response_async(
                        req_py, status_code, gateway_name.to_string(), response_text
                    ).await;
                }
            }
        }
        Ok(Err(e)) => {
            PYTHON_EXECUTOR.write_log(format!("交易接口：{}，请求执行失败：{}", gateway_name, e));
            let request_clone = {
                let guard = request_arc.read().await;
                tokio::task::block_in_place(|| Python::attach(|py| guard.clone_ref(py)))
            };
            let (on_error_opt, req_py) = PYTHON_EXECUTOR.update_request_error_async(request_clone).await?;
            let error_msg = e.to_string();
            if let Some(on_error) = on_error_opt {
                PYTHON_EXECUTOR.on_error_async(on_error, "Exception".to_string(), error_msg, Some(req_py)).await;
            } else {
                PYTHON_EXECUTOR.handle_error_response_async(req_py, error_msg, gateway_name.to_string()).await;
                PYTHON_EXECUTOR.save_connection_status(gateway_name.to_string(), false);
            }
        }
        Err(_) => {
            let request_clone = {
                let guard = request_arc.read().await;
                tokio::task::block_in_place(|| Python::attach(|py| guard.clone_ref(py)))
            };
            let (on_error_opt, req_py) = PYTHON_EXECUTOR.update_request_error_async(request_clone).await?;
            if let Some(on_error) = on_error_opt {
                PYTHON_EXECUTOR.on_error_async(
                    on_error, "TimeoutException".to_string(), "Request timeout".to_string(), Some(req_py)
                ).await;
            } else {
                PYTHON_EXECUTOR.handle_error_response_async(
                    req_py, "Request timeout".to_string(), gateway_name.to_string()
                ).await;
            }
        }
    }

    Ok(())
}

async fn execute_request_async_internal(
    client: &Client,
    request_arc: &Arc<RwLock<Py<Request>>>,
    gateway_name: &str,
    url_base: &str,
    _config: &ClientConfig,
) -> Result<(u16, String, Value, IndexMap<String, String>), Box<dyn std::error::Error + Send + Sync>> {
    let (url, method, headers_data, query_params, body_data, is_jsonrpc) = {
        let guard = request_arc.read().await;
        tokio::task::block_in_place(|| {
            Python::attach(|py| extract_request_data_with_gil(py, &guard, url_base, gateway_name))
        })
    };
    execute_request_with_data(client, &method, &url, headers_data, query_params, body_data, is_jsonrpc, gateway_name).await
}

async fn execute_request_with_data(
    client: &Client,
    method: &str,
    url: &str,
    headers_data: Vec<(String, String)>,
    query_params: Vec<(String, String)>,
    body_data: Option<String>,
    is_jsonrpc: bool,
    gateway_name: &str,
) -> Result<(u16, String, Value, IndexMap<String, String>), Box<dyn std::error::Error + Send + Sync>> {

    let http_method = match method.to_uppercase().as_str() {
        "GET" => reqwest::Method::GET,
        "POST" => reqwest::Method::POST,
        "PUT" => reqwest::Method::PUT,
        "DELETE" => reqwest::Method::DELETE,
        "PATCH" => reqwest::Method::PATCH,
        other => {
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，警告: 未知的HTTP方法 '{}'，使用GET", gateway_name, other
            ));
            reqwest::Method::GET
        }
    };

    let mut req_builder = client.request(http_method, url);

    for (k, v) in headers_data.iter() {
        let name = k.parse::<reqwest::header::HeaderName>()
            .map_err(|e| format!("Invalid header name '{}': {}", k, e))?;
        let value = reqwest::header::HeaderValue::from_str(v)
            .map_err(|e| format!("Invalid header value for '{}': {}", k, e))?;
        req_builder = req_builder.header(name, value);
    }

    if !query_params.is_empty() {
        req_builder = req_builder.query(&query_params);
    }

    if let Some(data) = body_data {
        if is_jsonrpc {
            match serde_json::from_str::<Value>(&data) {
                Ok(json_val) => { req_builder = req_builder.json(&json_val); }
                Err(e) => {
                    PYTHON_EXECUTOR.write_log(format!(
                        "交易接口：{}，JSON解析失败: {}，使用原始字符串", gateway_name, e
                    ));
                    req_builder = req_builder
                        .header("Content-Type", "application/json")
                        .body(data);
                }
            }
        } else {
            req_builder = req_builder.body(data);
        }
    }

    let response = req_builder.send().await.map_err(|e| {
        PYTHON_EXECUTOR.write_log(format!("交易接口：{}，请求发送失败: {}", gateway_name, e));
        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
    })?;

    let status_code = response.status().as_u16();
    let mut response_headers = IndexMap::new();
    for (name, value) in response.headers().iter() {
        if let Ok(v) = value.to_str() {
            response_headers.insert(name.as_str().to_string(), v.to_string());
        }
    }

    let response_text = response.text().await.map_err(|e| {
        PYTHON_EXECUTOR.write_log(format!("交易接口：{}，响应body读取失败: {}", gateway_name, e));
        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
    })?;

    let json_body = if status_code == 204 || response_text.trim().is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str(&response_text).unwrap_or_else(|e| {
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，JSON解析失败: {}，原始文本: {}", gateway_name, e, response_text
            ));
            let mut map = serde_json::Map::new();
            map.insert("text".to_string(), Value::String(response_text.clone()));
            Value::Object(map)
        })
    };

    Ok((status_code, response_text, json_body, response_headers))
}

fn handle_failed_response(
    py: Python,
    status_code: u16,
    request_guard: &Py<Request>,
    gateway_name: &str,
    response_text: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match serde_json::from_str::<Value>(response_text) {
        Ok(data) => {
            if let Some(msg) = data.get("msg").and_then(|v| v.as_str()) {
                let filter_msg = ["Endpoint request timeout. ", "No need to change position side."];
                if filter_msg.contains(&msg) {
                    return Ok(());
                }
            }
        }
        Err(_) => {
            let req = request_guard.bind(py).borrow();
            call_write_log(py, &format!(
                "交易接口：{}，REST API解码json数据出错，错误代码：{}，\n请求路径：{}，\n收到数据：{}",
                gateway_name, status_code, req.path, response_text
            ))?;
            return Ok(());
        }
    }

    let req = request_guard.bind(py).borrow();
    call_write_log(py, &format!(
        "交易接口：{}，REST API请求失败代码：{}，请求路径：{}，完整请求：{}",
        gateway_name, status_code, req.path, req.__str__(py)
    ))?;
    Ok(())
}

fn handle_error_response(
    py: Python,
    error_msg: &str,
    request_guard: &Py<Request>,
    gateway_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let req = request_guard.bind(py).borrow();
    call_write_log(py, &format!(
        "交易接口：{}，Unhandled RestClient Error：Exception\nrequest：{}\nException trace：\n{}\n",
        gateway_name, req.__str__(py), error_msg
    ))?;
    Ok(())
}

fn call_write_log(py: Python, msg: &str) -> PyResult<()> {
    let utility = py.import("vnpy.trader.utility")?;
    utility.getattr("write_log")?.call1((msg,))?;
    Ok(())
}

fn call_save_connection_status(py: Python, gateway_name: &str, status: bool) -> PyResult<()> {
    let utility = py.import("vnpy.trader.utility")?;
    utility.getattr("save_connection_status")?.call1((gateway_name, status))?;
    Ok(())
}

fn json_to_pyobject(py: Python, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map { dict.set_item(k, json_to_pyobject(py, v)?)?; }
            Ok(dict.unbind().into_any())
        }
        Value::Array(arr) => {
            let items: Vec<Py<PyAny>> = arr.iter().map(|v| json_to_pyobject(py, v)).collect::<PyResult<_>>()?;
            Ok(PyList::new(py, &items)?.unbind().into_any())
        }
        Value::String(s) => Ok(PyString::new(py, s).unbind().into_any()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() { Ok(i.into_pyobject(py)?.to_owned().unbind().into_any()) }
            else if let Some(f) = n.as_f64() { Ok(f.into_pyobject(py)?.to_owned().unbind().into_any()) }
            else { Ok(py.None()) }
        }
        Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().unbind().into_any()),
        Value::Null => Ok(py.None()),
    }
}

fn pythondict_to_json_string(dict: &Bound<PyDict>) -> PyResult<String> {
    let mut map = serde_json::Map::new();
    for (k, v) in dict.iter() {
        map.insert(k.extract::<String>()?, pyany_to_json_value(&v)?);
    }
    Ok(serde_json::to_string(&Value::Object(map)).unwrap())
}

fn pyany_to_json_value(obj: &Bound<PyAny>) -> PyResult<Value> {
    if obj.is_none() { return Ok(Value::Null); }
    if let Ok(b) = obj.extract::<bool>() { return Ok(Value::Bool(b)); }
    if let Ok(i) = obj.extract::<i64>() { return Ok(Value::Number(i.into())); }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null));
    }
    if let Ok(s) = obj.extract::<String>() { return Ok(Value::String(s)); }
    if let Ok(list) = obj.cast::<PyList>() {
        let arr = list.iter().map(|i| pyany_to_json_value(&i)).collect::<PyResult<Vec<_>>>()?;
        return Ok(Value::Array(arr));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            map.insert(k.extract::<String>()?, pyany_to_json_value(&v)?);
        }
        return Ok(Value::Object(map));
    }
    Ok(Value::String(obj.str()?.to_string()))
}

#[pyclass]
pub struct PyResponseObject {
    #[pyo3(get)] status_code: u16,
    #[pyo3(get)] text: String,
    #[pyo3(get)] headers: Py<PyDict>,
}

#[pymethods]
impl PyResponseObject {
    fn json(&self, py: Python) -> PyResult<Py<PyAny>> {
        let value: Value = serde_json::from_str(&self.text)
            .map_err(|e| PyRuntimeError::new_err(format!("JSON decode error: {}", e)))?;
        json_to_pyobject(py, &value)
    }
}

#[pymodule]
fn rust_rest_client(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<RequestStatus>()?;
    m.add_class::<Request>()?;
    m.add_class::<RestClient>()?;
    m.add_class::<PyResponseObject>()?;
    Ok(())
}
