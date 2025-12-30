use pyo3::prelude::*;
use pyo3::types::{PyDict, PyBytes, PyList, PyString};
use pyo3::exceptions::PyRuntimeError;
use reqwest::Client;
use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};
use tokio::sync::{mpsc, Semaphore, RwLock, oneshot};
use tokio::time::{timeout, Duration, Instant};
use serde_json::Value;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use indexmap::IndexMap;
use std::mem::ManuallyDrop;
use chrono::Local;
// 全局连接池和客户端缓存
static CLIENT_POOL: Lazy<DashMap<String, Arc<Client>>> = Lazy::new(|| DashMap::new());
// [新增] 全局 Tokio Runtime，避免由 Python GC 触发 Runtime Drop 导致的线程 Join Panic
static GLOBAL_RUNTIME: Lazy<ManuallyDrop<Arc<tokio::runtime::Runtime>>> = Lazy::new(|| {
    ManuallyDrop::new(Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(std::cmp::min(num_cpus::get(), 8))
            .thread_name("global-rest-client")
            .enable_all()
            .build()
            .expect("Failed to create global runtime")
    ))
});
/// 请求状态枚举
#[pyclass]
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
    
    // 内部字段，不暴露给Python
    response_text: Option<String>,
    status_code: Option<u16>,
    start_time: Option<Instant>,
    retry_count: AtomicUsize,
    
    // 新增用于异步处理的可选字段
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
        // Python requests 将 True/False 转为 "True"/"False"
        if b { Some("True".to_string()) } else { Some("False".to_string()) }
    } else {
        // 对于其他类型，尝试转字符串
        value.str().ok().map(|s| s.to_string())
    }
}
fn pythonlist_to_json_string(list: &Bound<PyList>) -> PyResult<String> {
    let mut array = Vec::new();
    for item in list.iter() {
        let v = pyany_to_json_value(&item)?;
        array.push(v);
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
            method,
            path,
            params,
            data,
            headers,
            callback,
            on_failed,
            on_error,
            extra,
            response: None,
            status: RequestStatus::Ready,
            response_text: None,
            status_code: None,
            start_time: None,
            retry_count: AtomicUsize::new(0),
            priority: 0,
            timeout_ms: 30000,
        }
    }

    // 在 Request 的 #[pymethods] impl 块中替换 __str__ 方法
    fn __str__(&self, py: Python<'_>) -> String {
        let status_code = self.status_code.unwrap_or(0);
        let response_text = self.response_text.as_deref().unwrap_or("");
        
        let headers_str = if let Some(ref headers) = self.headers {
            headers.bind(py).repr().map(|s| s.to_string()).unwrap_or_else(|e| {
                PYTHON_EXECUTOR.write_log(format!("Request.__str__: headers repr 失败: {}", e));
                "{}".to_string()
            })
        } else {
            "None".to_string()
        };
        
        let params_str = if let Some(ref params) = self.params {
            params.bind(py).repr().map(|s| s.to_string()).unwrap_or_else(|e| {
                PYTHON_EXECUTOR.write_log(format!("Request.__str__: params repr 失败: {}", e));
                "{}".to_string()
            })
        } else {
            "None".to_string()
        };
        
        let data_str = if let Some(ref data) = self.data {
            data.bind(py).repr().map(|s| s.to_string()).unwrap_or_else(|e| {
                PYTHON_EXECUTOR.write_log(format!("Request.__str__: data repr 失败: {}", e));
                "None".to_string()
            })
        } else {
            "None".to_string()
        };
        
        format!(
            "request : {} {} {} because {}: \nheaders: {}\nparams: {}\ndata: {}\nresponse:{}\n",
            self.method,
            self.path,
            self.status.name(),
            status_code,
            headers_str,
            params_str,
            data_str,
            response_text
        )
    }
    fn increment_retry(&self) -> usize {
        self.retry_count.fetch_add(1, Ordering::Relaxed)
    }

    #[getter]
    fn get_retry_count(&self) -> usize {
        self.retry_count.load(Ordering::Relaxed)
    }

    #[getter]
    fn get_elapsed_ms(&self) -> u128 {
        if let Some(start) = self.start_time {
            start.elapsed().as_millis()
        } else {
            0
        }
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
    max_retries: usize,
    retry_delay_ms: u64,
    batch_size: usize,
    semaphore_acquire_timeout_ms: u64,  // 新增：信号量获取超时
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            max_concurrent_requests: 1000,
            request_timeout_ms: 5000,
            connect_timeout_ms: 5000,
            pool_timeout_ms: 5000,
            max_retries: 3,
            retry_delay_ms: 1000,
            batch_size: 50,
            semaphore_acquire_timeout_ms: 30000,  // 30秒超时
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
    /// 更新请求状态（成功场景）
    UpdateRequestSuccess {
        request: Py<Request>,
        status_code: u16,
        response_text: String,
        response_headers: IndexMap<String, String>,
        response_tx: oneshot::Sender<PyResult<(Option<Py<PyAny>>, Py<Request>)>>,
    },
    /// 更新请求状态（失败场景）
    UpdateRequestFailed {
        request: Py<Request>,
        status_code: u16,
        response_text: String,
        response_headers: IndexMap<String, String>,
        response_tx: oneshot::Sender<PyResult<(Option<Py<PyAny>>, Option<Py<PyAny>>, bool, Py<Request>)>>,
    },
    /// 更新请求为错误状态
    UpdateRequestError {
        request: Py<Request>,
        response_tx: oneshot::Sender<PyResult<(Option<Py<PyAny>>, Py<Request>)>>,
    },
    /// 处理失败响应
    HandleFailedResponse {
        request: Py<Request>,
        status_code: u16,
        gateway_name: String,
        response_text: String,
    },
    /// 处理错误响应
    HandleErrorResponse {
        request: Py<Request>,
        error_msg: String,
        gateway_name: String,
    },
}


// 修改 PythonExecutor 结构体定义
struct PythonExecutor {
    task_tx: mpsc::Sender<PythonTask>,
    // 使用 ManuallyDrop 避免在程序退出时尝试 join 线程导致的 panic
    _handle: ManuallyDrop<std::thread::JoinHandle<()>>,
}

impl PythonExecutor {
    fn new() -> Self {
        let (task_tx, mut task_rx) = mpsc::channel::<PythonTask>(10000);
        
        let task_tx_for_thread = task_tx.clone();  // Clone before moving into closure
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(std::cmp::min(num_cpus::get(), 8))
                .thread_name("rest-client-python-executor")
                .enable_all()
                .build()
                .expect("Failed to create Python executor runtime");
            
            let python_pool = std::sync::Arc::new(
                threadpool::ThreadPool::with_name("rest-client-python-ops".to_string(), 4)
            );
            
            rt.block_on(async move {
                while let Some(task) = task_rx.recv().await {
                    let pool = python_pool.clone();
                    let log_tx = task_tx_for_thread.clone();  
                    
                    match task {
                        PythonTask::Sign { client, request, response_tx } => {
                            pool.execute(move || {
                                let result = Python::attach(|py| -> PyResult<Py<Request>> {
                                    let result = client.bind(py).call_method1("sign", (request.bind(py),))?;
                                    let signed_request = result.extract::<Py<Request>>()?;
                                    Ok(signed_request)
                                });
                                if let Err(_) = response_tx.send(result) {
                                    let _ = log_tx.try_send(PythonTask::WriteLog { 
                                        message: "PythonTask::Sign: 发送签名结果到通道失败，接收端可能已关闭".to_string() 
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
                                    let log_msg = format!(
                                        "PythonTask::Callback 处理错误: {}\n  收到数据: {}\n  发送请求: {:?}",
                                        e, data, request
                                    );
                                    let _ = log_tx.try_send(PythonTask::WriteLog { message: log_msg });
                                }
                            });
                        },
                        PythonTask::OnFailed { callback, status_code, request } => {
                            pool.execute(move || {
                                if let Err(e) = Python::attach(|py| -> PyResult<()> {
                                    callback.bind(py).call1((status_code, request.bind(py)))?;
                                    Ok(())
                                }) {
                                    let log_msg = format!(
                                        "PythonTask::OnFailed 处理错误: {}\n  状态码: {}\n  发送请求: {:?}",
                                        e, status_code, request
                                    );
                                    let _ = log_tx.try_send(PythonTask::WriteLog { message: log_msg });
                                }
                            });
                        },
                        PythonTask::OnError { callback, exception_value, request, .. } => {
                            pool.execute(move || {
                                if let Err(e) = Python::attach(|py| -> PyResult<()> {
                                    if let Some(req) = request.as_ref() {
                                        callback.bind(py).call1((
                                            py.get_type::<pyo3::exceptions::PyException>(),
                                            exception_value.clone(),
                                            py.None(),
                                            req.bind(py)
                                        ))?;
                                    } else {
                                        callback.bind(py).call1((
                                            py.get_type::<pyo3::exceptions::PyException>(),
                                            exception_value.clone(),
                                            py.None(),
                                            py.None()
                                        ))?;
                                    }
                                    Ok(())
                                }) {
                                    let log_msg = if let Some(req) = request {
                                        format!(
                                            "PythonTask::OnError 处理错误: {}\n  异常值: {}\n  发起请求: {:?}",
                                            e, exception_value, req
                                        )
                                    } else {
                                        format!(
                                            "PythonTask::OnError 处理错误: {}\n  异常值: {}\n  发起请求: None",
                                            e, exception_value
                                        )
                                    };
                                    let _ = log_tx.try_send(PythonTask::WriteLog { message: log_msg });
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
                        // ========== 新增任务处理 ==========
                        PythonTask::UpdateRequestSuccess { request, status_code, response_text, response_headers, response_tx } => {
                            pool.execute(move || {
                                let result = Python::attach(|py| -> PyResult<(Option<Py<PyAny>>, Py<Request>)> {
                                    let mut req = request.borrow_mut(py);
                                    req.status_code = Some(status_code);
                                    req.response_text = Some(response_text.clone());
                                    req.status = RequestStatus::Success;
                                    
                                    let headers_dict = PyDict::new(py);
                                    for (key, value) in response_headers.iter() {
                                        headers_dict.set_item(key, value)?;
                                    }
                                    let response_obj = PyResponseObject {
                                        status_code,
                                        text: response_text,
                                        headers: headers_dict.unbind(),
                                    };
                                    req.response = Some(Py::new(py, response_obj)?.into_any());
                                    
                                    let callback = req.callback.as_ref().map(|c| c.clone_ref(py));
                                    let request_clone = request.clone_ref(py);
                                    Ok((callback, request_clone))
                                });
                                let _ = response_tx.send(result);
                            });
                        },
                        PythonTask::UpdateRequestFailed { request, status_code, response_text, response_headers, response_tx } => {
                            pool.execute(move || {
                                let result = Python::attach(|py| -> PyResult<(Option<Py<PyAny>>, Option<Py<PyAny>>, bool, Py<Request>)> {
                                    let mut req = request.borrow_mut(py);
                                    req.status_code = Some(status_code);
                                    req.response_text = Some(response_text.clone());
                                    req.status = RequestStatus::Failed;
                                    
                                    let headers_dict = PyDict::new(py);
                                    for (key, value) in response_headers.iter() {
                                        headers_dict.set_item(key, value)?;
                                    }
                                    let response_obj = PyResponseObject {
                                        status_code,
                                        text: response_text,
                                        headers: headers_dict.unbind(),
                                    };
                                    req.response = Some(Py::new(py, response_obj)?.into_any());
                                    
                                    let has_on_failed = req.on_failed.is_some();
                                    let on_failed = req.on_failed.as_ref().map(|f| f.clone_ref(py));
                                    let request_clone = request.clone_ref(py);
                                    Ok((None, on_failed, !has_on_failed, request_clone))
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
            });
        });
        
        Self { 
            task_tx, 
            // 使用 ManuallyDrop 包装 handle，避免静态变量 drop 时尝试 join 线程
            _handle: ManuallyDrop::new(handle) 
        }
    }
    
    // ========== 原有方法 ==========
    async fn sign_async(&self, client: Py<RestClient>, request: Py<Request>) -> PyResult<Py<Request>> {
        let (response_tx, response_rx) = oneshot::channel();
        
        if let Err(_) = self.task_tx.send(PythonTask::Sign { client, request, response_tx }).await {
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
    
    // ========== 新增方法 ==========
    async fn update_request_success_async(
        &self,
        request: Py<Request>,
        status_code: u16,
        response_text: String,
        response_headers: IndexMap<String, String>,
    ) -> PyResult<(Option<Py<PyAny>>, Py<Request>)> {
        let (response_tx, response_rx) = oneshot::channel();
        
        if let Err(_) = self.task_tx.send(PythonTask::UpdateRequestSuccess { 
            request, status_code, response_text, response_headers, response_tx 
        }).await {
            self.write_log("update_request_success_async: 发送更新成功状态任务失败".to_string());
            return Err(PyRuntimeError::new_err("发送更新成功状态任务失败"));
        }
        
        response_rx.await.map_err(|_| {
            self.write_log("update_request_success_async: 未收到更新成功状态响应".to_string());
            PyRuntimeError::new_err("未收到更新成功状态响应")
        })?
    }
    
    async fn update_request_failed_async(
        &self,
        request: Py<Request>,
        status_code: u16,
        response_text: String,
        response_headers: IndexMap<String, String>,
    ) -> PyResult<(Option<Py<PyAny>>, Option<Py<PyAny>>, bool, Py<Request>)> {
        let (response_tx, response_rx) = oneshot::channel();
        
        if let Err(_) = self.task_tx.send(PythonTask::UpdateRequestFailed { 
            request, status_code, response_text, response_headers, response_tx 
        }).await {
            self.write_log("update_request_failed_async: 发送更新失败状态任务失败".to_string());
            return Err(PyRuntimeError::new_err("发送更新失败状态任务失败"));
        }
        
        response_rx.await.map_err(|_| {
            self.write_log("update_request_failed_async: 未收到更新失败状态响应".to_string());
            PyRuntimeError::new_err("未收到更新失败状态响应")
        })?
    }
    
    async fn update_request_error_async(&self, request: Py<Request>) -> PyResult<(Option<Py<PyAny>>, Py<Request>)> {
        let (response_tx, response_rx) = oneshot::channel();
        
        if let Err(_) = self.task_tx.send(PythonTask::UpdateRequestError { request, response_tx }).await {
            self.write_log("update_request_error_async: 发送更新错误状态任务失败".to_string());
            return Err(PyRuntimeError::new_err("发送更新错误状态任务失败"));
        }
        
        response_rx.await.map_err(|_| {
            self.write_log("update_request_error_async: 未收到更新错误状态响应".to_string());
            PyRuntimeError::new_err("未收到更新错误状态响应")
        })?
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

/// 提取请求数据的内部实现（在持有 GIL 时调用）
fn extract_request_data_impl(
    py: Python,
    request: &Py<Request>,
    url_base: &str,
    gateway_name: &str,
) -> (String, String, Vec<(String, String)>, Vec<(String, String)>, Option<String>, bool) {
    let request_ref = request.borrow(py);
    let path = request_ref.path.clone();
    let req_method = request_ref.method.clone();
    let url = format!("{}{}", url_base, path);
    
    let headers_data: Vec<(String, String)> = if let Some(headers_py) = &request_ref.headers {
        let headers = headers_py.bind(py);
        
        let result: Vec<(String, String)> = headers.iter()
            .filter_map(|(key, value)| {
                let key_str = match key.extract::<String>() {
                    Ok(k) => k,
                    Err(e) => {
                        PYTHON_EXECUTOR.write_log(format!(
                            "交易接口：{}，提取 header key 失败: {:?}, 错误: {}",
                            gateway_name, key, e
                        ));
                        return None;
                    }
                };
                
                let value_str = if let Ok(v) = value.extract::<String>() {
                    v
                } else if let Ok(i) = value.extract::<i64>() {
                    i.to_string()
                } else if let Ok(f) = value.extract::<f64>() {
                    f.to_string()
                } else if let Ok(b) = value.extract::<bool>() {
                    b.to_string()
                } else {
                    match value.str() {
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
                
                Some((key_str, value_str))
            })
            .collect();
        
        result
    } else {
        vec![]
    };
    
    let query_params: Vec<(String, String)> = if let Some(params_py) = &request_ref.params {
        let params_obj = params_py.bind(py);
        if let Ok(params_dict) = params_obj.cast::<PyDict>() {
            if params_dict.len() > 0 {
                let mut params: Vec<(String, String)> = Vec::new();
                for (key, value) in params_dict.iter() {
                    let k = match key.extract::<String>() {
                        Ok(k) => k,
                        Err(_) => continue,
                    };
                    if let Ok(list) = value.cast::<PyList>() {
                        for item in list.iter() {
                            if let Some(v_str) = pyany_to_param_string(&item) {
                                params.push((k.clone(), v_str));
                            }
                        }
                    } else {
                        if let Some(v_str) = pyany_to_param_string(&value) {
                            params.push((k, v_str));
                        }
                    }
                }
                params
            } else { vec![] }
        } else { vec![] }
    } else { vec![] };
    
    let (body_data, is_jsonrpc) = if let Some(data_py) = &request_ref.data {
        let data_obj = data_py.bind(py);
        if let Ok(data_str) = data_obj.extract::<String>() {
            if !data_str.is_empty() {
                let is_jsonrpc = data_str.contains("jsonrpc");
                (Some(data_str), is_jsonrpc)
            } else { (None, false) }
        } else if let Ok(data_dict) = data_obj.cast::<PyDict>() {
            if data_dict.len() > 0 {
                let is_jsonrpc = data_dict.contains("jsonrpc").unwrap_or(false);
                (pythondict_to_json_string(data_dict).ok(), is_jsonrpc)
            } else { (None, false) }
        } else if let Ok(data_list) = data_obj.cast::<PyList>() {
            // 处理 list 类型的 data（如 JSON-RPC batch request）
            if data_list.len() > 0 {
                let json_str = pythonlist_to_json_string(data_list).ok();
                let is_jsonrpc = json_str.as_ref().map(|s| s.contains("jsonrpc")).unwrap_or(false);
                (json_str, is_jsonrpc)
            } else { (None, false) }
        } else if let Ok(data_bytes) = data_obj.cast::<PyBytes>() {
            let bytes = data_bytes.as_bytes();
            if !bytes.is_empty() {
                let s = String::from_utf8_lossy(bytes).to_string();
                let is_jsonrpc = s.contains("jsonrpc");
                (Some(s), is_jsonrpc)
            } else { (None, false) }
        } else {
            let s = data_obj.str().ok().map(|s| s.to_string());
            let is_jsonrpc = s.as_ref().map(|s| s.contains("jsonrpc")).unwrap_or(false);
            (s, is_jsonrpc)
        }
    } else { (None, false) };
    
    (url, req_method, headers_data, query_params, body_data, is_jsonrpc)
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
    runtime: Arc<tokio::runtime::Runtime>,
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
        
        // [修改] 使用全局 Runtime，而不是每次创建一个新的
        // 这避免了当 RestClient 被 Python GC 回收时，Runtime 尝试 shutdown/join 线程导致的 Panic (os error 22)
        let runtime = (**GLOBAL_RUNTIME).clone();

        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_requests));
        let available = semaphore.available_permits();
        PYTHON_EXECUTOR.write_log(format!(
            "Semaphore初始化完成，最大并发: {}",
            available
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

        let client = self.runtime.block_on(async {
            create_simple_client(
                if !proxy_host.is_empty() && proxy_port > 0 {
                    Some(format!("http://{}:{}", proxy_host, proxy_port))
                } else {
                    None
                },
                &self.config,
                gateway_name,
            ).await
        })?;

        CLIENT_POOL.insert(self.client_key.clone(), Arc::new(client));

        Ok(())
    }

    fn start(slf: &Bound<'_, Self>) -> PyResult<()> {
        let mut self_mut = slf.borrow_mut();
        let url_base = self_mut.url_base.clone();
        let gateway_name = self_mut.gateway_name.clone();
        
        if self_mut.active.load(Ordering::SeqCst) {
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，REST客户端已在运行中，跳过启动", 
                gateway_name
            ));
            return Ok(());
        }

        self_mut.active.store(true, Ordering::SeqCst);

        let (sender, receiver) = mpsc::unbounded_channel();
        self_mut.sender = Some(sender);

        let gateway_name = self_mut.gateway_name.clone();
        let client_key = self_mut.client_key.clone();
        let active = Arc::clone(&self_mut.active);
        let semaphore = Arc::clone(&self_mut.semaphore);
        let config = self_mut.config.clone();
        let runtime = Arc::clone(&self_mut.runtime);
        
        let py = slf.py();
        let rest_client_py = slf.clone().unbind();
        self_mut.self_py = Some(rest_client_py.clone_ref(py));

        runtime.spawn(async move {
            run_async_worker(
                receiver,
                gateway_name,
                client_key,
                active,
                semaphore,
                config,
                url_base,
                rest_client_py,
            ).await;
        });

        Ok(())
    }

    fn stop(&mut self) -> PyResult<()> {
        self.active.store(false, Ordering::SeqCst);
        // 关闭发送通道，让 worker 能够优雅退出
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
        
        let request = Py::new(
            py,
            Request {
                method,
                path,
                params,
                data,
                headers,
                callback: Some(callback),
                on_failed,
                on_error,
                extra,
                response: None,
                status: RequestStatus::Ready,
                response_text: None,
                status_code: None,
                start_time: Some(Instant::now()),
                retry_count: AtomicUsize::new(0),
                priority: 0,
                timeout_ms: 30000,
            },
        )?;

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
        let runtime = Arc::clone(&self_ref.runtime);
        
        let rest_client_py = if let Some(ref self_py) = self_ref.self_py {
            self_py.clone_ref(py)
        } else {
            slf.clone().unbind()
        };
        
        let request = Py::new(
            py,
            Request {
                method,
                path,
                params,
                data,
                headers,
                callback: None,
                on_failed: None,
                on_error: None,
                extra: None,
                response: None,
                status: RequestStatus::Ready,
                response_text: None,
                status_code: None,
                start_time: Some(Instant::now()),
                retry_count: AtomicUsize::new(0),
                priority: 0,
                timeout_ms: config.request_timeout_ms,
            },
        )?;
        
        drop(self_ref);
        
        // 使用 handle 来避免嵌套 runtime 问题
        let handle = runtime.handle().clone();
        
        py.detach(move || {
            handle.block_on(async {
                // 签名阶段
                let signed_request = tokio::task::spawn_blocking(move || {
                    Python::attach(|py| -> PyResult<Py<Request>> {
                        let result = rest_client_py.bind(py).call_method1("sign", (request.bind(py),))?;
                        Ok(result.extract::<Py<Request>>()?)
                    })
                }).await;

                let signed_request = match signed_request {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => return Err(PyRuntimeError::new_err(format!("交易接口：{}，签名失败，错误信息： {}", gateway_name, e))),
                    Err(e) => return Err(PyRuntimeError::new_err(format!("Signing task failed: {}", e))),
                };

                let client = match CLIENT_POOL.get(&client_key) {
                    Some(c) => c.clone(),
                    None => {
                        let error_msg = "HTTP client not found";
                        PYTHON_EXECUTOR.write_log(format!(
                            "交易接口：{}，REST API创建出错，错误信息：{}，重启交易子进程", 
                            gateway_name, error_msg
                        ));
                        PYTHON_EXECUTOR.save_connection_status(gateway_name.clone(), false);
                        return Err(PyRuntimeError::new_err(error_msg));
                    }
                };

                let mut retry_count = 0;
                loop {
                    let (url, req_method, headers_data, query_params, body_data, is_jsonrpc) = 
                        extract_request_data(&signed_request, &url_base, &gateway_name);
                    
                    let result = timeout(
                        Duration::from_millis(config.request_timeout_ms),
                        execute_request_with_data(&client, &req_method, &url, headers_data, query_params, body_data, is_jsonrpc, &gateway_name)
                    ).await;
                    
                    match result {
                        Ok(Ok((status_code, response_text, _json_body, response_headers))) => {
                            return Python::attach(|py| {
                                let headers_dict = PyDict::new(py);
                                for (key, value) in response_headers.iter() {
                                    headers_dict.set_item(key, value)?;
                                }
                                Py::new(py, PyResponseObject {
                                    status_code,
                                    text: response_text,
                                    headers: headers_dict.unbind(),
                                })
                            });
                        }
                        Ok(Err(e)) => {
                            retry_count += 1;
                            if retry_count >= config.max_retries {
                                let error_msg = format!("经过{}次重试后REST API连接失败，错误信息：{}", config.max_retries, e);
                                PYTHON_EXECUTOR.write_log(format!(
                                    "交易接口：{}，REST API连接出错，错误信息：{}，重启交易子进程", 
                                    gateway_name, error_msg
                                ));
                                PYTHON_EXECUTOR.save_connection_status(gateway_name.clone(), false);
                                return Err(PyRuntimeError::new_err(error_msg));
                            }
                            tokio::time::sleep(Duration::from_millis(config.retry_delay_ms)).await;
                        }
                        Err(_) => {
                            retry_count += 1;
                            if retry_count >= config.max_retries {
                                let error_msg = format!("请求超时，重试 {} 次后仍未成功", config.max_retries);
                                PYTHON_EXECUTOR.write_log(format!(
                                    "交易接口：{}，REST API连接出错，错误信息：{}，重启交易子进程", 
                                    gateway_name, error_msg
                                ));
                                PYTHON_EXECUTOR.save_connection_status(gateway_name.clone(), false);
                                return Err(PyRuntimeError::new_err(error_msg));
                            }
                            tokio::time::sleep(Duration::from_millis(config.retry_delay_ms)).await;
                        }
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
        if let Some(timeout) = request_timeout_ms {
            self.config.request_timeout_ms = timeout;
        }
        Ok(())
    }

    #[getter]
    fn get_gateway_name(&self) -> &str {
        &self.gateway_name
    }

    #[setter]
    fn set_gateway_name(&mut self, name: String) {
        self.gateway_name = name;
    }

    #[getter]
    fn get_url_base(&self) -> &str {
        &self.url_base
    }

    #[setter]
    fn set_url_base(&mut self, url: String) {
        self.url_base = url;
    }

    #[getter]
    fn get_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    fn make_full_url(&self, path: &str) -> String {
        format!("{}{}", self.url_base, path)
    }

    fn on_failed(&self, py: Python, status_code: u16, request: &Bound<Request>) -> PyResult<()> {
        let req = request.borrow();
        
        if let Some(response_text) = &req.response_text {
            match serde_json::from_str::<Value>(response_text) {
                Ok(data) => {
                    if let Some(msg) = data.get("msg").and_then(|v| v.as_str()) {
                        let filter_msg = vec![
                            "Endpoint request timeout. ",
                            "No need to change position side.",
                        ];
                        if filter_msg.contains(&msg) {
                            return Ok(());
                        }
                    }
                }
                Err(_) => {
                    call_write_log(
                        py,
                        &format!(
                            "交易接口：{}，REST API解码json数据出错，错误代码：{}，\n请求路径：{}，\n收到数据：{}",
                            self.gateway_name, status_code, req.path, response_text
                        ),
                    )?;
                    return Ok(());
                }
            }
        }

        call_write_log(
            py,
            &format!(
                "交易接口：{}，REST API请求失败代码：{}，请求路径：{}，完整请求：{}",
                self.gateway_name, status_code, req.path, req.__str__(py)
            ),
        )?;

        Ok(())
    }


    fn on_error(
        &self,
        py: Python,
        exception_type: &str,
        exception_value: &str,
        request: Option<&Bound<Request>>,
    ) -> PyResult<()> {
        let text = self.exception_detail(py,exception_type, exception_value, request);
        call_write_log(py, &text)?;
        Ok(())
    }


    fn exception_detail(
        &self,
        py: Python,
        exception_type: &str,
        exception_value: &str,
        request: Option<&Bound<Request>>,
    ) -> String {
        let now = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f");
        let mut text = format!(
            "[{}]: Unhandled RestClient Error：{}\n",
            now, exception_type
        );
        
        if let Some(req) = request {
            text.push_str(&format!("request:{}\n", req.borrow().__str__(py)));
        }
        
        text.push_str(&format!("Exception trace: \n{}\n", exception_value));
        text
    }

}

/// 提取请求数据的辅助函数，避免在异步上下文中长时间持有 GIL
fn extract_request_data(
    signed_request: &Py<Request>,
    url_base: &str,
    gateway_name: &str,
) -> (String, String, Vec<(String, String)>, Vec<(String, String)>, Option<String>, bool) {
    Python::attach(|py| {
        let request_ref = signed_request.borrow(py);
        let path = request_ref.path.clone();
        let req_method = request_ref.method.clone();
        let url = format!("{}{}", url_base, path);
        
        let headers_data: Vec<(String, String)> = if let Some(headers_py) = &request_ref.headers {
            let headers = headers_py.bind(py);
            headers.iter()
                .filter_map(|(key, value)| {
                    let key_str = key.extract::<String>().ok()?;
                    let value_str = if let Ok(v) = value.extract::<String>() {
                        v
                    } else if let Ok(i) = value.extract::<i64>() {
                        i.to_string()
                    } else if let Ok(f) = value.extract::<f64>() {
                        f.to_string()
                    } else if let Ok(b) = value.extract::<bool>() {
                        b.to_string()
                    } else {
                        value.str().ok()?.to_string()
                    };
                    Some((key_str, value_str))
                })
                .collect()
        } else {
            vec![]
        };
        
        let query_params: Vec<(String, String)> = if let Some(params_py) = &request_ref.params {
            let params_obj = params_py.bind(py);
            if let Ok(params_dict) = params_obj.cast::<PyDict>() {
                if params_dict.len() > 0 {
                    let mut params: Vec<(String, String)> = Vec::new();
                    
                    for (key, value) in params_dict.iter() {
                        let k = match key.extract::<String>() {
                            Ok(k) => k,
                            Err(_) => continue,
                        };
                        
                        if let Ok(list) = value.cast::<PyList>() {
                            for item in list.iter() {
                                if let Some(v_str) = pyany_to_param_string(&item) {
                                    params.push((k.clone(), v_str));
                                }
                            }
                        } else {
                            if let Some(v_str) = pyany_to_param_string(&value) {
                                params.push((k, v_str));
                            }
                        }
                    }
                    params
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        };
        
        let (body_data, is_jsonrpc) = if let Some(data_py) = &request_ref.data {
            let data_obj = data_py.bind(py);
            if let Ok(data_str) = data_obj.extract::<String>() {
                if !data_str.is_empty() {
                    let is_jsonrpc = data_str.contains("jsonrpc");
                    (Some(data_str), is_jsonrpc)
                } else {
                    (None, false)
                }
            } else if let Ok(data_dict) = data_obj.cast::<PyDict>() {
                if data_dict.len() > 0 {
                    let is_jsonrpc = data_dict.contains("jsonrpc").unwrap_or(false);
                    (pythondict_to_json_string(data_dict).ok(), is_jsonrpc)
                } else {
                    (None, false)
                }
            } else if let Ok(data_list) = data_obj.cast::<PyList>() {
                // 处理 list 类型的 data（如 JSON-RPC batch request）
                if data_list.len() > 0 {
                    let json_str = pythonlist_to_json_string(data_list).ok();
                    let is_jsonrpc = json_str.as_ref().map(|s| s.contains("jsonrpc")).unwrap_or(false);
                    (json_str, is_jsonrpc)
                } else {
                    (None, false)
                }
            } else if let Ok(data_bytes) = data_obj.cast::<PyBytes>() {
                let bytes = data_bytes.as_bytes();
                if !bytes.is_empty() {
                    let s = String::from_utf8_lossy(bytes).to_string();
                    let is_jsonrpc = s.contains("jsonrpc");
                    (Some(s), is_jsonrpc)
                } else {
                    (None, false)
                }
            } else {
                let s = data_obj.str().ok().map(|s| s.to_string());
                let is_jsonrpc = s.as_ref().map(|s| s.contains("jsonrpc")).unwrap_or(false);
                (s, is_jsonrpc)
            }
        } else {
            (None, false)
        };
        
        (url, req_method, headers_data, query_params, body_data, is_jsonrpc)
    })
}

/// 创建简化的HTTP客户端
async fn create_simple_client(
    proxy: Option<String>,
    config: &ClientConfig,
    gateway_name: &str,
) -> PyResult<Client> {
    
    let mut builder = Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .connect_timeout(Duration::from_millis(config.connect_timeout_ms));
    
    builder = builder
        .pool_idle_timeout(Some(Duration::from_millis(config.pool_timeout_ms)))
        .pool_max_idle_per_host(10);

    if let Some(proxy_url) = proxy {
        match reqwest::Proxy::all(&proxy_url) {
            Ok(proxy_obj) => {
                builder = builder.proxy(proxy_obj);
            }
            Err(e) => {
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，✗ 代理配置失败: {}, 继续不使用代理", gateway_name, e
                ));
            }
        }
    }

    match builder.build() {
        Ok(client) => Ok(client),
        Err(e) => {
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，✗ HTTP客户端创建失败!", gateway_name
            ));
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，错误详情: {:?}", gateway_name, e
            ));
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，错误信息: {}", gateway_name, e
            ));
            Err(PyRuntimeError::new_err(format!("Failed to build HTTP client: {}", e)))
        }
    }
}

/// 高并发异步工作器 - 重构版本（无直接 Python::attach 调用）
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
    let mut batch = Vec::with_capacity(100);
    let mut last_batch_time = Instant::now();
    const BATCH_TIMEOUT: Duration = Duration::from_millis(10);

    while active.load(Ordering::SeqCst) {
        let should_process = if batch.is_empty() {
            match timeout(Duration::from_millis(100), receiver.recv()).await {
                Ok(Some(request)) => {
                    batch.push(request);
                    last_batch_time = Instant::now();
                    false
                }
                Ok(None) => {
                    PYTHON_EXECUTOR.write_log(format!(
                        "交易接口：{}，接收通道已关闭，停止worker",
                        gateway_name
                    ));
                    break;
                }
                Err(_) => continue,
            }
        } else {
            match timeout(BATCH_TIMEOUT, receiver.recv()).await {
                Ok(Some(request)) => {
                    batch.push(request);
                    batch.len() >= config.batch_size
                }
                Ok(None) => {
                    PYTHON_EXECUTOR.write_log(format!(
                        "交易接口：{}，接收通道已关闭（批处理中），停止worker",
                        gateway_name
                    ));
                    break;
                }
                Err(_) => true,
            }
        };

        let should_process = should_process || last_batch_time.elapsed() >= BATCH_TIMEOUT;

        if should_process && !batch.is_empty() {
            // 按优先级排序 - 使用 block_in_place 避免饿死 Tokio 运行时
            let priorities: Vec<i32> = tokio::task::block_in_place(|| {
                Python::attach(|py| {
                    batch.iter().map(|req_arc| {
                        if let Ok(req_guard) = req_arc.try_read() {
                            req_guard.borrow(py).priority
                        } else {
                            0
                        }
                    }).collect()
                })
            });
            
            let mut indexed_batch: Vec<_> = batch.drain(..).enumerate().collect();
            indexed_batch.sort_by(|(i, _), (j, _)| {
                priorities.get(*j).unwrap_or(&0).cmp(priorities.get(*i).unwrap_or(&0))
            });
            batch = indexed_batch.into_iter().map(|(_, req)| req).collect();

            if let Some(client) = CLIENT_POOL.get(&client_key) {
                let client = client.clone();

                for request_arc in batch.drain(..) {
                    let client_clone = client.clone();
                    let gateway_name_clone = gateway_name.clone();
                    let url_base_clone = url_base.clone();
                    let config_clone = config.clone();
                    let semaphore_clone = semaphore.clone();
                    let semaphore_timeout = config.semaphore_acquire_timeout_ms;
                    
                    // 使用 block_in_place 克隆 rest_client，避免饿死 Tokio 运行时
                    let rest_client_clone = tokio::task::block_in_place(|| {
                        Python::attach(|py| rest_client.clone_ref(py))
                    });

                    tokio::spawn(async move {
                        let permit = tokio::select! {
                            biased;
                            permit = semaphore_clone.acquire_owned() => {
                                match permit {
                                    Ok(p) => p,
                                    Err(e) => {
                                        PYTHON_EXECUTOR.write_log(format!(
                                            "交易接口：{}，信号量已关闭，丢弃请求: {}",
                                            gateway_name_clone, e
                                        ));
                                        return;
                                    }
                                }
                            }
                            _ = tokio::time::sleep(Duration::from_millis(semaphore_timeout)) => {
                                PYTHON_EXECUTOR.write_log(format!(
                                    "交易接口：{}，获取信号量超时({}ms)，丢弃请求",
                                    gateway_name_clone, semaphore_timeout
                                ));
                                return;
                            }
                        };
                        let _permit = permit;

                        if let Err(e) = process_request_async(
                            request_arc,
                            &client_clone,
                            &gateway_name_clone,
                            &url_base_clone,
                            &config_clone,
                            rest_client_clone,
                        ).await {
                            PYTHON_EXECUTOR.write_log(format!(
                                "交易所{}，异步request进程出错，错误信息：{}", 
                                gateway_name_clone, e
                            ));
                        }
                    });
                }

            } else {
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，错误：HTTP客户端未找到，key: {}",
                    gateway_name,
                    client_key
                ));
                // 注意：这里也需要避免直接调用 Python::attach
                // 简化处理：直接丢弃请求并记录日志
                for _request_arc in batch.drain(..) {
                    PYTHON_EXECUTOR.write_log(format!(
                        "交易接口：{}，由于 HTTP 客户端未找到，丢弃请求",
                        gateway_name
                    ));
                }
            }

            last_batch_time = Instant::now();
        }
    }
    
    PYTHON_EXECUTOR.write_log(format!(
        "交易接口：{}，异步worker已退出",
        gateway_name
    ));
}

/// 异步处理单个请求 - 重构版本（使用 block_in_place 处理 Python 操作）
async fn process_request_async(
    request_arc: Arc<RwLock<Py<Request>>>,
    client: &Client,
    gateway_name: &str,
    url_base: &str,
    config: &ClientConfig,
    rest_client: Py<RestClient>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // 1. 签名阶段 - 通过 PythonExecutor
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
    
    // 用签名后的请求替换原请求
    {
        let mut request_guard = request_arc.write().await;
        *request_guard = signed_request;
    }

    // 2. 获取超时时间
    let timeout_duration = {
        let request_guard = request_arc.read().await;
        let timeout_ms = tokio::task::block_in_place(|| {
            Python::attach(|py| request_guard.borrow(py).timeout_ms)
        });
        Duration::from_millis(timeout_ms)
    };

    let mut retry_count = 0;
    loop {
        let result = timeout(
            timeout_duration,
            execute_request_async_internal(client, &request_arc, gateway_name, url_base, config)
        ).await;

        match result {
            Ok(Ok((status_code, response_text, json_body, response_headers))) => {
                
                // 特殊处理502状态码
                if status_code == 502 {
                    let path = {
                        let request_guard = request_arc.read().await;
                        tokio::task::block_in_place(|| {
                            Python::attach(|py| request_guard.borrow(py).path.clone())
                        })
                    };
                    
                    let msg = format!(
                        "交易接口：{}，REST API请求失败，请求地址：{}{}，错误代码：{}，错误信息：{}",
                        gateway_name, url_base, path, status_code, response_text
                    );
                    PYTHON_EXECUTOR.write_log(msg);
                    PYTHON_EXECUTOR.save_connection_status(gateway_name.to_string(), false);
                    return Ok(());
                }
                
                // 更新请求状态并获取回调信息
                let is_success = status_code / 100 == 2;
                
                if is_success {
                    let request_guard = request_arc.read().await;
                    let request_clone = tokio::task::block_in_place(|| {
                        Python::attach(|py| request_guard.clone_ref(py))
                    });
                    drop(request_guard);
                    
                    let (callback_opt, request_py) = PYTHON_EXECUTOR
                        .update_request_success_async(request_clone, status_code, response_text.clone(), response_headers)
                        .await?;
                    
                    if let Some(callback) = callback_opt {
                        PYTHON_EXECUTOR.callback_async(callback, json_body, request_py).await;
                    }
                } else {
                    let request_guard = request_arc.read().await;
                    let request_clone = tokio::task::block_in_place(|| {
                        Python::attach(|py| request_guard.clone_ref(py))
                    });
                    drop(request_guard);
                    
                    let (_, on_failed_opt, should_handle_failed, request_py) = PYTHON_EXECUTOR
                        .update_request_failed_async(request_clone, status_code, response_text.clone(), response_headers)
                        .await?;
                    
                    if let Some(on_failed) = on_failed_opt {
                        PYTHON_EXECUTOR.on_failed_async(on_failed, status_code, request_py).await;
                    } else if should_handle_failed {
                        PYTHON_EXECUTOR.handle_failed_response_async(
                            request_py, 
                            status_code, 
                            gateway_name.to_string(), 
                            response_text.clone()
                        ).await;
                    }
                }
                
                break;
            }
            Ok(Err(e)) => {
                retry_count += 1;
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，请求执行失败 (重试 {}/{})", 
                    gateway_name, retry_count, config.max_retries
                ));
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，错误信息：{}", gateway_name, e
                ));
                
                if retry_count >= config.max_retries {
                    let request_guard = request_arc.read().await;
                    let request_clone = tokio::task::block_in_place(|| {
                        Python::attach(|py| request_guard.clone_ref(py))
                    });
                    drop(request_guard);
                    
                    let (on_error_opt, request_py) = PYTHON_EXECUTOR
                        .update_request_error_async(request_clone)
                        .await?;
                    
                    let error_msg = e.to_string();
                    if let Some(on_error) = on_error_opt {
                        PYTHON_EXECUTOR.on_error_async(
                            on_error, 
                            "Exception".to_string(), 
                            error_msg, 
                            Some(request_py)
                        ).await;
                    } else {
                        PYTHON_EXECUTOR.handle_error_response_async(
                            request_py,
                            error_msg,
                            gateway_name.to_string()
                        ).await;
                        PYTHON_EXECUTOR.save_connection_status(gateway_name.to_string(), false);
                    }
                    
                    break;
                } else {
                    tokio::time::sleep(Duration::from_millis(config.retry_delay_ms)).await;
                    
                    // 增加重试计数
                    let request_guard = request_arc.read().await;
                    tokio::task::block_in_place(|| {
                        Python::attach(|py| {
                            request_guard.borrow(py).increment_retry();
                        })
                    });
                }
            }
            Err(_) => {
                retry_count += 1;
                
                if retry_count >= config.max_retries {
                    let request_guard = request_arc.read().await;
                    let request_clone = tokio::task::block_in_place(|| {
                        Python::attach(|py| request_guard.clone_ref(py))
                    });
                    drop(request_guard);
                    
                    let (on_error_opt, request_py) = PYTHON_EXECUTOR
                        .update_request_error_async(request_clone)
                        .await?;
                    
                    if let Some(on_error) = on_error_opt {
                        PYTHON_EXECUTOR.on_error_async(
                            on_error, 
                            "TimeoutException".to_string(), 
                            "Request timeout".to_string(), 
                            Some(request_py)
                        ).await;
                    } else {
                        PYTHON_EXECUTOR.handle_error_response_async(
                            request_py,
                            "Request timeout".to_string(),
                            gateway_name.to_string()
                        ).await;
                    }
                    
                    break;
                } else {
                    tokio::time::sleep(Duration::from_millis(config.retry_delay_ms)).await;
                    
                    let request_guard = request_arc.read().await;
                    tokio::task::block_in_place(|| {
                        Python::attach(|py| {
                            request_guard.borrow(py).increment_retry();
                        })
                    });
                }
            }
        }
    }

    Ok(())
}


/// 执行请求内部实现 - 重构版本（使用 block_in_place 处理 Python 操作）
async fn execute_request_async_internal(
    client: &Client,
    request_arc: &Arc<RwLock<Py<Request>>>,
    gateway_name: &str,
    url_base: &str,
    _config: &ClientConfig,
) -> Result<(u16, String, Value, IndexMap<String, String>), Box<dyn std::error::Error + Send + Sync>> { 
    
    // 使用 block_in_place 提取数据，避免饿死 Tokio 运行时
    let (url, method, headers_data, query_params, body_data, is_jsonrpc) = {
        let request_guard = request_arc.read().await;
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                extract_request_data_impl(py, &request_guard, url_base, gateway_name)
            })
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
        _ => {
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，警告: 未知的HTTP方法 '{}', 使用GET", gateway_name, method
            ));
            reqwest::Method::GET
        }
    };

    let mut req_builder = client.request(http_method.clone(), url);

    for (k, v) in headers_data.iter() {
        match (
            k.parse::<reqwest::header::HeaderName>(),
            reqwest::header::HeaderValue::from_str(&v)
        ) {
            (Ok(name), Ok(value)) => {
                req_builder = req_builder.header(name, value);
            }
            (Err(e), _) => {
                return Err(format!("Invalid header name '{}': {}", k, e).into());
            }
            (_, Err(e)) => {
                return Err(format!("Invalid header value for '{}': {}", k, e).into());
            }
        }
    }

    if !query_params.is_empty() {
        req_builder = req_builder.query(&query_params);
    }

    if let Some(data) = body_data {
        if is_jsonrpc {
            match serde_json::from_str::<Value>(&data) {
                Ok(json_value) => {
                    req_builder = req_builder.json(&json_value);
                }
                Err(e) => {
                    PYTHON_EXECUTOR.write_log(format!(
                        "交易接口：{}，JSON解析失败: {}, 使用原始字符串", gateway_name, e
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

    let response = match req_builder.send().await {
        Ok(resp) => resp,
        Err(e) => {
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，✗ 请求发送失败!", gateway_name
            ));
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，错误类型: {:?}", gateway_name, e
            ));
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，错误信息: {}", gateway_name, e
            ));
            return Err(Box::new(e));
        }
    };
    
    let status_code = response.status().as_u16();

    let mut response_headers = IndexMap::new();
    for (name, value) in response.headers().iter() {
        if let Ok(value_str) = value.to_str() {
            response_headers.insert(name.as_str().to_string(), value_str.to_string());
        }
    }
    
    let response_text = match response.text().await {
        Ok(text) => text,
        Err(e) => {
            PYTHON_EXECUTOR.write_log(format!(
                "交易接口：{}，✗ 响应body读取失败: {}", gateway_name, e
            ));
            return Err(Box::new(e));
        }
    };

    let json_body = if status_code == 204 || response_text.trim().is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        match serde_json::from_str(&response_text) {
            Ok(json) => json,
            Err(e) => {
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，✗ JSON解析失败: {}, 返回包含原始文本的对象", gateway_name, e
                ));
                PYTHON_EXECUTOR.write_log(format!(
                    "交易接口：{}，原始响应文本: {}", gateway_name, response_text
                ));
                let mut map = serde_json::Map::new();
                map.insert("text".to_string(), Value::String(response_text.clone()));
                Value::Object(map)
            }
        }
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
                let filter_msg = vec![
                    "Endpoint request timeout. ",
                    "No need to change position side.",
                ];
                if filter_msg.contains(&msg) {
                    return Ok(());
                }
            }
        }
        Err(_) => {
            let request = request_guard.bind(py).borrow();
            let path = request.path.clone();
            
            call_write_log(
                py,
                &format!(
                    "交易接口：{}，REST API解码json数据出错，错误代码：{}，\n请求路径：{}，\n收到数据：{}",
                    gateway_name, status_code, path, response_text
                ),
            )?;
            return Ok(());
        }
    }

    let request = request_guard.bind(py).borrow();
    let path = request.path.clone();
    let request_str = request.__str__(py);
    
    call_write_log(
        py,
        &format!(
            "交易接口：{}，REST API请求失败代码：{}，请求路径：{}，完整请求：{}",
            gateway_name, status_code, path, request_str
        ),
    )?;

    Ok(())
}

fn handle_error_response(
    py: Python,
    error_msg: &str,
    request_guard: &Py<Request>,
    gateway_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let request = request_guard.bind(py).borrow();
    let request_str = request.__str__(py);
    
    let text = format!(
        "交易接口：{}，Unhandled RestClient Error：Exception\nrequest：{}\nException trace：\n{}\n",
        gateway_name,
        request_str,
        error_msg
    );
    
    call_write_log(py, &text)?;
    Ok(())
}



fn call_write_log(py: Python, msg: &str) -> PyResult<()> {
    let utility = py.import("vnpy.trader.utility")?;
    let write_log = utility.getattr("write_log")?;
    write_log.call1((msg,))?;
    Ok(())
}

fn call_save_connection_status(py: Python, gateway_name: &str, status: bool) -> PyResult<()> {
    let utility = py.import("vnpy.trader.utility")?;
    let save_status = utility.getattr("save_connection_status")?;
    save_status.call1((gateway_name, status))?;
    Ok(())
}

fn json_to_pyobject(py: Python, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_to_pyobject(py, v)?)?;
            }
            Ok(dict.unbind().into_any())
        }
        Value::Array(arr) => {
            let items: Vec<Py<PyAny>> = arr
                .iter()
                .map(|v| json_to_pyobject(py, v))
                .collect::<PyResult<Vec<_>>>()?;
            let list = PyList::new(py, &items)?;
            Ok(list.unbind().into_any())
        }
        Value::String(s) => {
            let py_str = PyString::new(py, s);
            Ok(py_str.unbind().into_any())
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                let bound = i.into_pyobject(py)?;
                Ok(bound.to_owned().unbind().into_any())
            } else if let Some(f) = n.as_f64() {
                let bound = f.into_pyobject(py)?;
                Ok(bound.to_owned().unbind().into_any())
            } else {
                Ok(py.None())
            }
        }
        Value::Bool(b) => {
            let bound = b.into_pyobject(py)?;
            Ok(bound.to_owned().unbind().into_any())
        }
        Value::Null => Ok(py.None()),
    }
}

fn pythondict_to_json_string(dict: &Bound<PyDict>) -> PyResult<String> {
    let mut map = serde_json::Map::new();
    for (key, value) in dict.iter() {
        let k = key.extract::<String>()?;
        let v = pyany_to_json_value(&value)?;
        map.insert(k, v);
    }
    Ok(serde_json::to_string(&Value::Object(map)).unwrap())
}

fn pyany_to_json_value(obj: &Bound<PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        Ok(Value::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(Value::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(Value::Number(i.into()))
    } else if let Ok(f) = obj.extract::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            Ok(Value::Number(n))
        } else {
            Ok(Value::Null)
        }
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(Value::String(s))
    } else if let Ok(list) = obj.cast::<PyList>() {
        let mut array = Vec::new();
        for item in list.iter() {
            let v = pyany_to_json_value(&item)?;
            array.push(v);
        }
        Ok(Value::Array(array))
    } else if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            let k = key.extract::<String>()?;
            let v = pyany_to_json_value(&value)?;
            map.insert(k, v);
        }
        Ok(Value::Object(map))
    } else {
        Ok(Value::String(obj.str()?.to_string()))
    }
}

#[pyclass]
pub struct PyResponseObject {
    #[pyo3(get)]
    status_code: u16,
    #[pyo3(get)]
    text: String,
    #[pyo3(get)]
    headers: Py<PyDict>,
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
