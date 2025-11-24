use pyo3::prelude::*;
use pyo3::types::{PyDict, PyBytes, PyList, PyString};
use pyo3::exceptions::PyRuntimeError;
use reqwest::Client;
use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};
use tokio::sync::{mpsc, Semaphore, RwLock, oneshot};
use tokio::time::{timeout, Duration, Instant};
use chrono::Local;
use serde_json::Value;
use futures::future::join_all;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use indexmap::IndexMap;

// 全局连接池和客户端缓存
static CLIENT_POOL: Lazy<DashMap<String, Arc<Client>>> = Lazy::new(|| DashMap::new());

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

    fn __str__(&self) -> String {
        let status_code = self.status_code.unwrap_or(0);
        
        let response_text = self.response_text.as_deref().unwrap_or("");
        
        // Format headers, params, and data for display
        let headers_str = if let Some(ref headers) = self.headers {
            Python::attach(|py| {
                headers.bind(py).repr().map(|s| s.to_string()).unwrap_or_else(|_| "{}".to_string())
            })
        } else {
            "None".to_string()
        };
        
        let params_str = if let Some(ref params) = self.params {
            Python::attach(|py| {
                params.bind(py).repr().map(|s| s.to_string()).unwrap_or_else(|_| "{}".to_string())
            })
        } else {
            "None".to_string()
        };
        
        let data_str = if let Some(ref data) = self.data {
            Python::attach(|py| {
                data.bind(py).repr().map(|s| s.to_string()).unwrap_or_else(|_| "None".to_string())
            })
        } else {
            "None".to_string()
        };
        
        format!(
            "request：{} {} {} because {}：\nheaders：{}\nparams：{}\ndata：{}\nresponse：{}\n",
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
    batch_size: usize
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            // 最大连接数（通常指连接池的大小），限制客户端同时保持的 TCP 连接数量
            max_connections: 100,
            
            // 最大并发请求数，限制同时正在执行的异步请求任务数量（通常用于信号量控制以防止系统过载）
            max_concurrent_requests: 1000,
            
            // 请求超时时间（毫秒），指从发送请求到接收完响应数据的总最长等待时间（此处为5秒）
            request_timeout_ms: 5000,
            
            // 连接超时时间（毫秒），指与服务器建立 TCP 连接（握手）的最长等待时间（此处为5秒）
            connect_timeout_ms: 5000,
            
            // 连接池空闲超时时间（毫秒），指一个空闲连接在被关闭前可以在池中保留的时间（此处为5秒）
            pool_timeout_ms: 5000,
            
            // 最大重试次数，当请求失败（如超时或网络错误）时尝试重新发送的次数
            max_retries: 3,
            
            // 重试延迟时间（毫秒），在下一次重试之前等待的时间间隔（此处为1秒）
            retry_delay_ms: 1000,
            // 批处理大小
            batch_size:50,
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
}

/// Python 操作执行器
struct PythonExecutor {
    task_tx: mpsc::UnboundedSender<PythonTask>,
    _handle: tokio::task::JoinHandle<()>,
}

impl PythonExecutor {
    fn new() -> Self {
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<PythonTask>();
        
        let handle = tokio::spawn(async move {
            // 创建专用的线程池用于 Python 操作
            let python_pool = std::sync::Arc::new(
                threadpool::ThreadPool::with_name("python-ops".to_string(), 4)
            );
            
            while let Some(task) = task_rx.recv().await {
                let pool = python_pool.clone();
                
                match task {
                    PythonTask::Sign { client, request, response_tx } => {
                        pool.execute(move || {
                            let result = Python::attach(|py| -> PyResult<Py<Request>> {
                                
                                let result = client.bind(py).call_method1("sign", (request.bind(py),))?;
                                let signed_request = result.extract::<Py<Request>>()?;
                                
                                Ok(signed_request)
                            });
                            
                            let _ = response_tx.send(result);
                        });
                    },
                    PythonTask::Callback { callback, data, request } => {
                        pool.execute(move || {
                            let _ = Python::attach(|py| -> PyResult<()> {
                                let py_dict = json_to_pyobject(py, &data)?;
                                callback.bind(py).call1((py_dict, request.bind(py)))?;
                                Ok(())
                            });
                        });
                    },
                    PythonTask::OnFailed { callback, status_code, request } => {
                        pool.execute(move || {
                            let _ = Python::attach(|py| -> PyResult<()> {
                                callback.bind(py).call1((status_code, request.bind(py)))?;
                                Ok(())
                            });
                        });
                    },
                    PythonTask::OnError { callback, exception_type, exception_value, request } => {
                        pool.execute(move || {
                            let _ = Python::attach(|py| -> PyResult<()> {
                                if let Some(req) = request {
                                    callback.bind(py).call1((
                                        py.get_type::<pyo3::exceptions::PyException>(),
                                        exception_value,
                                        py.None(),
                                        req.bind(py)
                                    ))?;
                                } else {
                                    callback.bind(py).call1((
                                        py.get_type::<pyo3::exceptions::PyException>(),
                                        exception_value,
                                        py.None(),
                                        py.None()
                                    ))?;
                                }
                                Ok(())
                            });
                        });
                    },
                }
            }
        });
        
        Self {
            task_tx,
            _handle: handle,
        }
    }
    
    /// 异步调用 Python sign 方法
    async fn sign_async(&self, client: Py<RestClient>, request: Py<Request>) -> PyResult<Py<Request>> {
        let (response_tx, response_rx) = oneshot::channel();
        
        self.task_tx.send(PythonTask::Sign {
            client,
            request,
            response_tx,
        }).map_err(|_| PyRuntimeError::new_err("发送签名任务失败"))?;
        
        response_rx.await
            .map_err(|_| PyRuntimeError::new_err("未收到签名响应"))?
    }
    
    /// 异步调用 Python callback
    async fn callback_async(&self, callback: Py<PyAny>, data: Value, request: Py<Request>) {
        let _ = self.task_tx.send(PythonTask::Callback {
            callback,
            data,
            request,
        });
    }
    
    /// 异步调用 Python on_failed
    async fn on_failed_async(&self, callback: Py<PyAny>, status_code: u16, request: Py<Request>) {
        let _ = self.task_tx.send(PythonTask::OnFailed {
            callback,
            status_code,
            request,
        });
    }
    
    /// 异步调用 Python on_error  
    async fn on_error_async(&self, callback: Py<PyAny>, exception_type: String, exception_value: String, request: Option<Py<Request>>) {
        let _ = self.task_tx.send(PythonTask::OnError {
            callback,
            exception_type,
            exception_value,
            request,
        });
    }
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
    proxies: Option<IndexMap<String, String>>,  // HashMap -> IndexMap
    self_py: Option<Py<RestClient>>,
}

#[pymethods]
impl RestClient {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<pyo3::types::PyTuple>, _kwargs: Option<&Bound<PyDict>>) -> PyResult<Self> {
        let config = ClientConfig::default();
        
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(std::cmp::min(num_cpus::get(), 8))
                .thread_name("rest-client")
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?
        );

        // 确保 semaphore 正确初始化
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_requests));
        
        // 验证 semaphore 初始化
        let available = semaphore.available_permits();
        let _ = Python::attach(|py| {
            call_write_log(py, &format!(
                "Semaphore初始化完成，可用许可: {}", 
                available
            ))
        });

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
        
        // 设置代理
        if !proxy_host.is_empty() && proxy_port > 0 {
            let proxy = format!("http://{}:{}", proxy_host, proxy_port);
            let mut proxies = IndexMap::new();  // HashMap -> IndexMap
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

        // 创建并缓存HTTP客户端
        let client = self.runtime.block_on(async {
            create_simple_client(
                if !proxy_host.is_empty() && proxy_port > 0 {
                    Some(format!("http://{}:{}", proxy_host, proxy_port))
                } else {
                    None
                },
                &self.config,
                gateway_name,  // 添加此参数
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
            let _ = Python::attach(|py| {
                call_write_log(py, &format!(
                    "交易接口：{}，REST客户端已在运行中，跳过启动", 
                    gateway_name
                ))
            });
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
        
        // 存储 self 的 Python 引用
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
            // 直接发送未签名的请求，签名将在异步worker中完成
            let request_arc = Arc::new(RwLock::new(request.clone_ref(py)));
            sender.send(request_arc).map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to send request: {}", e))
            })?;
        }

        Ok(request)
    }

    fn sign<'py>(&self, request: Bound<'py, Request>) -> PyResult<Bound<'py, Request>> {
        // 基础实现 - 子类应该重写此方法
        Ok(request)
    }

    // 在 RestClient 的 #[pymethods] 块中添加以下方法
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
        
        // === 关键修改：使用 allow_threads 替代 detach ===
        py.detach(move || {
            runtime.block_on(async {
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

                // 获取客户端
                let client = match CLIENT_POOL.get(&client_key) {
                    Some(c) => c.clone(),
                    None => {
                        let error_msg = "HTTP client not found";
                        let _ = Python::attach(|py| {
                            let _= call_write_log(py, &format!("交易接口：{}，REST API创建出错，错误信息：{}，重启交易子进程", gateway_name, error_msg));
                            call_save_connection_status(py, &gateway_name, false)
                        });
                        return Err(PyRuntimeError::new_err(error_msg));
                    }
                };

                // 执行请求（带重试）
                let mut retry_count = 0;
                loop {
                    // 提取请求数据
                    let (url, req_method, headers_data, query_params, body_data) = Python::attach(|py| {
                        let request_ref = signed_request.borrow(py);
                        let path = request_ref.path.clone();
                        let req_method = request_ref.method.clone();
                        let url = format!("{}{}", url_base, path);
                        
                        // 提取 headers
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
                        
                        // 提取 params
                        let query_params: Vec<(String, String)> = if let Some(params_py) = &request_ref.params {
                            let params_obj = params_py.bind(py);
                            if let Ok(params_dict) = params_obj.cast::<PyDict>() {
                                if params_dict.len() > 0 {
                                    params_dict.iter()
                                        .filter_map(|(key, value)| {
                                            let k = key.extract::<String>().ok()?;
                                            let v_str = if let Ok(v) = value.extract::<String>() {
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
                                            Some((k, v_str))
                                        })
                                        .collect()
                                } else {
                                    vec![]
                                }
                            } else {
                                vec![]
                            }
                        } else {
                            vec![]
                        };
                        
                        // 提取 body data
                        let body_data = if let Some(data_py) = &request_ref.data {
                            let data_obj = data_py.bind(py);
                            if let Ok(data_str) = data_obj.extract::<String>() {
                                if !data_str.is_empty() {
                                    Some(data_str)
                                } else {
                                    None
                                }
                            } else if let Ok(data_dict) = data_obj.cast::<PyDict>() {
                                if data_dict.len() > 0 {
                                    pythondict_to_json_string(data_dict).ok()
                                } else {
                                    None
                                }
                            } else if let Ok(data_bytes) = data_obj.cast::<PyBytes>() {
                                let bytes = data_bytes.as_bytes();
                                if !bytes.is_empty() {
                                    Some(String::from_utf8_lossy(bytes).to_string())
                                } else {
                                    None
                                }
                            } else {
                                data_obj.str().ok().map(|s| s.to_string())
                            }
                        } else {
                            None
                        };
                        (url, req_method, headers_data, query_params, body_data)
                    });
                    
                    // 执行 HTTP 请求
                    let result = timeout(
                        Duration::from_millis(config.request_timeout_ms),
                        execute_request_with_data(&client, &req_method, &url, headers_data, query_params, body_data, &gateway_name)
                    ).await;
                    
                    match result {
                        Ok(Ok((status_code, response_text, _json_body, response_headers))) => {
                            // 请求成功，创建响应对象
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
                                let error_msg = format!("经过{}次重试后REST API连接失败，错误信息：{}",config.max_retries, e);
                                let _ = Python::attach(|py| {
                                    let _= call_write_log(py, &format!("交易接口：{}，REST API连接出错，错误信息：{}，重启交易子进程", gateway_name, error_msg));
                                    call_save_connection_status(py, &gateway_name, false)
                                });
                                return Err(PyRuntimeError::new_err(error_msg));
                            }
                            tokio::time::sleep(Duration::from_millis(config.retry_delay_ms)).await;
                        }
                        Err(_) => {
                            retry_count += 1;
                            if retry_count >= config.max_retries {
                                let error_msg = format!("请求超时，重试 {} 次后仍未成功", config.max_retries);
                                let _ = Python::attach(|py| {
                                    let _= call_write_log(py, &format!("交易接口：{}，REST API连接出错，错误信息：{}，重启交易子进程", gateway_name, error_msg));
                                    call_save_connection_status(py, &gateway_name, false)
                                });
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
            // 尝试解码JSON，就像Python中的实现
            match serde_json::from_str::<Value>(response_text) {
                Ok(data) => {
                    if let Some(msg) = data.get("msg").and_then(|v| v.as_str()) {
                        //过滤OKX请求超时错误和币安重复设置持仓模式错误
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
                    // JSON解码错误，记录日志
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
                self.gateway_name, status_code, req.path, req.__str__()
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
        let text = self.exception_detail(exception_type, exception_value, request);
        call_write_log(py, &text)?;
        Ok(())
    }

    fn exception_detail(
        &self,
        exception_type: &str,
        exception_value: &str,
        request: Option<&Bound<Request>>,
    ) -> String {
        let now = Local::now().format("%Y-%m-%dT%H:%M:%S%.6f");
        let mut text = format!(
            "[{}]：Unhandled RestClient Error：{}\n",
            now, exception_type
        );
        
        if let Some(req) = request {
            text.push_str(&format!("request:{}\n", req.borrow().__str__()));
        }
        
        text.push_str(&format!("Exception trace: \n{}\n", exception_value));
        text
    }
}



/// 创建简化的HTTP客户端
async fn create_simple_client(
    proxy: Option<String>,
    config: &ClientConfig,
    gateway_name: &str,  // 添加此参数
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
                let msg = format!("交易接口：{}，✗ 代理配置失败: {}, 继续不使用代理", gateway_name, e);
                let _ = Python::attach(|py| call_write_log(py, &msg));
            }
        }
    }

    match builder.build() {
        Ok(client) => {

            Ok(client)
        }
        Err(e) => {
            let error_msgs = vec![
                format!("交易接口：{}，✗ HTTP客户端创建失败!", gateway_name),
                format!("交易接口：{}，错误详情: {:?}", gateway_name, e),
                format!("交易接口：{}，错误信息: {}", gateway_name, e),
                format!("交易接口：{}，客户端创建失败 \n", gateway_name),
            ];
            let _ = Python::attach(|py| {
                for msg in &error_msgs {
                    let _ = call_write_log(py, msg);
                }
            });
            Err(PyRuntimeError::new_err(format!("Failed to build HTTP client: {}", e)))
        }
    }
}

/// 高并发异步工作器
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
                    let _ = Python::attach(|py| {
                        call_write_log(py, &format!(
                            "交易接口：{}，接收通道已关闭，停止worker",
                            gateway_name
                        ))
                    });
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
                    let _ = Python::attach(|py| {
                        call_write_log(py, &format!(
                            "交易接口：{}，接收通道已关闭（批处理中），停止worker",
                            gateway_name
                        ))
                    });
                    break;
                }
                Err(_) => true,
            }
        };

        let should_process = should_process || last_batch_time.elapsed() >= BATCH_TIMEOUT;

        if should_process && !batch.is_empty() {

            // 按优先级排序
            batch.sort_by(|a, b| {
                let a_priority = Python::attach(|py| {
                    if let Ok(req) = a.try_read() {
                        req.borrow(py).priority
                    } else {
                        0
                    }
                });
                let b_priority = Python::attach(|py| {
                    if let Ok(req) = b.try_read() {
                        req.borrow(py).priority
                    } else {
                        0
                    }
                });
                b_priority.cmp(&a_priority)
            });

            if let Some(client) = CLIENT_POOL.get(&client_key) {
                let client = client.clone();
                // 这里不再需要等待任务结果，所以不需要收集 JoinHandle
                // 我们希望任务在后台"发射后不管"(fire-and-forget)，但内部会受信号量控制

                for request_arc in batch.drain(..) {
                    // 准备闭包需要的变量
                    let client_clone = client.clone();
                    let gateway_name_clone = gateway_name.clone();
                    let url_base_clone = url_base.clone();
                    let config_clone = config.clone();
                    let rest_client_clone = Python::attach(|py| rest_client.clone_ref(py));
                    // 关键修改：将 semaphore 的 clone 移入循环，并传入 spawn
                    let semaphore_clone = semaphore.clone();

                    tokio::spawn(async move {
                        // === 关键修改开始 ===
                        // 1. 在这里（子任务内）等待信号量，而不是在主循环中等待
                        // acquire_owned() 会挂起当前 task 直到有空闲槽位，但这不会阻塞 worker 线程
                        let _permit = match semaphore_clone.acquire_owned().await {
                            Ok(p) => p,
                            Err(e) => {
                                // 信号量被关闭（通常是程序退出时）
                                let _ = Python::attach(|py| {
                                    call_write_log(py, &format!(
                                        "交易接口：{}，信号量已关闭，丢弃请求: {}",
                                        gateway_name_clone, e
                                    ))
                                });
                                return;
                            }
                        };
                        // === 关键修改结束 ===

                        // 2. 获取到信号量后，才开始处理请求
                        // _permit 会在当前 scope 结束（即请求处理完毕）时自动 drop，释放槽位
                        if let Err(e) = process_request_async(
                            request_arc,
                            &client_clone,
                            &gateway_name_clone,
                            &url_base_clone,
                            &config_clone,
                            rest_client_clone,
                        ).await {
                            let msg = format!("交易所{}，异步request进程出错，错误信息：{}", gateway_name_clone, e);
                            let _= Python::attach(|py| call_write_log(py, &msg));
                        }
                    });
                }

            } else {
                // 客户端未找到日志
                let _ = Python::attach(|py| {
                    call_write_log(py, &format!(
                        "交易接口：{}，错误：HTTP客户端未找到，key: {}",
                        gateway_name,
                        client_key
                    ))
                });
                // 重新将请求放回队列
                if let Some(ref sender) = Python::attach(|py| {
                    rest_client.borrow(py).sender.clone()
                }) {
                    for request_arc in batch.drain(..) {
                        let _= sender.send(request_arc);
                    }
                }
            }

            last_batch_time = Instant::now();
        }
    }

}

/// 异步处理单个请求
async fn process_request_async(
    request_arc: Arc<RwLock<Py<Request>>>,
    client: &Client,
    gateway_name: &str,
    url_base: &str,
    config: &ClientConfig,
    rest_client: Py<RestClient>,  // 添加 RestClient 引用用于调用 sign
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // 首先对请求进行签名
    let signed_request = {
        let request_guard = request_arc.read().await;
        let request_py = Python::attach(|py| request_guard.clone_ref(py));
        
        // 异步调用 sign 方法
        match PYTHON_EXECUTOR.sign_async(rest_client, request_py).await {
            Ok(signed) => signed,
            Err(e) => {
                let msg = format!("交易接口：{}，签名失败：{}", gateway_name, e);
                let _ = Python::attach(|py| call_write_log(py, &msg));
                return Err(format!("Sign failed: {}", e).into());
            }
        }
    };
    
    // 用签名后的请求替换原请求
    {
        let mut request_guard = request_arc.write().await;
        *request_guard = signed_request;
    }

    let timeout_duration = {
        let request_guard = request_arc.read().await;
        Duration::from_millis(Python::attach(|py| {
            let request = request_guard.borrow(py);
            request.timeout_ms
        }))
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
                    let url_base_owned = url_base.to_string();
                    let gateway_name_owned = gateway_name.to_string();
                    tokio::task::spawn_blocking(move || {
                        Python::attach(|py| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                            let request_guard = tokio::task::block_in_place(|| {
                                tokio::runtime::Handle::current().block_on(request_arc.read())
                            });
                            let request = request_guard.borrow(py);
                            let msg = format!(
                                "交易接口：{}，REST API请求失败，请求地址：{}{}，错误代码：{}，错误信息：{}",
                                gateway_name_owned, 
                                url_base_owned,
                                request.path,
                                status_code, 
                                response_text
                            );
                            call_write_log(py, &msg)?;
                            call_save_connection_status(py, &gateway_name_owned, false)?;
                            Ok(())
                        })
                    }).await??;
                    return Ok(());
                }
                
                let request_guard = request_arc.write().await;
                
                let (callback_opt, on_failed_opt, should_handle_failed) = Python::attach(|py| -> Result<(Option<Py<PyAny>>, Option<Py<PyAny>>, bool), Box<dyn std::error::Error + Send + Sync>> {
                    let mut request = request_guard.borrow_mut(py);
                    request.status_code = Some(status_code);
                    request.response_text = Some(response_text.clone());

                    let headers_dict = PyDict::new(py);
                    for (key, value) in response_headers.iter() {
                        headers_dict.set_item(key, value)?;
                    }
                    
                    let response_obj = PyResponseObject {
                        status_code,
                        text: response_text.clone(),
                        headers: headers_dict.unbind(),
                    };
                    request.response = Some(Py::new(py, response_obj)?.into_any());

                    let is_success = status_code / 100 == 2;
                    
                    if is_success {
                        request.status = RequestStatus::Success;
                        Ok((request.callback.as_ref().map(|c| c.clone_ref(py)), None, false))
                    } else {
                        request.status = RequestStatus::Failed;
                        let has_on_failed = request.on_failed.is_some();
                        Ok((None, request.on_failed.as_ref().map(|f| f.clone_ref(py)), !has_on_failed))
                    }
                })?;
                
                let request_py = {
                    drop(request_guard);
                    let read_guard = request_arc.read().await;
                    Python::attach(|py| read_guard.clone_ref(py))
                };
                
                if let Some(callback) = callback_opt {
                    PYTHON_EXECUTOR.callback_async(callback, json_body, request_py).await;
                } else if let Some(on_failed) = on_failed_opt {
                    PYTHON_EXECUTOR.on_failed_async(on_failed, status_code, request_py).await;
                } else if should_handle_failed {
                    let gateway_name_owned = gateway_name.to_string();
                    tokio::task::spawn_blocking(move || {
                        Python::attach(|py| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                            handle_failed_response(py, status_code, &request_py, &gateway_name_owned, &response_text)?;
                            Ok(())
                        })
                    }).await??;
                }
                
                break;
            }
            Ok(Err(e)) => {
                retry_count += 1;
                let msg1 = format!("交易接口：{}，请求执行失败 (重试 {}/{})", gateway_name, retry_count, config.max_retries);
                let msg2 = format!("交易接口：{}，错误信息：{}", gateway_name, e);
                let _ = Python::attach(|py| {
                    let _ = call_write_log(py, &msg1);
                    let _ = call_write_log(py, &msg2);
                });
                if retry_count >= config.max_retries {
                    
                    let request_guard = request_arc.write().await;
                    
                    let on_error_opt = Python::attach(|py| -> Result<Option<Py<PyAny>>, Box<dyn std::error::Error + Send + Sync>> {
                        let mut request = request_guard.borrow_mut(py);
                        request.status = RequestStatus::Error;
                        Ok(request.on_error.as_ref().map(|e| e.clone_ref(py)))
                    })?;
                    
                    let request_py = {
                        drop(request_guard);
                        let read_guard = request_arc.read().await;
                        Python::attach(|py| read_guard.clone_ref(py))
                    };
                    
                    let error_msg = e.to_string();
                    if let Some(on_error) = on_error_opt {
                        let request_py_for_error = Python::attach(|py| request_py.clone_ref(py));
                        PYTHON_EXECUTOR.on_error_async(
                            on_error, 
                            "Exception".to_string(), 
                            error_msg.clone(), 
                            Some(request_py_for_error)
                        ).await;
                    } else {
                        let gateway_name_owned = gateway_name.to_string();
                        tokio::task::spawn_blocking(move || {
                            Python::attach(|py| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                                handle_error_response(py, &error_msg, &request_py, &gateway_name_owned)?;
                                call_save_connection_status(py, &gateway_name_owned, false)?;
                                Ok(())
                            })
                        }).await??;
                    }
                    
                    break;
                } else {
                    tokio::time::sleep(Duration::from_millis(config.retry_delay_ms)).await;
                    
                    let request_guard = request_arc.read().await;
                    Python::attach(|py| {
                        let request = request_guard.borrow(py);
                        request.increment_retry();
                    });
                }
            }
            Err(_) => {
                retry_count += 1;
                
                if retry_count >= config.max_retries {
                    
                    let request_guard = request_arc.write().await;
                    
                    let on_error_opt = Python::attach(|py| -> Result<Option<Py<PyAny>>, Box<dyn std::error::Error + Send + Sync>> {
                        let mut request = request_guard.borrow_mut(py);
                        request.status = RequestStatus::Error;
                        Ok(request.on_error.as_ref().map(|e| e.clone_ref(py)))
                    })?;
                    
                    let request_py = {
                        drop(request_guard);
                        let read_guard = request_arc.read().await;
                        Python::attach(|py| read_guard.clone_ref(py))
                    };
                    
                    if let Some(on_error) = on_error_opt {
                        let request_py_for_timeout = Python::attach(|py| request_py.clone_ref(py));
                        PYTHON_EXECUTOR.on_error_async(
                            on_error, 
                            "TimeoutException".to_string(), 
                            "Request timeout".to_string(), 
                            Some(request_py_for_timeout)
                        ).await;
                    } else {
                        let gateway_name_owned = gateway_name.to_string();
                        tokio::task::spawn_blocking(move || {
                            Python::attach(|py| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                                handle_error_response(py, "Request timeout", &request_py, &gateway_name_owned)?;
                                Ok(())
                            })
                        }).await??;
                    }
                    
                    break;
                } else {
                    tokio::time::sleep(Duration::from_millis(config.retry_delay_ms)).await;
                    
                    let request_guard = request_arc.read().await;
                    Python::attach(|py| {
                        let request = request_guard.borrow(py);
                        request.increment_retry();
                    });
                }
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
    config: &ClientConfig,
) -> Result<(u16, String, Value, IndexMap<String, String>), Box<dyn std::error::Error + Send + Sync>> { 
    
    let (url, method, headers_data, query_params, body_data) = {
        let request_guard = request_arc.read().await;
        Python::attach(|py| {
            let request = request_guard.borrow(py);
            
            let path = request.path.clone();
            let method = request.method.clone();
            let url = format!("{}{}", url_base, path);
            
            // 提取 headers - 保持顺序
            let headers_data: Vec<(String, String)> = if let Some(headers_py) = &request.headers {
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
            
            // 提取 params - 直接从 PyDict 提取，保持插入顺序
            let query_params: Vec<(String, String)> = if let Some(params_py) = &request.params {
                let params_obj = params_py.bind(py);
                if let Ok(params_dict) = params_obj.cast::<PyDict>() {
                    if params_dict.len() > 0 {
                        let params: Vec<_> = params_dict.iter()
                            .filter_map(|(key, value)| {
                                let k = key.extract::<String>().ok()?;
                                let v_str = if let Ok(v) = value.extract::<String>() {
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
                                Some((k, v_str))
                            })
                            .collect();
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

            // 提取 body data - 字典类型直接转JSON字符串，保持顺序
            let body_data = if let Some(data_py) = &request.data {
                let data_obj = data_py.bind(py);
                
                if let Ok(data_str) = data_obj.extract::<String>() {
                    if !data_str.is_empty() {
                        Some(data_str)
                    } else {
                        None
                    }
                } else if let Ok(data_dict) = data_obj.cast::<PyDict>() {
                    if data_dict.len() > 0 {
                        match pythondict_to_json_string(data_dict) {
                            Ok(json_str) => {
                                Some(json_str)
                            }
                            Err(e) => {
                                let msg = format!("交易接口：{}，Dict转JSON失败，错误信息： {}", gateway_name, e);
                                let _ = Python::attach(|py| call_write_log(py, &msg));
                                None
                            }
                        }
                    } else {
                        None
                    }
                } else if let Ok(data_bytes) = data_obj.cast::<PyBytes>() {
                    let bytes = data_bytes.as_bytes();
                    if !bytes.is_empty() {
                        let s = String::from_utf8_lossy(bytes).to_string();
                        Some(s)
                    } else {
                        None
                    }
                } else {
                    match data_obj.str() {
                        Ok(s) => {
                            let s_str = s.to_string();
                            if !s_str.is_empty() && s_str != "None" {
                                Some(s_str)
                            } else {
                                None
                            }
                        }
                        Err(e) => {
                            let msg = format!("交易接口：{}，request.data转换失败，错误信息：{}", gateway_name, e);
                            let _ = Python::attach(|py| call_write_log(py, &msg));
                            None
                        }
                    }
                }
            } else {
                None
            };

            (url, method, headers_data, query_params, body_data)
        })
    };

    execute_request_with_data(client, &method, &url, headers_data, query_params, body_data,gateway_name).await
}

async fn execute_request_with_data(
    client: &Client,
    method: &str,
    url: &str,
    headers_data: Vec<(String, String)>,
    query_params: Vec<(String, String)>,
    body_data: Option<String>,
    gateway_name: &str,  // 添加此参数
) -> Result<(u16, String, Value, IndexMap<String, String>), Box<dyn std::error::Error + Send + Sync>> {
    
    let http_method = match method.to_uppercase().as_str() {
        "GET" => reqwest::Method::GET,
        "POST" => reqwest::Method::POST,
        "PUT" => reqwest::Method::PUT,
        "DELETE" => reqwest::Method::DELETE,
        "PATCH" => reqwest::Method::PATCH,
        _ => {
            let msg = format!("交易接口：{}，警告: 未知的HTTP方法 '{}', 使用GET", gateway_name, method);
            let _ = Python::attach(|py| call_write_log(py, &msg));
            reqwest::Method::GET
        }
    };

    let mut req_builder = client.request(http_method.clone(), url);

    // 添加headers
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

    // 添加query参数
    if !query_params.is_empty() {
        req_builder = req_builder.query(&query_params);
    }

    // 处理body data
    if let Some(data) = body_data {
        if data.contains("jsonrpc") {
            match serde_json::from_str::<Value>(&data) {
                Ok(json_value) => {
                    req_builder = req_builder.json(&json_value);
                }
                Err(e) => {
                    let msg = format!("交易接口：{}，JSON解析失败: {}, 使用原始字符串", gateway_name, e);
                    let _ = Python::attach(|py| call_write_log(py, &msg));
                    req_builder = req_builder
                        .header("Content-Type", "application/json")
                        .body(data);
                }
            }
        } else {
            req_builder = req_builder
                .header("Content-Type", "application/json")
                .body(data);
        }
    } else {
    }

    // 发送请求
    let response = match req_builder.send().await {
        Ok(resp) => {
            resp
        }
        Err(e) => {
            let msgs = vec![
                format!("交易接口：{}，✗ 请求发送失败!", gateway_name),
                format!("交易接口：{}，错误类型: {:?}", gateway_name, e),
                format!("交易接口：{}，错误信息: {}", gateway_name, e),
            ];
            let _ = Python::attach(|py| {
                for msg in &msgs {
                    let _ = call_write_log(py, msg);
                }
            });
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
        Ok(text) => {
            text
        }
        Err(e) => {
            let msg = format!("交易接口：{}，✗ 响应body读取失败: {}", gateway_name, e);
            let _ = Python::attach(|py| call_write_log(py, &msg));
            return Err(Box::new(e));
        }
    };

    let json_body = if status_code == 204 || response_text.trim().is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        match serde_json::from_str(&response_text) {
            Ok(json) => {
                json
            }
            Err(e) => {
                let msgs = vec![
                    format!("交易接口：{}，✗ JSON解析失败: {}, 返回包含原始文本的对象", gateway_name, e),
                    format!("交易接口：{}，原始响应文本: {}", gateway_name, response_text),
                ];
                let _ = Python::attach(|py| {
                    for msg in &msgs {
                        let _ = call_write_log(py, msg);
                    }
                });
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
    // 处理JSON解码错误
    match serde_json::from_str::<Value>(response_text) {
        Ok(data) => {
            if let Some(msg) = data.get("msg").and_then(|v| v.as_str()) {
                //过滤OKX请求超时错误和币安重复设置持仓模式错误
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
            let (path, _request_str) = {
                let request = request_guard.bind(py).borrow();
                (request.path.clone(), request.__str__())
            };
            
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

    let (path, request_str) = {
        let request = request_guard.bind(py).borrow();
        (request.path.clone(), request.__str__())
    };
    
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
    _gateway_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = Local::now().format("%Y-%m-%dT%H:%M:%S%.6f");
    
    let request_str = {
        let request = request_guard.bind(py).borrow();
        request.__str__()
    };
    
    let text = format!(
        "[{}]：Unhandled RestClient Error：Exception\nrequest：{}\nException trace：\n{}\n",
        now,
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
        // Handle Python lists - convert to JSON arrays
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