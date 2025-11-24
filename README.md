
## 前置要求

### 安装 Rust

```bash
# Linux/macOS
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Windows
# 下载并运行 rustup-init.exe
# https://rustup.rs/
```

安装完成后，重启终端并验证安装：

```bash
rustc --version
cargo --version
```
## 安装步骤

```bash
pip install maturin
```
**生产构建：**
```bash
cd rust_rest_client_project
maturin build --release
pip install target/wheels/*.whl
```
## 使用示例
```
# vnpyt/apit/rest/__init__.py代码修改如下
#from .rest_client import Request, Response, RestClient
from rust_rest_client import Request, RestClient
# 交易接口初始化需要传递的参数，我增加了gateway_name参数方便定位接口错误
self.init(REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
```
