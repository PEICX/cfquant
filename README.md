# cfquant

## 项目简介

> **基于 CFQuant 的个人定制版。非通用。原版见 www.cfquant.org**

### 官方资源

- 官网与问题反馈：[www.cfquant.org](https://cfquant.org)

## 第三方项目自动化接入

`cfquant.management.RuntimeManager` 支持隐藏启动后台服务、通过代码初始化及绑定账号、配置是否自动启动 QMT，并通过官网或 GitHub 更新服务和回滚。无需打开网页，后台服务使用独立目录，更新不会覆盖第三方进程已经导入的 SDK。

```python
from cfquant.management import RuntimeManager

runtime = RuntimeManager(home=r"D:\MyApp\cfquant")
runtime.start()  # 隐藏启动；遵循各账号保存的 QMT 启动设置
runtime.initialize(
    account_id="YOUR_ACCOUNT_ID", account_type="STOCK",
    qmt_dir=r"D:\QMT", mode="ctypes",
    auto_start_qmt=False, strategy_autorun=True, live=False,
)
# 用户自行启动并登录 QMT；首次部署前需关闭 QMT。
runtime.wait_ready("YOUR_ACCOUNT_ID", timeout=120)
runtime.configure_client()

# 用户关闭 QMT 后，可通过 runtime.updates.apply() 更新后台服务。
```

## cftrader 模拟账号实测

| 路径 | RPC 次数 | 提交耗时 | 单笔均摊 |
|---|---:|---:|---:|
| 批量同步 `order_stock_batch` | 1 | 963.252 ms | 9.6325 ms |
| 单笔同步循环 `order_stock` x100 | 100 | 23250.174 ms | 232.5017 ms |
| 批量异步 `order_stock_batch_async` | 1 | 52.578 ms | 0.5258 ms |
| 单笔异步循环 `order_stock_async` x100 | 100 | 2952.912 ms | 29.5291 ms |

## 项目交流群
<img src="docs/f5f1d14256e57023dd67513481f16718.jpg" alt="cfquant 项目交流群二维码" width="280" />


## 联系作者
- #### 地球号:shcfquant,请注明来意
- #### 邮箱:litaoflyme@163.com


## 许可证

本项目采用 [MIT License](LICENSE) 开源。
