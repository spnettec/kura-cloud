# kura-cloud 的 IDEA 测试配置

完整指南见 [`../../kura/docs/testing/idea-test-setup.md`](../../kura/docs/testing/idea-test-setup.md)
（前置要求、Maven 缓存、数据目录机制、如何新增配置）。本仓库已内置共享运行配置
`.run/`（clone 后 IDEA 自动加载）：

| 配置 | 类型 | 用途 |
| --- | --- | --- |
| Kura cloud core protocol runtime | JUnit | `CloudFactoryRuntimeIT.coreHandlersOverMqtt`，真实 Equinox/SCR 云连接运行时 |
| Kura cloud Sparkplug runtime | JUnit | `CloudFactoryRuntimeIT.sparkplugFactoryPipeline`，Sparkplug 管道（可断点调试） |
| Kura cloud TLS runtime | JUnit | `CloudFactoryRuntimeIT.sparkplugTlsFilesystemPipeline`，文件密钥库 TLS |
| Kura Sparkplug interrupted reconnect JUnit | JUnit | `SparkplugInterruptedReconnectTest`，中断重连回归单测 |

运行前置：kura 仓库已组装 `kura-dev-runtime/target/runtime`（或让 kura 项目先跑一次），
本仓库 Maven 导入正常；Maven 缓存缺 plc4x 时先在 `plc4x-yofc` 目录
`mvn install -DskipTests`。环境检查可执行 `../kura/tools/ide/setup-idea.sh`。
