# Makefile 使用说明

本文档说明当前 `Makefile` 的编译、安装、备份和回退用法。

## 核心变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PROJECT_NAME` | `log-track` | 编译产物名称，例如 `bin/log-track` |
| `APP_NAME` | `$(PROJECT_NAME)` | 部署服务名称，用于安装目录、目标文件名、备份文件名、supervisor 程序名 |
| `VERSION` | `dev` | 注入到程序版本信息 |
| `INSTALL_DIR` | `/usr/local/go-server/$(APP_NAME)` | 安装目录 |
| `INSTALL_PATH` | `$(INSTALL_DIR)/$(APP_NAME)` | 线上目标文件 |
| `SUPERVISOR_PROGRAM` | `$(APP_NAME)` | supervisor 程序名 |
| `COMMIT` | 空 | 仅 `make backoff` 使用，指定要恢复的 commit 备份 |

`PROJECT_NAME` 和 `APP_NAME` 默认相同。通常可以只使用默认值；如果编译产物名和线上服务名不同，可以分别指定。

## 编译

```bash
make build
```

默认编译到：

```text
bin/log-track
```

指定项目名：

```bash
make build PROJECT_NAME=order
```

会编译到：

```text
bin/order
```

指定版本：

```bash
make build VERSION=v1.2.0
```

编译时会注入以下信息：

```text
ProjectName
Version
GitCommit
GitMsg
BuildDate
```

查看二进制版本信息：

```bash
./bin/log-track -v
```

输出格式：

```text
project=log-track
version=dev
git_commit=b1849c8
git_msg=log-track
build_date=2026-06-25T02:01:42Z
app_env=dev
```

`app_env` 从 `APP_ENV` 环境变量读取，未设置时默认为 `dev`。

## 运行

```bash
make run
```

直接执行 `go run ./cmd/server`，不会生成 `bin/` 产物。也会注入版本信息，可通过：

```bash
make run ARGS=-v
```

查看。

## 安装

```bash
make install
```

安装前会要求确认：

```text
Install to ... ? [y/N]
```

输入 `y` 才会继续。

`make install` 只安装，不编译。安装前必须先执行：

```bash
make build
```

默认安装流程：

```text
1. 检查 bin/<PROJECT_NAME> 是否存在且可执行
2. 备份当前线上文件到 <APP_NAME>.<GIT_COMMIT>
3. 备份当前线上文件到 <APP_NAME>.last
4. supervisorctl stop <APP_NAME>
5. 复制 bin/<PROJECT_NAME> 到 /usr/local/go-server/<APP_NAME>/<APP_NAME>
6. chmod +x
7. supervisorctl start <APP_NAME>
```

默认项目 `log-track` 的备份文件为：

```text
/usr/local/go-server/log-track/log-track.<GIT_COMMIT>
/usr/local/go-server/log-track/log-track.last
```

示例：

```bash
make build
make install
```

指定服务名：

```bash
make build PROJECT_NAME=order
make install PROJECT_NAME=order APP_NAME=order
```

如果编译产物名和线上服务名不同：

```bash
make build PROJECT_NAME=order
make install PROJECT_NAME=order APP_NAME=order-api
```

此时会：

```text
复制 bin/order 到 /usr/local/go-server/order-api/order-api
停止/启动 supervisor 程序 order-api
生成备份 order-api.<GIT_COMMIT> 和 order-api.last
```

## 回退

### 回退到 last

```bash
make backoff
```

默认恢复：

```text
/usr/local/go-server/<APP_NAME>/<APP_NAME>.last
```

默认流程：

```text
1. 确认输入 y
2. 检查备份文件是否存在
3. supervisorctl stop <APP_NAME>
4. 复制备份文件到线上目标文件
5. chmod +x
6. supervisorctl start <APP_NAME>
```

### 回退到指定 commit

```bash
make backoff COMMIT=b1849c8
```

会恢复：

```text
/usr/local/go-server/<APP_NAME>/<APP_NAME>.b1849c8
```

指定服务名：

```bash
make backoff APP_NAME=order COMMIT=abc1234
```

会恢复：

```text
/usr/local/go-server/order/order.abc1234
```

## 清理

```bash
make clean
```

删除：

```text
bin/
```

## 查看将注入的版本信息

```bash
make version
```

输出当前将注入的构建信息：

```text
project_name
version
git_commit
git_msg
build_date
```

## 常用命令

当前项目默认使用：

```bash
make build
./bin/log-track -v
make install
make backoff
make backoff COMMIT=b1849c8
```
