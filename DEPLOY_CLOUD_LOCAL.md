# 云端采集 + 本地回传部署命令

下面这套命令按这个目标设计：

- 云服务器持续运行采集器
- 云服务器每小时把已结束小时分区压缩成最终文件
- 本地机器只拉取压缩后的最终文件

把下面的变量先按你的实际环境修改，再逐段执行。

## 1. 统一变量

云服务器上使用这些变量：

```bash
export REPO_URL='<你的仓库地址>'
export CLOUD_REPO_DIR='/srv/Poly-Collector'
export CLOUD_DATA_ROOT='/var/lib/polymarket-books'
export CLOUD_SERVICE_NAME='polymarket-books-guard'
```

本地机器上使用这些变量：

```bash
export LOCAL_REPO_URL='<你的仓库地址>'
export LOCAL_REPO_DIR="$HOME/Poly-Collector"
export LOCAL_DATA_ROOT='/data/polymarket-books'
export CLOUD_SSH='youruser@your-server'
export CLOUD_REMOTE_DATA_ROOT='/var/lib/polymarket-books'
```

## 2. 云服务器初始化

安装系统依赖：

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv rsync git
```

拉代码并安装 Python 环境：

```bash
git clone "$REPO_URL" "$CLOUD_REPO_DIR"
cd "$CLOUD_REPO_DIR"
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install -e .
```

创建数据目录：

```bash
sudo mkdir -p "$CLOUD_DATA_ROOT"
sudo chown -R "$USER":"$USER" "$CLOUD_DATA_ROOT"
mkdir -p "$CLOUD_DATA_ROOT/state"
```

## 3. 云服务器手工试跑

先手工启动一次采集栈：

```bash
cd "$CLOUD_REPO_DIR"
./scripts/start_books_guard.sh \
  --data-root "$CLOUD_DATA_ROOT" \
  --python-exe "$CLOUD_REPO_DIR/.venv/bin/python" \
  --collector-arg=--write-shards \
  --collector-arg=64
```

检查进程和日志：

```bash
cat "$CLOUD_DATA_ROOT/state/guard.pid"
cat "$CLOUD_DATA_ROOT/state/supervisor.pid"
cat "$CLOUD_DATA_ROOT/state/stream.pid"
cat "$CLOUD_DATA_ROOT/state/monitor.pid"
tail -f "$CLOUD_DATA_ROOT/state/guard_stdout.log"
```

如果需要停止试跑：

```bash
cd "$CLOUD_REPO_DIR"
./scripts/stop_books_stack.sh --data-root "$CLOUD_DATA_ROOT"
```

## 4. 云服务器安装 systemd 守护

先停止第 3 步里手工拉起的 guard，避免和 `systemd` 托管进程冲突：

```bash
cd "$CLOUD_REPO_DIR"
./scripts/stop_books_stack.sh --data-root "$CLOUD_DATA_ROOT"
```

安装并启动服务：

```bash
cd "$CLOUD_REPO_DIR"
sudo ./scripts/install_books_systemd.sh \
  --service-name "$CLOUD_SERVICE_NAME" \
  --data-root "$CLOUD_DATA_ROOT" \
  --python-exe "$CLOUD_REPO_DIR/.venv/bin/python" \
  --collector-arg=--write-shards \
  --collector-arg=64
```

检查服务状态：

```bash
sudo systemctl status "${CLOUD_SERVICE_NAME}.service"
sudo systemctl restart "${CLOUD_SERVICE_NAME}.service"
sudo systemctl stop "${CLOUD_SERVICE_NAME}.service"
sudo systemctl start "${CLOUD_SERVICE_NAME}.service"
```

如果以后要卸载：

```bash
cd "$CLOUD_REPO_DIR"
sudo ./scripts/uninstall_books_systemd.sh --service-name "$CLOUD_SERVICE_NAME"
```

## 5. 云服务器安装 cron 封板压缩任务

编辑当前用户的 crontab：

```bash
crontab -e
```

加入这一行。作用是每小时 `05` 分压缩已经结束的小时分区：

如果你修改了仓库目录或数据目录，这里要同步替换成你的真实绝对路径。

```cron
5 * * * * cd /srv/Poly-Collector && /srv/Poly-Collector/.venv/bin/python scripts/finalize_hourly_shards.py --data-root /var/lib/polymarket-books >> /var/lib/polymarket-books/state/finalize_hourly.log 2>&1
```

手工验证一次压缩脚本：

```bash
cd "$CLOUD_REPO_DIR"
"$CLOUD_REPO_DIR/.venv/bin/python" scripts/finalize_hourly_shards.py \
  --data-root "$CLOUD_DATA_ROOT" \
  --dry-run
```

## 6. 本地机器初始化

安装依赖：

```bash
sudo apt-get update
sudo apt-get install -y rsync openssh-client git
```

拉代码：

```bash
git clone "$LOCAL_REPO_URL" "$LOCAL_REPO_DIR"
cd "$LOCAL_REPO_DIR"
```

创建本地数据目录：

```bash
sudo mkdir -p "$LOCAL_DATA_ROOT"
sudo chown -R "$USER":"$USER" "$LOCAL_DATA_ROOT"
mkdir -p "$LOCAL_DATA_ROOT/state/transfers"
```

## 7. 本地 SSH 免密

如果本地还没有 SSH key：

```bash
ssh-keygen -t ed25519
```

把公钥装到云服务器：

```bash
ssh-copy-id "$CLOUD_SSH"
```

先测试连通：

```bash
ssh "$CLOUD_SSH" "echo ok"
```

## 8. 本地手工试拉压缩后的最终文件

先做 dry-run：

```bash
cd "$LOCAL_REPO_DIR"
./scripts/pull_cloud_data.sh \
  --remote "${CLOUD_SSH}:${CLOUD_REMOTE_DATA_ROOT}" \
  --local-root "$LOCAL_DATA_ROOT" \
  --include all_market_meta.json \
  --include resolved_market \
  --include finalized \
  --dry-run
```

正式拉取：

```bash
cd "$LOCAL_REPO_DIR"
./scripts/pull_cloud_data.sh \
  --remote "${CLOUD_SSH}:${CLOUD_REMOTE_DATA_ROOT}" \
  --local-root "$LOCAL_DATA_ROOT" \
  --include all_market_meta.json \
  --include resolved_market \
  --include finalized
```

如果要限速，例如限制到 `20 MB/s` 左右：

```bash
cd "$LOCAL_REPO_DIR"
./scripts/pull_cloud_data.sh \
  --remote "${CLOUD_SSH}:${CLOUD_REMOTE_DATA_ROOT}" \
  --local-root "$LOCAL_DATA_ROOT" \
  --include all_market_meta.json \
  --include resolved_market \
  --include finalized \
  --bwlimit 20480
```

## 9. 本地安装 cron 拉取任务

编辑本地机器 crontab：

```bash
crontab -e
```

如果你每天只拉一次，建议加这一行。作用是每天 `02:15` 执行一次：

如果你修改了本地仓库目录、本地数据目录、云服务器用户名或主机名，这里要同步替换成你的真实绝对路径和 SSH 地址。

```cron
15 2 * * * cd /home/youruser/Poly-Collector && ./scripts/pull_cloud_data.sh --remote youruser@your-server:/var/lib/polymarket-books --local-root /data/polymarket-books --include all_market_meta.json --include resolved_market --include finalized >> /data/polymarket-books/state/transfers/pull.log 2>&1
```

如果你想每小时拉一次，建议加这一行。作用是每小时 `15` 分执行一次：

```cron
15 * * * * cd /home/youruser/Poly-Collector && ./scripts/pull_cloud_data.sh --remote youruser@your-server:/var/lib/polymarket-books --local-root /data/polymarket-books --include all_market_meta.json --include resolved_market --include finalized >> /data/polymarket-books/state/transfers/pull.log 2>&1
```

## 10. 常用检查命令

云服务器检查：

```bash
sudo systemctl status "${CLOUD_SERVICE_NAME}.service"
ls -lah "$CLOUD_DATA_ROOT/state"
tail -n 50 "$CLOUD_DATA_ROOT/state/guard_stdout.log"
tail -n 50 "$CLOUD_DATA_ROOT/state/stream_stdout.log"
tail -n 50 "$CLOUD_DATA_ROOT/state/finalize_hourly.log"
find "$CLOUD_DATA_ROOT/finalized" -type f | tail
```

本地检查：

```bash
ls -lah "$LOCAL_DATA_ROOT/state/transfers"
cat "$LOCAL_DATA_ROOT/state/transfers/pull_cloud_data_latest.json"
find "$LOCAL_DATA_ROOT/finalized" -type f | tail
tail -n 50 "$LOCAL_DATA_ROOT/state/transfers/pull.log"
```

## 11. 推荐执行顺序

按这个顺序做最稳：

1. 云服务器初始化
2. 云服务器手工试跑
3. 云服务器安装 `systemd`
4. 云服务器安装压缩 `cron`
5. 本地初始化
6. 本地 SSH 免密
7. 本地 `dry-run`
8. 本地正式拉取
9. 本地安装拉取 `cron`
