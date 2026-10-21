#!/bin/sh
# 容器入口：以 root 起步做最小化的目录准备，然后降权到 app(10001) 运行。
#
# 为什么需要这一步：./auths、./data 是 bind mount，宿主目录的属主往往不是 10001，
# 容器内的 app 用户会写不进去（state.json / 凭证刷新都会失败）。
# 这里只在「确实写不进去」时才 chown，避免无谓改写宿主属主。
set -e

mkdir -p /app/auths /app/data /app/data/logs

if ! su-exec app:app test -w /app/auths 2>/dev/null; then
  chown -R app:app /app/auths || true
fi
if ! su-exec app:app test -w /app/data 2>/dev/null; then
  chown -R app:app /app/data || true
fi

# config 文件不存在时用镜像内置默认（只读挂载/空挂载都能跑起来）
CFG="${TW2A_CONFIG:-/app/config.json}"
if [ ! -f "$CFG" ]; then
  echo "[entrypoint] config $CFG not found, fallback to built-in /app/config.example.json" >&2
  CFG=/app/config.example.json
fi

exec su-exec app:app /app/tw2api -config "$CFG" "$@"
