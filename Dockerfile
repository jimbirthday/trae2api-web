# syntax=docker/dockerfile:1
# 多阶段构建：Go 编译 + alpine 运行时。
# 运行时以 root 起步（entrypoint 修 bind mount 属主）后立刻降权到 app（uid 10001）。
FROM golang:1.23-alpine AS build
WORKDIR /src
# 本仓库零第三方依赖：go.sum 不存在时通配符也不会报错。
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/tw2api ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata su-exec \
 && adduser -D -u 10001 app \
 && mkdir -p /app/auths /app/data \
 && chown -R app:app /app
WORKDIR /app
COPY --from=build /out/tw2api /app/tw2api
# 镜像内置一份默认配置（端口 8000、目录 /app/auths /app/data）；
# 需要自定义时在 compose 里把 ./config.json 挂到 /app/config.json 即可。
COPY config.example.json /app/config.json
COPY config.example.json /app/config.example.json
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENV TZ=Asia/Shanghai
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8000/healthz >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
