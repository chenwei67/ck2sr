# 构建阶段
FROM golang:1.25.1-alpine AS builder

# 设置工作目录
WORKDIR /app

# 安装必要的包
RUN apk add --no-cache git ca-certificates tzdata

# 复制 go mod 文件
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 构建应用程序
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o ck2sr main.go

# 运行阶段
FROM alpine:latest

# 安装 CA 证书和时区数据
RUN apk --no-cache add ca-certificates tzdata

# 设置时区
ENV TZ=Asia/Shanghai

# 创建非 root 用户
RUN addgroup -g 1001 ck2sr && \
    adduser -D -s /bin/sh -u 1001 -G ck2sr ck2sr

# 设置工作目录
WORKDIR /app

# 创建数据目录
RUN mkdir -p /data && chown -R ck2sr:ck2sr /data

# 从构建阶段复制二进制文件
COPY --from=builder /app/ck2sr .

# 复制配置文件
COPY --from=builder /app/configs ./configs

# 设置权限
RUN chown -R ck2sr:ck2sr /app

# 切换到非 root 用户
USER ck2sr

# 暴露端口
EXPOSE 8080 8081

# 设置默认配置文件
ENV CONFIG_FILE=/app/configs/config.yaml

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8081/health || exit 1

# 启动命令
ENTRYPOINT ["./ck2sr"]
CMD ["-config", "/app/configs/config.yaml"]