FROM --platform=$BUILDPLATFORM golang:1.25-alpine3.21 AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /app/fsb -ldflags="-w -s" ./cmd/fsb

FROM alpine:3.21
RUN apk add --no-cache ffmpeg ca-certificates tzdata
RUN adduser -D -u 1000 hfuser \
    && mkdir -p /app \
    && chown -R hfuser:hfuser /app

WORKDIR /app
COPY --from=builder --chown=hfuser:hfuser /app/fsb /app/fsb

USER 1000
ENV PORT=7860
EXPOSE 7860
ENTRYPOINT ["/app/fsb", "run"]
