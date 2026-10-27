# Pinned build/runtime inputs; refresh digests deliberately and rerun checks.
FROM golang:1.25-bookworm@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/server ./cmd/worker ./cmd/dbtool ./cmd/probe

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
WORKDIR /app
COPY --from=build /out/ /app/
USER 65532:65532
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/app/server"]
