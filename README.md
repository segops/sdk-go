# SegOps Go SDK

Behavioral segmentation SDK for Go services.

## Install

```bash
go get github.com/segops/sdk-go
```

## Usage

The Go SDK is server-side and authenticates directly with a secret key (`sk_…`).
Public keys and the browser session handshake are intended for client SDKs
(browser, iOS, Android); a Go service should use a secret key kept in its
environment.

```go
import "github.com/segops/sdk-go"

client := segops.New(segops.Options{
    APIURL: "https://api.segops.ai",
    APIKey: "sk_...",
})

client.Track(segops.Event{
    UserID:    "u-123",
    EventType: "order_placed",
    Payload:   map[string]any{"total": 42},
})

// On shutdown (flushes pending events):
client.Shutdown(context.Background())
```

## License

MIT
