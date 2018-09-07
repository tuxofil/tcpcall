# Packet oriented transport - client for Golang

## Examples

### Request-Reply and Cast workflows

Create the server:

```go
package main

import (
    "log"
    "tcpcall"
    "time"
)

func main() {
    // configure the server
    serverCfg := tcpcall.NewServerConf()
    serverCfg.PortNumber = 9000
    serverCfg.RequestCallback = RequestCallback
    serverCfg.CastCallback = CastCallback
    serverCfg.MaxConnections = 100
    serverCfg.Concurrency = 10
    serverCfg.MaxRequestSize = 0xffff
    // start listening for connections
    server, err := tcpcall.Listen(serverCfg)
    if err != nil {
        log.Fatalf("server start failed: %s", err)
    }
    for { // loop forever
        time.Sleep(time.Hour)
    }
}

// Handler for incoming Request
func RequestCallback(packet []byte) (reply []byte) {
    log.Printf("REQUEST received: %v", packet)
    // here we just send packet back to the client
    return packet
}

// Handler for incoming Cast
func CastCallback(packet []byte) {
    // do something with the packet
    log.Printf("CAST received: %v", packet)
}
```

Start the client and communicate:

```go
package main

import (
    "log"
    "tcpcall"
)

func main() {
    // configure the connection pool
    poolCfg := tcpcall.NewPoolConf()
    poolCfg.PeersFetcher = func() []string {
        // you can add some more sophisticated
        // logic here and the pool will reconfigure
        // itself automatically, reconnecting to new
        // servers and disconnecting from old ones
        return []string{
            "127.0.0.1:9000",
        }
    }
    poolCfg.ReconfigPeriod = 10 * time.Second
    // connect to the server (or servers)
    pool := tcpcall.NewPool(poolCfg)

    // communicate with server, using Request-Reply
    packet := []byte("Hello, World!")
    reply, err := pool.Req(packet, 3 * time.Second)
    if err == nil {
        log.Printf("got reply: %v", reply)
    }else{
        log.Printf("request failed: %s", err)
    }

    // communicate, using Cast (send only)
    if err := pool.Cast(packet); err == nil {
        log.Printf("cast sent")
    }else{
        log.Printf("cast failed: %s", err)
    }
}
```
