package main

import (
	"log"
	"os/exec"
	"strconv"
	"tcpcall"
	"time"
)

func main() {
	log.Println("Started")
	defer log.Println("DONE")
	// Start server process
	cmd := exec.Command("./server.escript")
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	defer cmd.Process.Kill()
	// warming up
	time.Sleep(time.Second * 2)
	// Start tcpcall client
	c, err := tcpcall.Dial(":5000", tcpcall.NewClientConf())
	defer c.Close()
	if err != nil {
		log.Fatalf("failed to connect to server: %s", err)
	}
	// Communicate with server.
	// Make a few requests to the server in parallel.
	// Each request have different processing duration.
	results := make(chan int, 5)
	for i := cap(results) - 1; 0 <= i; i-- {
		go func(j int) {
			log.Printf(" req> %d...\n", j)
			resp, err := c.Req([]byte(strconv.Itoa(j)), time.Second*5)
			if err != nil {
				log.Fatalf("failed to make req %d: %v", j, err)
			}
			r, exterr := strconv.Atoi(string(resp))
			if exterr != nil {
				log.Fatalf("failed to parse reply for %d: %v", j, err)
			}
			if r != j+1 {
				log.Fatalf("bad reply for %d. expected %v but %v found\n", j, j+1, r)
			}
			results <- r
			log.Printf("   %d OK\n", j)
		}(i)
	}
	for i := 0; i < cap(results); i++ {
		select {
		case r := <-results:
			if r != i+1 {
				log.Fatalf("order violated. expected %d but %d found", i+1, r)
			}
		case <-time.After(time.Second * 3):
			log.Fatalf("timeout waiting for result #%d", i)
		}
	}
	log.Printf("Order is OK")
}
