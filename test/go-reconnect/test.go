package main

import (
	"log"
	"os/exec"
	"tcpcall"
	"time"
)

func main() {
	log.Println("Started")
	defer log.Println("DONE")

	// Start tcpcall client
	c, err := tcpcall.Dial(":5000", tcpcall.NewClientConf())
	defer c.Close()
	if err != nil {
		log.Printf("Its OK. failed to connect to the server: %s", err)
	}

	// Start server process
	cmd := exec.Command("./server.escript")
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	defer cmd.Process.Kill()

	// warming up
	time.Sleep(time.Second)

	// Communicate with server.
	communicate(c, "a")

	// Kill the server
	cmd.Process.Kill()

	// Communicate with server (expect error).
	log.Printf(" requesting to dead server...")
	resp, err := c.Req([]byte("b"), time.Second*5)
	if err == nil {
		log.Fatalf("expected error, but got response: %v", resp)
	}

	// Start server process again
	cmd = exec.Command("./server.escript")
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	defer cmd.Process.Kill()

	// warming up
	time.Sleep(time.Second)

	// Communicate with server.
	communicate(c, "c")
}

func communicate(c *tcpcall.Client, request string) {
	log.Printf(" sending `%s`...", request)
	resp, err := c.Req([]byte(request), time.Second*5)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	if string(resp) != request {
		log.Fatalf("bad response. Expected `%s` but `%s` found", request, string(resp))
	}
}
