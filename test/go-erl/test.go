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
	// communicate with server
	for i := 0; i < 5; i++ {
		log.Printf(" req> %d...\n", i)
		resp, err := c.Req([]byte(strconv.Itoa(i)), time.Second*2)
		if err != nil {
			log.Fatalf("failed to make request: %v", err)
		}
		r, t_err := strconv.Atoi(string(resp))
		if t_err != nil {
			log.Fatalf("failed to parse reply: %v", err)
		}
		if r != i*2 {
			log.Fatalf("bad reply. expected %v but %v found\n", i*2, r)
		}
		log.Printf("   OK\n")
	}
}
