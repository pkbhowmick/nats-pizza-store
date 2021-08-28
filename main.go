package main

import (
	"log"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func main() {
	natsServer, err := server.NewServer(&server.Options{
		JetStream: true,
	})
	if err != nil {
		log.Fatalln(err)
	}
	go natsServer.Start()
	defer natsServer.Shutdown()

	if !natsServer.ReadyForConnections(10 * time.Second) {
		log.Fatalln("can't start server")
	}
	log.Println("NATS Server is running!")
	select {}
}
