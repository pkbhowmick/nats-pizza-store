package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	Timeout = 5 * time.Second
)

func main() {
	err := takeOrder()
	if err != nil {
		panic(err)
	}
}

func takeOrder() error {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return err
	}
	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	strInfo, err := js.AddStream(&nats.StreamConfig{
		Name:        "ORDERS",
		Description: "Take order for pizza from customer",
		Subjects:    []string{"ORDERS.*"},
	})
	if err != nil {
		fmt.Println("error1")
		return err
	}

	conInfo, err := js.AddConsumer(strInfo.Config.Name, &nats.ConsumerConfig{
		Durable:       "MONITOR",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "ORDERS.PIZZA",
	})
	if err != nil {
		return err
	}

	sub, err := js.PullSubscribe(conInfo.Config.FilterSubject, conInfo.Name, nats.BindStream(conInfo.Stream))
	if err != nil {
		return err
	}
	defer func() {
		err := sub.Unsubscribe()
		if err != nil {
			panic(err)
		}
	}()

	for {
		msgs, err := sub.Fetch(1)
		if err == nats.ErrTimeout {
			continue
		}
		if err != nil {
			return err
		}
		if len(msgs) == 0 {
			continue
		}
		err = msgs[0].Ack()
		if err != nil {
			return err
		}
		fmt.Printf("Order " + string(msgs[0].Data) + " received\n")
	}
}
