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
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	go func() {
		err := takeOrder(js)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		err := processPizza(js)
		if err != nil {
			panic(err)
		}
	}()

	select {}
}

func takeOrder(js nats.JetStreamContext) error {
	strInfo, err := js.AddStream(&nats.StreamConfig{
		Name:        "ORDERS",
		Description: "Take order for pizza from customer",
		Subjects:    []string{"ORDERS.*"},
	})
	if err != nil {
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
		natsMsg := &nats.Msg{
			Subject: "PROCESS.PIZZA",
			Data:    msgs[0].Data,
		}
		js.PublishMsg(natsMsg)
	}
}

func processPizza(js nats.JetStreamContext) error {
	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "PROCESS",
		Description: "Take order for pizza from customer",
		Subjects:    []string{"PROCESS.PIZZA"},
	})
	if err != nil {
		return err
	}
	_, err = js.QueueSubscribe("PROCESS.PIZZA", "GROUP", func(msg *nats.Msg) {
		fmt.Println("Oven 1 is preparing pizza " + string(msg.Data))
		msg.Ack()
	}, nats.ManualAck())
	if err != nil {
		return err
	}

	_, err = js.QueueSubscribe("PROCESS.PIZZA", "GROUP", func(msg *nats.Msg) {
		fmt.Println("Oven 2 is preparing pizza " + string(msg.Data))
		msg.Ack()
	}, nats.ManualAck())
	if err != nil {
		return err
	}

	_, err = js.QueueSubscribe("PROCESS.PIZZA", "GROUP", func(msg *nats.Msg) {
		fmt.Println("Oven 3 is preparing pizza " + string(msg.Data))
		msg.Ack()
	}, nats.ManualAck())
	if err != nil {
		return err
	}
	return nil
}
