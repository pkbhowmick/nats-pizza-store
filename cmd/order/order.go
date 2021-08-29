package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var Booked = make(map[string]int)
var locker = sync.RWMutex{}

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

	go func() {
		err := deliverPizza(js)
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
		pretty("Order " + string(msgs[0].Data) + " is received")
		natsMsg := &nats.Msg{
			Subject: "PROCESS.PIZZA",
			Data:    msgs[0].Data,
		}
		_, err = js.PublishMsg(natsMsg)
		if err != nil {
			return err
		}
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
		locker.Lock()
		if Booked["A"] == 1 {
			locker.Unlock()
			return
		}
		Booked["A"] = 1
		pretty("Oven A is processing order " + string(msg.Data))
		time.Sleep(10 * time.Second)
		msg.Ack()
		Booked["A"] = 0
		locker.Unlock()
		notifyDeliveryOrPanic(js, msg)
	}, nats.ManualAck())
	if err != nil {
		return err
	}

	_, err = js.QueueSubscribe("PROCESS.PIZZA", "GROUP", func(msg *nats.Msg) {
		locker.Lock()
		if Booked["B"] == 1 {
			locker.Unlock()
			return
		}
		Booked["B"] = 1
		pretty("Oven B is processing order " + string(msg.Data))
		time.Sleep(10 * time.Second)
		msg.Ack()
		Booked["B"] = 0
		locker.Unlock()
		notifyDeliveryOrPanic(js, msg)
	}, nats.ManualAck())
	if err != nil {
		return err
	}

	_, err = js.QueueSubscribe("PROCESS.PIZZA", "GROUP", func(msg *nats.Msg) {
		locker.Lock()
		if Booked["C"] == 1 {
			locker.Unlock()
			return
		}
		Booked["C"] = 1
		pretty("Oven C is processing order " + string(msg.Data))
		time.Sleep(10 * time.Second)
		msg.Ack()
		Booked["C"] = 0
		locker.Unlock()
		notifyDeliveryOrPanic(js, msg)
	}, nats.ManualAck())
	if err != nil {
		return err
	}
	return nil
}

func notifyDeliveryOrPanic(js nats.JetStreamContext, msg *nats.Msg) {
	natsMsg := &nats.Msg{
		Subject: "ORDERS.DELIVERY",
		Data:    msg.Data,
	}
	_, err := js.PublishMsg(natsMsg)
	if err != nil {
		panic(err)
	}
}

func deliverPizza(js nats.JetStreamContext) error {
	conInfo, err := js.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:       "DELIVERY",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "ORDERS.DELIVERY",
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
		pretty("Order " + string(msgs[0].Data) + " is prepared and ready to serve")
	}
}

func pretty(str interface{}) {
	t := time.Now()
	fmt.Printf("\n<NATS PIZZA STORE> %q Time:%v\n", str, t.Format("03:04:05 PM 2006-01-02"))
}
