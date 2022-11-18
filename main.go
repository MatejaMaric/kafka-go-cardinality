package main

import (
	"context"
	"os/signal"
	"sync"
	"syscall"

	"github.com/MatejaMaric/kafka-go-cardinality/kafkaio"
	"github.com/MatejaMaric/kafka-go-cardinality/processor"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	r, w := kafkaio.Init()
	defer kafkaio.Close(r, w)

	recivedMessages := make(chan processor.UserMsg, 100)
	messagesToSend := make(chan processor.StatMsg, 100)

	var wg sync.WaitGroup
	wg.Add(3)
	go kafkaio.ReceiveMessages(ctx, &wg, r, recivedMessages)
	go processor.ProcessMessages(ctx, &wg, recivedMessages, messagesToSend)
	go kafkaio.SendMessages(ctx, &wg, w, messagesToSend)
	wg.Wait()
}
