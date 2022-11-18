package processor

import (
	"context"
	"sync"
	"testing"
)

func TestIntervalFrom(t *testing.T) {

}

func TestCreateStatProcessor(t *testing.T) {

}

func BenchmarkProcessMessages(b *testing.B) {
	b.StopTimer()

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(1)

	recivedMessages := make(chan UserMsg, 100)
	messagesToSend := make(chan StatMsg, 100)

	go func() {
		for i := 0; i < b.N; i++ {
			recivedMessages <- UserMsg{"abc", 123}
		}
		close(recivedMessages)
	}()

	b.StartTimer()

	go ProcessMessages(ctx, &wg, recivedMessages, messagesToSend)

	for range messagesToSend {
	}
	wg.Wait()
}
