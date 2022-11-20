package processor

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
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
		ts := uint64(time.Now().Unix())
		for i := 0; i < b.N; i++ {
			recivedMessages <- UserMsg{
				Uid: "user" + strconv.Itoa(rand.Intn(100)),
				Ts:  ts,
			}
			ts += uint64(rand.Intn(3600))
		}
		close(recivedMessages)
	}()

	b.StartTimer()

	go ProcessMessages(ctx, &wg, recivedMessages, messagesToSend)

	for range messagesToSend {
	}
	wg.Wait()
}
