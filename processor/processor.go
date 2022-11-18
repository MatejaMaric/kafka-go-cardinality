package processor

import (
	"context"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
)

// Message types and helper methodes

type UserMsg struct {
	Uid string `json:"uid"`
	Ts  uint64 `json:"ts"`
}

type StatMsg struct {
	Type      StatType
	Timestamp uint64
	Value     uint64
}

type StatType string

const (
	Minute StatType = "minute_count"
	Day             = "day_count"
	Week            = "week_count"
	Month           = "month_count"
	Year            = "year_count"
)

// Takes the timestamp and return the number of seconds to the next
// minute/day/week/month/year depending on the StatType
func (s StatType) intervalFrom(timestamp uint64) uint64 {
	switch s {
	case Minute:
		return 60
	case Day:
		return 3600 * 24
	case Week:
		return 3600 * 24 * 7
	case Month:
		moment := time.Unix(int64(timestamp), 0)
		return uint64(moment.AddDate(0, 1, 0).Sub(moment).Seconds())
	case Year:
		moment := time.Unix(int64(timestamp), 0)
		return uint64(moment.AddDate(1, 0, 0).Sub(moment).Seconds())
	default:
		return 0
	}
}

// Processing messages from input channel and routing them to the output channel
func ProcessMessages(ctx context.Context, wg *sync.WaitGroup, inputChannel <-chan UserMsg, outputChannel chan<- StatMsg) {
	defer wg.Done()
	defer close(outputChannel)

	processMinute := createStatProcessor(Minute)
	processDay := createStatProcessor(Day)
	processWeek := createStatProcessor(Week)
	processMonth := createStatProcessor(Month)
	processYear := createStatProcessor(Year)

	for msg := range inputChannel {
		var wg sync.WaitGroup
		wg.Add(5)
		go processMinute(&wg, msg, outputChannel)
		go processDay(&wg, msg, outputChannel)
		go processWeek(&wg, msg, outputChannel)
		go processMonth(&wg, msg, outputChannel)
		go processYear(&wg, msg, outputChannel)
		wg.Wait()
	}
}

// The entire point of the program
func createStatProcessor(statType StatType) func(*sync.WaitGroup, UserMsg, chan<- StatMsg) {
	hll := hyperloglog.New()
	var lastFlush uint64
	interval := statType.intervalFrom(lastFlush)

	return func(wg *sync.WaitGroup, msg UserMsg, outputChannel chan<- StatMsg) {
		defer wg.Done()

		hll.Insert([]byte(msg.Uid))

		if msg.Ts-interval > lastFlush {
			statMsg := StatMsg{
				Type:      statType,
				Timestamp: lastFlush,
				Value:     hll.Estimate(),
			}
			hll = hyperloglog.New()
			lastFlush = msg.Ts
			interval = statType.intervalFrom(lastFlush)

			outputChannel <- statMsg
		}
	}
}
