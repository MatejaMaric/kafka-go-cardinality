package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/segmentio/kafka-go"
)

type UserMsg struct {
	Uid string `json:"uid"`
	Ts  int64  `json:"ts"`
}

type Stats struct {
	PerMinute uint64 `json:"users_last_minute"`
	PerDay    uint64 `json:"users_last_day"`
	PerWeek   uint64 `json:"users_last_week"`
	PerMonth  uint64 `json:"users_last_month"`
	PerYear   uint64 `json:"users_last_year"`
}

type StatsHLL struct {
	PerMinute *hyperloglog.Sketch
	PerDay    *hyperloglog.Sketch
	PerWeek   *hyperloglog.Sketch
	PerMonth  *hyperloglog.Sketch
	PerYear   *hyperloglog.Sketch
}

func NewStatsHLL() *StatsHLL {
	return &StatsHLL{
		hyperloglog.New(),
		hyperloglog.New(),
		hyperloglog.New(),
		hyperloglog.New(),
		hyperloglog.New(),
	}
}

func (hll *StatsHLL) ToStats() *Stats {
	return &Stats{
		hll.PerMinute.Estimate(),
		hll.PerDay.Estimate(),
		hll.PerWeek.Estimate(),
		hll.PerMonth.Estimate(),
		hll.PerYear.Estimate(),
	}
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	r, w := initKafka()
	defer closeKafka(r, w)

	minutes := make(map[int64]*hyperloglog.Sketch)

	var wg sync.WaitGroup
	wg.Add(2)
	go processMessages(ctx, &wg, minutes, r)
	go emitStats(ctx, &wg, minutes, w)
	wg.Wait()
}

func initKafka() (*kafka.Reader, *kafka.Writer) {
	kBroker, set := os.LookupEnv("KAFKA_BROKER")
	if !set {
		kBroker = "localhost:9092"
	}

	inTopic, set := os.LookupEnv("USERS_TOPIC")
	if !set {
		inTopic = "users"
	}

	outTopic, set := os.LookupEnv("STATS_TOPIC")
	if !set {
		outTopic = "stats"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kBroker},
		Topic:   inTopic,
	})
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kBroker},
		Topic:   outTopic,
	})

	return r, w
}

func closeKafka(r *kafka.Reader, w *kafka.Writer) {
	log.Println("Closing Kafka Reader...")
	err := r.Close()
	if err != nil {
		log.Println("Failed closing Kafka Reader: ", err.Error())
	} else {
		log.Println("Closed Kafka Reader...")
	}

	log.Println("Closing Kafka Writer...")
	err = w.Close()
	if err != nil {
		log.Println("Failed closing Kafka Writer: ", err.Error())
	} else {
		log.Println("Closed Kafka Writer...")
	}
}

func processMessages(ctx context.Context, wg *sync.WaitGroup, minutes map[int64]*hyperloglog.Sketch, r *kafka.Reader) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Println("Done with message processing...")
			return
		default:
			payload, err := r.ReadMessage(ctx)
			if err != nil {
				log.Println("Unable to read message: ", err.Error())
				break
			}

			var msg UserMsg
			err = json.Unmarshal(payload.Value, &msg)
			if err != nil {
				log.Println("Unable to parse message: ", err.Error())
				break
			}

			minute := msg.Ts - (msg.Ts % 60)
			hll, exists := minutes[minute]
			if !exists {
				hll = hyperloglog.New()
			}
			hll.Insert([]byte(msg.Uid))
			minutes[minute] = hll
		}
	}
}

func emitStats(ctx context.Context, wg *sync.WaitGroup, minutes map[int64]*hyperloglog.Sketch, w *kafka.Writer) {
	defer wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Println("Done with stats emitting...")
			return
		case <-ticker.C:
			statsHLL := NewStatsHLL()

			now := time.Now()
			minuteAgo := now.Add(-time.Minute)
			dayAgo := now.AddDate(0, 0, -1)
			weekAgo := now.AddDate(0, 0, -7)
			monthAgo := now.AddDate(0, -1, 0)
			yearAgo := now.AddDate(-1, 0, 0)

			for minute, ll := range minutes {
				if minuteAgo.Unix() < minute && minute <= now.Unix() {
					statsHLL.PerMinute.Merge(ll)
				}
				if dayAgo.Unix() < minute && minute <= now.Unix() {
					statsHLL.PerDay.Merge(ll)
				}
				if weekAgo.Unix() < minute && minute <= now.Unix() {
					statsHLL.PerWeek.Merge(ll)
				}
				if monthAgo.Unix() < minute && minute <= now.Unix() {
					statsHLL.PerMonth.Merge(ll)
				}
				if yearAgo.Unix() < minute && minute <= now.Unix() {
					statsHLL.PerYear.Merge(ll)
				}
			}

			json, err := json.MarshalIndent(statsHLL.ToStats(), "", "  ")
			if err != nil {
				log.Println("Unable to convert to JSON: ", err.Error())
				break
			}

			err = w.WriteMessages(ctx, kafka.Message{
				Value: json,
			})
			if err != nil {
				log.Println("Unable to send a message: ", err.Error())
			}
		}
	}
}
