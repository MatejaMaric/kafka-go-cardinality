package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/axiomhq/hyperloglog"
	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
)

type UserMsg struct {
	Uid string `json:"uid"`
	Ts  uint64 `json:"ts"`
}

type StatType string

const (
	Minute StatType = "minute_count"
	Day             = "day_count"
	Week            = "week_count"
	Month           = "month_count"
	Year            = "year_count"
)

func (s StatType) IntervalFrom(timestamp uint64) uint64 {
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

type StatMsg struct {
	Type      StatType
	Timestamp uint64
	Value     uint64
}

func ReciveMessages(ctx context.Context, wg *sync.WaitGroup, r *kafka.Reader, outputChannel chan<- UserMsg) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Println("Done with reciving messages...")
			close(outputChannel)
			break

		default:
			payload, err := r.ReadMessage(ctx)
			if err != nil {
				log.Println("Unable to read message: ", err.Error())
				continue
			}

			var msg UserMsg
			err = jsoniter.Unmarshal(payload.Value, &msg)
			if err != nil {
				log.Println("Unable to parse message: ", err.Error())
				continue
			}

			outputChannel <- msg
		}
	}
}

func ProcessMessages(ctx context.Context, wg *sync.WaitGroup, inputChannel <-chan UserMsg, outputChannel chan<- StatMsg) {
	defer wg.Done()
	defer close(outputChannel)

	for msg := range inputChannel {

	}
}

func SendMessages(ctx context.Context, wg *sync.WaitGroup, w *kafka.Writer, inputChannel <-chan StatMsg) {
	defer wg.Done()

	for statMsg := range inputChannel {
		json, err := jsoniter.MarshalIndent(statMsg, "", "  ")
		if err != nil {
			log.Println("Unable to convert to JSON: ", err.Error())
			continue
		}

		err = w.WriteMessages(ctx, kafka.Message{
			Value: json,
		})
		if err != nil {
			log.Println("Unable to send a message: ", err.Error())
		}
	}
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	r, w := initKafka()
	defer closeKafka(r, w)

	processMinute := createStatProcessor(ctx, w, Minute)
	processDay := createStatProcessor(ctx, w, Day)
	processWeek := createStatProcessor(ctx, w, Week)
	processMonth := createStatProcessor(ctx, w, Month)
	processYear := createStatProcessor(ctx, w, Year)

	for processing := true; processing; {
		select {
		case <-ctx.Done():
			log.Println("Done with message processing...")
			processing = false
		default:
			payload, err := r.ReadMessage(ctx)
			if err != nil {
				log.Println("Unable to read message: ", err.Error())
				break
			}

			var msg UserMsg
			err = jsoniter.Unmarshal(payload.Value, &msg)
			if err != nil {
				log.Println("Unable to parse message: ", err.Error())
				break
			}

			var wg sync.WaitGroup
			wg.Add(5)
			go processMinute(&wg, msg)
			go processDay(&wg, msg)
			go processWeek(&wg, msg)
			go processMonth(&wg, msg)
			go processYear(&wg, msg)
			wg.Wait()
		}
	}
}

func createStatProcessor(ctx context.Context, w *kafka.Writer, statType StatType) func(wg *sync.WaitGroup, msg UserMsg) {
	hll := hyperloglog.New()
	var lastFlush uint64
	interval := statType.IntervalFrom(lastFlush)

	return func(wg *sync.WaitGroup, msg UserMsg) {
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
			interval = statType.IntervalFrom(lastFlush)

			json, err := jsoniter.MarshalIndent(statMsg, "", "  ")
			if err != nil {
				log.Println("Unable to convert to JSON: ", err.Error())
				return
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
