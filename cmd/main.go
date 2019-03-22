package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/squeakysimple/gofilebasedkafkaproducer/config"
	producer "github.com/squeakysimple/gofilebasedkafkaproducer/fileproducer"
)

var (
	//messages     []string
	byteArray    []byte
	conf         []*config.AppConfig
	err          error
	producerMsgs []*sarama.ProducerMessage
	limit        int
)

func init() {
	conf, err = config.LoadConfig()
	if err != nil {
		panic(err)
	}

	limit, err = strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}
}

var waitStack, processStack, errorStack []string

func main() {
	now := time.Now()
	// syncProducer, err := producer.NewProducer()
	// if err != nil {
	// 	panic(err)
	// }
	// defer syncProducer.Close()

	//addToWaitStack() // inserts all tables into waitStack
	/*
		go func() {
			i := 0
			for i < len(conf) {
				if len(processStack) < 2 {
					if conf[i].Enabled {
						//tableToProcess := PopFromStack(waitStack)
						addToProcessStack(conf[i].Table)
					}
					i++
				}
			}
		}()

		go func() {
			processingTable := PopFromStack(processStack)
			var messages []string
			fmt.Println("executing", producer.Tables[processingTable])
			ReadCSV(conf[i].FileLoc, conf[i].Table, &messages)
			//		}

			producerMsgs = producer.PrepareMessages(conf[i].Topic, messages)
			err = syncProducer.SendMessages(producerMsgs)
			if err != nil {
				panic(err)
			}
		}()

		//	}
	*/
	for i := 0; i < len(conf); i++ {
		var messages []string
		// fmt.Println(conf[i].Enabled)
		if conf[i].Enabled {
			fmt.Println("executing", producer.Tables[conf[i].Table])
			ReadCSV(conf[i].FileLoc, conf[i].Table, conf[i].Topic, &messages)
		}

		// producerMsgs = producer.PrepareMessages(conf[i].Topic, messages)
		// err = syncProducer.SendMessages(producerMsgs)
		// if err != nil {
		// 	panic(err)
		// }
	}
	dur := time.Since(now)
	fmt.Println(dur.Seconds())
}

// ReadCSV reads a csv file and adds data to messages slice
func ReadCSV(csvFile string, table string, topic string, m *[]string) {
	file, err := os.Open(csvFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	syncProducer, err := producer.NewProducer()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := syncProducer.Close(); err != nil {
			// Should not reach here
			panic(err)
		}
	}()

	reader := csv.NewReader(bufio.NewReader(file))
	var i, k int

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if i == 0 {
			i++
			continue
		}
		switch producer.Tables[table] {
		case "Links":
			rec, err := strconv.Atoi(record[0])
			if err != nil {
				panic(err)
			}

			link := producer.Links{
				MovieID:     uint32(rec),
				ImdbID:      record[1],
				TmdbID:      record[2],
				CurrentTime: time.Now().UTC(),
			}

			byteArray, err = json.Marshal(link)
			if err != nil {
				panic(err)
			}
		case "Movies":
			rec, err := strconv.Atoi(record[0])
			if err != nil {
				fmt.Println(rec)
				panic(err)
			}

			movie := producer.Movies{
				MovieID:     uint32(rec),
				Title:       record[1],
				Genres:      record[2],
				CurrentTime: time.Now().UTC(),
			}

			byteArray, err = json.Marshal(movie)
			if err != nil {
				panic(err)
			}
		case "Ratings":
			userid, err := strconv.Atoi(record[0])
			if err != nil {
				panic(err)
			}

			movieid, err := strconv.Atoi(record[1])
			if err != nil {
				panic(err)
			}

			rt, err := strconv.ParseFloat(record[2], 32)
			if err != nil {
				panic(err)
			}

			t, err := strconv.ParseInt(record[3], 10, 64)
			if err != nil {
				panic(err)
			}
			tm := time.Unix(t, 0)

			rating := producer.Ratings{
				UserID:      uint32(userid),
				MovieID:     uint32(movieid),
				Rating:      float32(rt),
				RecordTime:  tm,
				CurrentTime: time.Now().UTC(),
			}

			byteArray, err = json.Marshal(rating)
			if err != nil {
				panic(err)
			}
		case "Tags":
			userid, err := strconv.Atoi(record[0])
			if err != nil {
				panic(err)
			}

			movieid, err := strconv.Atoi(record[1])
			if err != nil {
				panic(err)
			}

			t, err := strconv.ParseInt(record[3], 10, 64)
			if err != nil {
				panic(err)
			}
			tm := time.Unix(t, 0)

			tag := producer.Tags{
				UserID:      uint32(userid),
				MovieID:     uint32(movieid),
				Tag:         record[2],
				RecordTime:  tm,
				CurrentTime: time.Now().UTC(),
			}

			byteArray, err = json.Marshal(tag)
			if err != nil {
				panic(err)
			}
		default:
			panic(errors.New("No valid table name supplied"))
		}

		*m = append(*m, string(byteArray))
		k++
		if k == limit {
			err := prodc(syncProducer, topic, *m)
			if err != nil {
				panic(err)
			}
			var n []string
			//now := time.Now()
			m = &n
			//dur := time.Since(now)
			//fmt.Println(dur.Seconds())
			k = 0
		}
	}
	fmt.Println("topic name", topic)
	err = prodc(syncProducer, topic, *m)
	if err != nil {
		panic(err)
	}
	fmt.Println("debug1", producer.Tables[table])
}

func addToWaitStack() {
	for i := 0; i < len(conf); i++ {
		if conf[i].Enabled {
			waitStack = append(waitStack, conf[i].Table)
		}
	}
}

func addToProcessStack(table string) {
	processStack = append(processStack, table)
}

// PopFromStack implements Pop function for a stack
func PopFromStack(stack []string) string {
	val := stack[len(stack)-1]
	fmt.Println("popped value:", val)
	stack = stack[:len(stack)-1]

	return val
}

func prodc(syncProducer sarama.SyncProducer, topic string, msg []string) error {
	producerMsgs = producer.PrepareMessages(topic, msg)
	err = syncProducer.SendMessages(producerMsgs)
	//fmt.Println("debug2")
	return err
}
