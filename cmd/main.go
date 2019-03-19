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

	producer "github.com/squeakysimple/gofilebasedkafkaproducer/fileproducer"
)

var (
	messages  []string
	table     = os.Args[1]
	topic     string
	byteArray []byte
)

func main() {
	now := time.Now()
	syncProducer, err := producer.NewProducer()
	if err != nil {
		fmt.Println(err)
	}

	defer syncProducer.Close()
	//fmt.Println(producer.Tables[table])
	switch producer.Tables[table] {
	case "Links":
		topic = "movielens-links"
		csvFile := "./data/ml-latest-small/links.csv"
		ReadCSV(csvFile, &messages)
	case "Movies":
		topic = "movielens-movies"
		csvFile := "./data/ml-latest-small/movies.csv"
		ReadCSV(csvFile, &messages)
	default:
		panic(errors.New("No valid table name supplied"))
	}

	producerMsgs := producer.PrepareMessages(topic, messages)

	err = syncProducer.SendMessages(producerMsgs)
	if err != nil {
		fmt.Println(err)
	}

	dur := time.Since(now)
	fmt.Println(dur.Seconds())
	//fmt.Println(partition, offset)

}

// ReadCSV reads a csv file and adds data to messages slice
func ReadCSV(csvFile string, m *[]string) {
	file, err := os.Open(csvFile)
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(bufio.NewReader(file))
	var i int
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
				fmt.Println(rec)
				panic(err)
			}

			link := producer.Links{
				MovieID: uint32(rec),
				ImdbID:  record[1],
				TmdbID:  record[2],
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
				MovieID: uint32(rec),
				Title:   record[1],
				Genres:  record[2],
			}

			byteArray, err = json.Marshal(movie)
			if err != nil {
				panic(err)
			}
		default:
			panic(errors.New("No valid table name supplied"))
		}

		*m = append(*m, string(byteArray))
	}
}
