package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/umahmood/haversine"
)

type Record struct {
	Zip string
	Cor haversine.Coord
}

type Distance struct {
	src   string
	dst   string
	miles float64
}

const precision int = 3

func main() {
	if len(os.Args) != 3 {
		fmt.Println("distance <zipcodes.csv> <distances.csv>")
		return
	}

	input := os.Args[1]
	output := os.Args[2]

	// read the coordinates of each zipcode
	records, err := csv2records(input)
	if err != nil {
		log.Fatal(err)
		return
	}

	// channel to receive the distance calculations
	rescha := make(chan Distance, 100)
	// channel to send workloads
	srccha := make(chan Record, len(records))
	// worker done
	done := make(chan bool)

	// distance collector
	var wg sync.WaitGroup
	wg.Add(1)
	go collectDistances(rescha, &wg, output)

	// workers
	numWorkers := runtime.NumCPU() * 4
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go calcDistances(srccha, rescha, done, records, &wg)
	}

	// fill srccha
	for _, src := range records {
		srccha <- src
	}
	close(srccha)

	// close done when workers are finished
	for _ = range done {
		numWorkers--
		if numWorkers == 0 {
			break
		}
	}
	close(done)

	// close results
	close(rescha)

	wg.Wait()
}

func collectDistances(reschan chan Distance, wg *sync.WaitGroup, output string) {
	defer wg.Done()

	// create file for output
	file, err := os.Create(output)
	defer file.Close()
	if err != nil {
		log.Fatal(err)
		return
	}

	// create csv writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// buffer for writting csv in batches
	buffer := make([][]string, 3)
	count := 0
	for dis := range reschan {
		if dis.src != dis.dst {
			strMiles := strconv.FormatFloat(dis.miles, 'f', precision, 32)
			buffer = append(buffer, []string{dis.src, dis.dst, strMiles})
			count++
		}
		if count%2000 == 0 {
			// write the buffer and reset it
			err := writer.WriteAll(buffer)
			if err != nil {
				log.Fatal(err)
				return
			}
			buffer = make([][]string, 3)
			count = 0
		}
	}
	if count > 0 {
		err := writer.WriteAll(buffer)
		if err != nil {
			log.Fatal(err)
			return
		}
	}
}

// calculate the distances
func calcDistances(srccha chan Record, rescha chan Distance, done chan bool, records []Record, wg *sync.WaitGroup) {
	defer wg.Done()
	for src := range srccha {
		for _, dst := range records {
			miles, _ := haversine.Distance(src.Cor, dst.Cor)
			rescha <- Distance{src.Zip, dst.Zip, miles}
		}
	}
	done <- true
}

// return a slice with all zipcode records
func csv2records(path string) ([]Record, error) {
	in, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	r := csv.NewReader(bytes.NewReader(in))
	r.Comma = ','
	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	result := make([]Record, len(records)-1)
	for i, record := range records[1:] {
		lat, _ := strconv.ParseFloat(record[1], 64)
		lon, _ := strconv.ParseFloat(record[2], 64)
		src := Record{
			record[0],
			haversine.Coord{
				Lat: lat,
				Lon: lon,
			}}

		result[i] = src
	}
	return result, nil
}
