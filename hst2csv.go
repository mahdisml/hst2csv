package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

type HeaderBytes struct {
	Version   int32
	Copyright string
	Symbol    string
	Period    int32
	Digits    int32
	TimeSign  int32
	LastSync  int32
	Unused    int32
}

type HistoricalBytes struct {
	Time   string
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume int32
}

func (h HeaderBytes) String() string {
	return fmt.Sprintf(`"%d","%s","%s","%d","%d","%d","%d","%d"`,
		h.Version, h.Copyright, h.Symbol, h.Period,
		h.Digits, h.TimeSign, h.LastSync, h.Unused,
	)
}

func (h HistoricalBytes) String() string {
	return fmt.Sprintf(`"%s","%f","%f","%f","%f","%d"`,
		h.Time, h.Open, h.High, h.Low, h.Close, h.Volume,
	)
}

func readInt32(file *os.File, byteNum int32) (ret int32) {
	b := make([]byte, byteNum)
	buf := bytes.NewBuffer(b)
	_, err := file.Read(b)
	if err == io.EOF {
		file.Close()
		os.Exit(0)
	}
	binary.Read(buf, binary.LittleEndian, &ret)
	return
}

func readInt64(file *os.File, byteNum int32) (ret int64) {
	b := make([]byte, byteNum)
	buf := bytes.NewBuffer(b)
	_, err := file.Read(b)
	if err == io.EOF {
		file.Close()
		os.Exit(0)
	}
	binary.Read(buf, binary.LittleEndian, &ret)
	return
}

func readString(file *os.File, byteNum int32) (ret string) {
	b := make([]byte, byteNum)
	_, err := file.Read(b)
	if err == io.EOF {
		file.Close()
		os.Exit(0)
	}
	ret = string(b[:byteNum])
	return
}

func readFloat64(file *os.File, byteNum int64) (ret float64) {
	b := make([]byte, byteNum)
	_, err := file.Read(b)
	if err == io.EOF {
		file.Close()
		os.Exit(0)
	}
	bits := binary.LittleEndian.Uint64(b[:byteNum])
	ret = math.Float64frombits(bits)
	return
}

func ParseHeader(file *os.File) (hdr HeaderBytes) {

	hdr.Version = readInt32(file, 4)
	hdr.Copyright = readString(file, 64)
	hdr.Symbol = readString(file, 12)
	hdr.Period = readInt32(file, 4)
	hdr.Digits = readInt32(file, 4)
	hdr.TimeSign = readInt32(file, 4)
	hdr.LastSync = readInt32(file, 4)
	hdr.Unused = readInt32(file, 52)

	return
}

func ParseHistory(file *os.File) (hst HistoricalBytes) {

	hst.Time = strings.Replace(
		time.Unix(readInt64(file, 8), 0).Format(time.RFC3339),
		"+", "Z", 1)
	hst.Open = readFloat64(file, 8)
	hst.High = readFloat64(file, 8)
	hst.Low = readFloat64(file, 8)
	hst.Close = readFloat64(file, 8)
	hst.Volume = readInt32(file, 4)
	readInt32(file, 16)

	return
}

func ParseHistoryOld(file *os.File) (hst HistoricalBytes) {

	hst.Time = strings.Replace(
		time.Unix(int64(readInt32(file, 4)), 0).Format(time.RFC3339),
		"+", "Z", 1)
	hst.Open = readFloat64(file, 8)
	hst.Low = readFloat64(file, 8)
	hst.High = readFloat64(file, 8)
	hst.Close = readFloat64(file, 8)
	hst.Volume = readInt32(file, 8)

	return
}

func createCsvName(args_file_name string) String() string {
	var file_name String()
	var csv_file String()
	const csv_dir := "/src/csv/"
	file_name := args_file_name
	csv_file := strings.Replace(file_name, ".hst", ".csv", 1)

	return fmt.Sprintf(`"%s + %s"`, 
		   csv_dir, csv_file,
  	)
}

func main() {
	var header HeaderBytes
	var history HistoricalBytes
	in_file, in_err := os.Open(os.Args[1])
	out_file, out_err := os.Create(createCsvName(os.Args[1]))
	if in_err != nil {
		fmt.Println(in_err)
		return
	}
	if out_err != nil {
		fmt.Println(out_err)
		return
	}

	//defer in_file.Close()

	header = ParseHeader(in_file)
		if in_err != nil {
		fmt.Println(in_err)
		return
	}
	if header.Version < 401 {
		for {
			history = ParseHistoryOld(in_file)
			//fmt.Println(history)
			out_file.WriteString(history)
			if out_err != nil {
				fmt.Println(out_err)
				return
			}
		}
	} else {
		for {
			history = ParseHistory(in_file)
			// fmt.Println(history)
			out_file.WriteString(history)
			if err != nil {
				fmt.Println(out_err)
				return
			}
		}
	}

	defer in_file.Close()
	defer out_file.Close()
}
