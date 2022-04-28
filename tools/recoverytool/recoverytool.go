package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

var start = flag.Int64("start", 0, "start time of full backup")
var end = flag.Int64("end", 0, "end time of full backup")
var target = flag.Int64("target", 0, "target time of recovery")
var walsmeta = flag.String("walsmeta", "", "path of walsmeta")
var cmd = flag.String("cmd", "", "comand eg: calwals")

func calwals(start int64, end int64, target int64, walsmeta string) {
	if start >= end {
		fmt.Println("[Error] end time of full backup should larger than start time of full backup")
		return
	}

	if target <= end {
		fmt.Println("[Error] time of recovery should larger than end time of full backup")
		return
	}

	fi, err := os.Open(walsmeta)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()

	br := bufio.NewReader(fi)
	firtline, _, err := br.ReadLine()
	if err != nil {
		fmt.Println("[Error] read walsmeta failed: ", err.Error())
		return
	}

	n, err := strconv.Atoi(string(firtline))
	if err != nil {
		fmt.Println("[Error] not valid record:", string(firtline))
		return
	}
	if n <= 0 {
		fmt.Println("[Error] not valid len of records:", n)
		return
	}

	var wals []string
	for i := 0; i < n; i++ {
		content, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}

		cuts := strings.Split(string(content), ",")
		if len(cuts) != 3 {
			fmt.Println("[Error] not valid record:", string(content))
			return
		}
		wal := cuts[0]
		s, err := strconv.Atoi(cuts[1])
		if err != nil {
			fmt.Println("[Error] not valid record:", string(content))
			return
		}
		e, err := strconv.Atoi(cuts[2])
		if err != nil {
			fmt.Println("[Error] not valid record:", string(content))
			return
		}

		if s == 0 || e == 0 {
			fmt.Println("[Waring] abnormal record:", string(content))
		}

		if (s <= int(start) && e >= int(start)) || (s <= int(target) && e >= int(target)) || (s > int(start) && e < int(target)) {
			wals = append(wals, wal)
		}
	}

	sz := len(wals)
	fmt.Println("Backup agent should recover the following wals:")
	if sz == 1 {
		fmt.Print("\"", wals[0], "\"")
	} else {
		for i := 0; i < sz-1; i++ {
			fmt.Print("\"", wals[i], "\",")
		}
		fmt.Print("\"", wals[sz-1], "\"")
	}
}

func main() {
	flag.Parse()

	isvalid := true
	if *start == 0 {
		fmt.Println("[Error] param of start should set and larger than 0")
		isvalid = false
	}
	if *end == 0 {
		fmt.Println("[Error] param of end should set and larger than 0")
		isvalid = false
	}
	if *target == 0 {
		fmt.Println("[Error] param of target should set and larger than 0")
		isvalid = false
	}
	if *walsmeta == "" {
		fmt.Println("[Error] param of walsmeta is null")
		isvalid = false
	}
	if *cmd == "" {
		fmt.Println("[Error] param of cmd is null")
		isvalid = false
	}

	if !isvalid {
		return
	}

	if *cmd == "calwals" {
		calwals(*start, *end, *target, *walsmeta)
	} else {
		fmt.Println("[Error] not support cmd:", *cmd)
	}

}
