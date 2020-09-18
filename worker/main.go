package main

import (
	"bufio"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var finished int64

type Task struct {
	account int
	message int
}

func startWorker(wg *sync.WaitGroup) chan *Task {
	ch := make(chan *Task, 10)
	wg.Add(1)

	go func() {
		defer wg.Done()

		for task := range ch {
			log.Printf("Start task: account %d, message %d\n", task.account, task.message)
			time.Sleep(time.Second) // simulate work
			log.Printf("End task: account %d, message %d\n", task.account, task.message)
			atomic.AddInt64(&finished, 1)
		}
	}()

	return ch
}

func atoi(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalln(err)
	}
	return val
}

func unmarshal(s string) *Task {
	data := strings.Split(s, " ")

	return &Task{
		account: atoi(data[0]),
		message: atoi(data[1]),
	}
}

func main() {
	log.Println("=============== Start ===============")

	conn, err := net.Dial("tcp", ":8081")
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		err := conn.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}()

	conn.SetReadDeadline(time.Time{})

	workersCh := make(map[int]chan *Task)
	reader := bufio.NewReader(conn)
	wg := &sync.WaitGroup{}

	for {
		s, err := reader.ReadString('\n')
		if err != nil {
			for _, ch := range workersCh {
				close(ch)
			}
			wg.Wait()

			log.Printf("%d tasks have been completed\n", finished)
			log.Println("=============== Done ===============")

			return
		}

		s = s[:len(s)-1]
		task := unmarshal(s)

		if _, ok := workersCh[task.account]; !ok {
			workersCh[task.account] = startWorker(wg)
		}

		workersCh[task.account] <- task
	}
}
