package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
)

const maxSequentialTask = 10

func init() {
	rand.Seed(time.Now().Unix())
}

type Task struct {
	account int
	message int
}

type Generator struct {
	nAccount int
	tasks    map[int]*Task
}

func NewGenerator(nAccount int) *Generator {
	return &Generator{
		nAccount: nAccount,
		tasks:    make(map[int]*Task),
	}
}

// рэндомно генерируем от 1 до 10 последовательных задач для случайного аккаунта
func (g *Generator) Generate() []Task {
	nSequentialTask := 1 + rand.Intn(maxSequentialTask)
	tasks := make([]Task, nSequentialTask)

	account := rand.Intn(g.nAccount)
	if _, ok := g.tasks[account]; !ok {
		g.tasks[account] = &Task{
			account: account,
			message: 0,
		}
	}

	for i := 0; i < nSequentialTask; i++ {
		g.tasks[account].message++
		tasks[i] = *g.tasks[account]
	}

	return tasks
}

// генерируем очередь
// пишем задачи в сокет по адресу 127.0.0.1:8081
func run(nTask, nAccount int) {
	listener, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatalln(err)
	}

	conn, err := listener.Accept()
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		err := conn.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}()

	conn.SetWriteDeadline(time.Time{})

	g := NewGenerator(nAccount)
	var i int

	for i < nTask {
		tasks := g.Generate()
		i += len(tasks)
		if i > nTask {
			tasks = tasks[:len(tasks)-i+nTask]
		}

		for _, t := range tasks {
			log.Printf("account %d, message %d\n", t.account, t.message)
			fmt.Fprintf(conn, "%d %d\n", t.account, t.message)
		}
	}
}

func main() {
	var (
		nTask    *int
		nAccount *int
	)

	nTask = flag.Int("task-number", 10_000, "Количество задач")
	nAccount = flag.Int("account-number", 1_000, "Количество аккаунтов")

	flag.Parse()

	log.Println("============= Start =============")
	defer log.Println("============= Done =============")

	run(*nTask, *nAccount)
}
