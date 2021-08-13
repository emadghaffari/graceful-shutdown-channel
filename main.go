package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

func main() {
	c := consumer{
		pool: make(chan int, 1),
		jobs: make(chan int, runtime.NumCPU()),
	}

	p := producer{
		callBack: c.callBack,
	}
	go p.start()

	// Set up cancellation context and waitgroup
	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	go c.listen(ctx)

	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go c.worker(ctx, wg, i)
	}

	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	<-termChan // Blocks here until interrupted

	// Handle shutdown
	fmt.Println("*********************************\nShutdown signal received\n*********************************")
	cancelFunc() // Signal cancellation to context.Context
	wg.Wait()    // Block here until are workers are done

	fmt.Println("All workers done, shutting down!")

}

type producer struct {
	callBack func(i int)
}

func (p producer) start() {
	for i := 0; ; i++ {
		p.callBack(i)
		fmt.Printf("producer: created new job: %d \n", i)
		time.Sleep(time.Millisecond * 100)
	}
}

type consumer struct {
	pool chan int
	jobs chan int
}

func (c consumer) listen(ctx context.Context) {
	for {
		select {
		case job := <-c.pool:
			c.jobs <- job
		case <-ctx.Done():
			close(c.jobs)
			fmt.Println("jobs closed by context")
			return
		}
	}
}

func (c consumer) worker(ctx context.Context, wg *sync.WaitGroup, index int) {
	defer wg.Done()

	for i := range c.jobs {
		fmt.Println("work on item: ", i)
		fmt.Printf("Worker %d started job %d\n", index, i)
		time.Sleep(time.Millisecond * time.Duration(1000+rand.Intn(2000)))
		fmt.Printf("Worker %d finished processing job %d\n", index, i)
	}
}

func (c consumer) callBack(i int) {
	c.pool <- i
}
