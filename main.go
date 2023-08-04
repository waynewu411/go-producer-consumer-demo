package main

import (
	"fmt"
	"github.com/waynewu411/go-producer-consumer-demo/config"
	"log"
	"sync"
	"time"
)

func main() {
	_ = config.InitConfig()

	// Channels for communication between producer and consumers
	errorChan := make(chan error)   // Send errors from goroutines to main goroutine
	doneChan := make(chan struct{}) // Signal when all goroutines are done

	// WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Start the producer goroutine
	producerDataChan := make(chan int)
	stopProducer := make(chan struct{}) // Stop signal for the producer
	wg.Add(1)
	go producer(producerDataChan, errorChan, stopProducer, doneChan, &wg)

	// Start the consumer goroutines
	consumerDataChans := make([]chan int, config.GetConfig().ConsumerCount)
	stopWorkers := make([]chan struct{}, config.GetConfig().ConsumerCount) // Slice of stop signals for the consumers
	for i := 0; i < config.GetConfig().ConsumerCount; i++ {
		consumerDataChans[i] = make(chan int)
		stopWorkers[i] = make(chan struct{})
		wg.Add(1)
		go consumer(i, consumerDataChans[i], errorChan, stopWorkers[i], doneChan, &wg)
	}

	// start a goroutine for distributing data from producer to consumers
	go FanOutWithChannel(producerDataChan, consumerDataChans...)

	// Monitor for errors from goroutines and done signal
	select {
	case err := <-errorChan:
		log.Printf("Main goroutine: Received error: %v\n", err)

		// Send the stop signal to all goroutines
		close(stopProducer)
		for i := 0; i < config.GetConfig().ConsumerCount; i++ {
			close(stopWorkers[i])
		}

		// Wait for all goroutines to finish
		wg.Wait()
		log.Println("All goroutines stopped.")

	case <-doneChan:
		log.Println("All goroutines completed without errors.")
	}
}

func producer(dataChan chan<- int, errorChan chan<- error, stopSignal <-chan struct{}, doneChan chan<- struct{}, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			errorChan <- fmt.Errorf("%v", r)
		}
	}()

	defer wg.Done()

	// Simulate reading data from an external source
	for i := 1; i <= 100; i++ {
		// Check for the stop signal
		select {
		case <-stopSignal:
			log.Println("producer: Received stop signal. Stopping.")
			return
		default:
		}

		if config.GetConfig().ProducerFailNumber >= 0 && i == config.GetConfig().ProducerFailNumber {
			errorChan <- fmt.Errorf("producer: Simulated error while reading data %v", i)
			return
		}

		if config.GetConfig().ProducerPanicNumber >= 0 && i == config.GetConfig().ProducerPanicNumber {
			panic(fmt.Sprintf("producer panic on %d", i))
		}

		// Send data to the consumers
		dataChan <- i
		log.Printf("producer sent %d", i)
		time.Sleep(time.Duration(config.GetConfig().ProducerSendInternal) * time.Millisecond)
	}

	// Close the data channel when done
	close(dataChan)

	// Signal that the producer is done
	doneChan <- struct{}{}
}

func consumer(id int, dataChan <-chan int, errorChan chan<- error, stopSignal <-chan struct{}, doneChan chan<- struct{}, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			errorChan <- fmt.Errorf("%v", r)
		}
	}()

	defer wg.Done()

	for {
		select {
		case data, ok := <-dataChan:
			if !ok {
				// Data channel has been closed
				log.Printf("[consumer %d] Data channel closed. Stopping.\n", id)
				return
			}

			// Process the data
			if config.GetConfig().ConsumerFailNumber >= 0 && data == config.GetConfig().ConsumerFailNumber {
				// Suppose an error occurs in one of the consumers
				errorChan <- fmt.Errorf("[consumer %d] fail to process data %v", id, data)
				return
			}

			if config.GetConfig().ConsumerPanicNumber >= 0 && data == config.GetConfig().ConsumerPanicNumber {
				panic(fmt.Sprintf("[consumer %d] panic on processing data %d", id, data))
			}

			log.Printf("[consumer %d] processed data %d\n", id, data)
			time.Sleep(time.Duration(config.GetConfig().ConsumerReadInternal) * time.Millisecond)

		case <-stopSignal:
			// Received stop signal from the main goroutine
			log.Printf("[consumer %d] Received stop signal. Stopping.\n", id)
			return
		}
	}
}

func FanIn[T any](channels ...chan T) chan T {
	merged := make(chan T)
	done := make(chan struct{})

	for _, channel := range channels {
		go func(c <-chan T) {
			defer func() {
				done <- struct{}{}
			}()
			for data := range c {
				merged <- data
			}
		}(channel)
	}

	go func() {
		for i := 0; i < len(channels); i++ {
			<-done
		}
		close(merged)
	}()

	return merged
}

func FanOutWithData[T any](data T, channels ...chan T) {
	for _, channel := range channels {
		go func(channel chan T) {
			channel <- data
		}(channel)
	}
}

func FanOutWithChannel[T any](inDataChan chan T, outDataChans ...chan T) {
	i := 0
	for {
		select {
		case data, ok := <-inDataChan:
			if !ok {
				// inDataChan has been closed which means no more data from producer
				// close all the outDataChans
				for _, ch := range outDataChans {
					close(ch)
				}
				return
			}
			go func() {
				outDataChans[i] <- data
			}()
			i = (i + 1) % len(outDataChans)
		}
	}
}
