package main

import (
	"fmt"
	"github.com/waynewu411/go-producer-consumer-demo/config"
	"log"
	"time"
)

func main() {
	cfg := config.InitConfig()

	err := producerConsumer(cfg.ConsumerCount)
	log.Printf("ProducerConsumer finished, error: %v", err)
}

func producerConsumer(consumerCount int) error {
	errChans := make([]chan error, 0, 1+consumerCount) // Send errors from goroutines to main goroutine

	// Start the producer goroutine
	producerDataChan := make(chan int)
	producerErrChan := make(chan error)
	errChans = append(errChans, producerErrChan)
	stopProducerChan := make(chan struct{}) // Stop signal for the producer
	go producer(producerDataChan, producerErrChan, stopProducerChan)

	// Start the consumer goroutines
	consumerDataChans := make([]chan int, consumerCount)
	stopConsumers := make([]chan struct{}, consumerCount) // Slice of stop signals for the consumers
	for i := 0; i < consumerCount; i++ {
		consumerDataChans[i] = make(chan int)
		consumerErrChan := make(chan error)
		errChans = append(errChans, consumerErrChan)
		stopConsumers[i] = make(chan struct{})
		go consumer(i, consumerDataChans[i], consumerErrChan, stopConsumers[i])
	}

	// start a goroutine for distributing data from producer to consumers
	go FanOutWithChannel(producerDataChan, consumerDataChans...)

	mergedErrChan := FanIn(errChans...)

	var mergedError error = nil
	for i := 0; i < 1+consumerCount; i++ {
		err := <-mergedErrChan
		log.Printf("[main]: Received error: %v", err)
		if err != nil {
			mergedError = err
			close(stopProducerChan)
			for i := 0; i < consumerCount; i++ {
				close(stopConsumers[i])
			}
		}
	}
	return mergedError
}

func producer(dataChan chan<- int, errChan chan<- error, stopSignal <-chan struct{}) {
	var err error = nil

	defer func() {
		r := recover()
		if r != nil {
			errChan <- fmt.Errorf("%v", r)
		} else {
			errChan <- err
		}
	}()

	// Simulate reading data from an external source
	for i := 1; i <= 100; i++ {
		// Check for the stop signal
		select {
		case <-stopSignal:
			log.Println("[producer] received stop signal. Stopping.")
			return
		default:
		}

		if config.GetConfig().ProducerFailNumber >= 0 && i == config.GetConfig().ProducerFailNumber {
			err = fmt.Errorf("[producer] fail on %v", i)
			return
		}

		if config.GetConfig().ProducerPanicNumber >= 0 && i == config.GetConfig().ProducerPanicNumber {
			panic(fmt.Sprintf("[producer] panic on %d", i))
		}

		// Send data to the consumers
		dataChan <- i
		log.Printf("[producer] sent %d", i)
		time.Sleep(time.Duration(config.GetConfig().ProducerSendInternal) * time.Millisecond)
	}

	// Close the data channel when done
	close(dataChan)
}

func consumer(id int, dataChan <-chan int, errChan chan<- error, stopSignal <-chan struct{}) {
	var err error = nil

	defer func() {
		r := recover()
		if r != nil {
			errChan <- fmt.Errorf("%v", r)
		} else {
			errChan <- err
		}
	}()

	for {
		select {
		case data, ok := <-dataChan:
			if !ok {
				// Data channel has been closed
				log.Printf("[consumer %d] data channel closed. Stopping.", id)
				return
			}

			// Process the data
			if config.GetConfig().ConsumerFailNumber >= 0 && data == config.GetConfig().ConsumerFailNumber {
				// Suppose an error occurs in one of the consumers
				err = fmt.Errorf("[consumer %d] fail on %v", id, data)
				return
			}

			if config.GetConfig().ConsumerPanicNumber >= 0 && data == config.GetConfig().ConsumerPanicNumber {
				panic(fmt.Sprintf("[consumer %d] panic on %d", id, data))
			}

			log.Printf("[consumer %d] processed %d", id, data)
			time.Sleep(time.Duration(config.GetConfig().ConsumerReadInternal) * time.Millisecond)

		case <-stopSignal:
			// Received stop signal from the main goroutine
			log.Printf("[consumer %d] received stop signal. Stopping.", id)
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
