package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Constants struct {
	Broker         string
	Port           int16
	FactoryNumbers uint8
	DeviceNumbers  uint8
	TopicNumbers   int16
}

func SendAsDevice(wg *sync.WaitGroup, DevNumber uint8, constants Constants, client mqtt.Client, channel chan int32) {
	defer wg.Done()
	var j int16 = 1
	for j < constants.TopicNumbers {
		var (
			message     string
			final_topic string
		)
		message = "/datapoint-" + fmt.Sprintf("%d", j)

		final_topic = "factory-" + fmt.Sprintf("%d", constants.FactoryNumbers) +
			"/device-" + fmt.Sprintf("%d", DevNumber) +
			message

		token := client.Publish(final_topic, 0, false, message)
		token.Wait()
		j++
	}
	channel <- int32(j)
}

func main() {
	mqtt.ERROR = log.New(os.Stdout, "[ERROR] ", 0)
	mqtt.CRITICAL = log.New(os.Stdout, "[CRIT] ", 0)

	channel_to_read := make(chan int32, 100) // Buffered channel to prevent blocking

	constants := Constants{
		Broker:         "localhost",
		Port:           1883,
		FactoryNumbers: 1,
		DeviceNumbers:  20,
		TopicNumbers:   1200,
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", constants.Broker, constants.Port))
	opts.SetClientID("go_mqtt_client")
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		fmt.Printf("Connection lost: %v\n", err)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("Connection failed: %v\n", token.Error())
		return
	}

	var wg sync.WaitGroup
	wg.Add(int(constants.DeviceNumbers))

	// Start a timer for 1 second
	stopTimer := time.After(1 * time.Second)

	// Launch goroutines to send messages as devices
	for i := uint8(1); i <= constants.DeviceNumbers; i++ {
		go SendAsDevice(&wg, i, constants, client, channel_to_read)
	}

	// Wait for all the goroutines in parallel or 1 second, whichever comes first
	select {
	case <-stopTimer:
		fmt.Println("1 second elapsed, stopping the process.")
		client.Disconnect(255)
	case <-time.After(1 * time.Second):
		wg.Wait()
		fmt.Println("All goroutines finished before timeout.")
	}

	close(channel_to_read)

	// Read from the channel
	runs := int32(0)
	for i := range channel_to_read {
		runs += i
	}

	fmt.Println("Total runs:", runs)
}
