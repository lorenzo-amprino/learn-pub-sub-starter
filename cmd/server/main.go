package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	connectionString := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Println("Error connecting to RabbitMQ:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating channel:", err)
		return
	}
	defer ch.Close()

	gamelogic.PrintServerHelp()

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, "game_logs", "game_logs.*", pubsub.Durable)
	if err != nil {
		fmt.Println("Error declaring and binding queue:", err)
		return
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			ps := routing.PlayingState{IsPaused: true}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, ps)
			if err != nil {
				fmt.Println("Error publishing message:", err)
			}
			fmt.Println("Published message to RabbitMQ")
		case "resume":
			fmt.Println("Resume command received.")
			ps := routing.PlayingState{IsPaused: false}

			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, ps)
			if err != nil {
				fmt.Println("Error publishing message:", err)
			}

			fmt.Println("Published message to RabbitMQ")
		case "quit":
			fmt.Println("Exiting server.")
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
	}

}
