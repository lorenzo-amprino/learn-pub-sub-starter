package main

import (
	"fmt"
	"os"
	"os/signal"

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

	name, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error occurred:", err)
		return
	}

	state := gamelogic.NewGameState(name)
	pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+name, routing.PauseKey, pubsub.Transient, handlerPause(state))

	pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+name, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(state))

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating channel:", err)
		return
	}
	defer ch.Close()
	for {
		input := gamelogic.GetInput()
		switch input[0] {
		case "spawn":
			state.CommandSpawn(input)
		case "move":
			mv, err := state.CommandMove(input)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+name, mv)
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("pamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
	}
	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("RabbitMQ connection closed.")
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Println("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Println("> ")
		gs.HandleMove(move)
	}
}
