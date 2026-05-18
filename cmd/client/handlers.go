package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.GetUsername(), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Printf("error publishing war recognition: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerAllWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rec gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		out, winner, loser := gs.HandleWar(rec)

		if out == gamelogic.WarOutcomeNotInvolved {
			fmt.Printf("War detected between %s and %s. You are not involved.\n", rec.Attacker.Username, rec.Defender.Username)
			return pubsub.NackRequeue
		}
		if out == gamelogic.WarOutcomeNoUnits {
			fmt.Printf("War detected between %s and %s. You have no units in the war.\n", rec.Attacker.Username, rec.Defender.Username)
			return pubsub.NackDiscard
		}
		if out == gamelogic.WarOutcomeOpponentWon {
			message := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(ch, routing.GameLog{
				Username: rec.Defender.Username,
				Message:  message,
			})
			if err != nil {
				fmt.Printf("error publishing game log: %s\n", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		}
		if out == gamelogic.WarOutcomeYouWon {
			message := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(ch, routing.GameLog{
				Username: rec.Defender.Username,
				Message:  message,
			})
			if err != nil {
				fmt.Printf("error publishing game log: %s\n", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		}
		if out == gamelogic.WarOutcomeDraw {
			message := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := publishGameLog(ch, routing.GameLog{
				Username: rec.Defender.Username,
				Message:  message,
			})
			if err != nil {
				fmt.Printf("error publishing game log: %s\n", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		} else {
			fmt.Println("err")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp.Channel, log routing.GameLog) error {
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+log.Username, log)
	if err != nil {
		fmt.Printf("error publishing game log: %s\n", err)
		return err
	}
	return nil
}
