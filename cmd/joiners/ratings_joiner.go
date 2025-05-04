package main

func main() {
	// err := env.LoadEnv()
	// if err != nil {
	// 	log.Fatalf("Failed to load environment variables: %v", err)
	// }

	// conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	// if err != nil {
	// 	log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	// }
	// defer conn.Close()

	// stringID := strconv.Itoa(env.AppEnv.ID)

	// ratingsJoinerConsumer, err := utils.NewConsumerQueueWithRoutingKey(conn, "ratings_joiner", "ratings_joiner", stringID, "ratings_joiner_ratings_internal")
	// if err != nil {
	// 	log.Fatalf("Failed to declare a queue: %v", err)
	// }

	// moviesJoinerConsumer, err := utils.NewConsumerQueueWithRoutingKey(conn, "movies_filtered_by_year_q3", "movies_filtered_by_year_q3", stringID, "ratings_joiner_movies_internal")
	// if err != nil {
	// 	log.Fatalf("Failed to declare a queue: %v", err)
	// }

	// sinkProducer, err := utils.NewProducerQueue(conn, "q3_sink", "q3_sink")
	// if err != nil {
	// 	log.Fatalf("Failed to declare a queue: %v", err)
	// }

	// ratingsJoiner := joiners.NewRatingsJoiner(ratingsJoinerConsumer, moviesJoinerConsumer, sinkProducer)
	// log.Printf("Ratings joiner initialized with id '%d'", env.AppEnv.ID)
	// ratingsJoiner.JoinRatings()

}
