import logging
from transformers import pipeline
import csv
from io import StringIO

MovieID = 0
MovieTitle = 1
MovieBudget = 4
MovieRevenue = 6
MovieOverview = 7

# const (
# 	MovieID = iota
# 	MovieTitle
# 	MovieReleaseDate
# 	MovieGenres
# 	MovieBudget
# 	MovieProductionCountries
# 	MovieRevenue
# 	MovieOverview
# )

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SentimentWorker:
    def __init__(self, input_queue, output_queue):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
        logger.info("Sentiment worker initialized with transformer model")

    def analyze_sentiment(self, text):
        if not text or len(text.strip()) == 0:
            return {"label": "NEUTRAL", "score": 0.5}

        try:
            result = self.sentiment_analyzer(text)
            return result[0]
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return {"label": "ERROR", "score": 0.0}

    def process_message(self, ch, method, properties, body):     
        try:
            message_str = body.decode('utf-8').strip()
            if message_str == "FINISHED":
                logger.info("Received FINISHED message, forwarding...")
                try:
                    self.output_queue.publish("FINISHED")
                except Exception as e:
                    logger.error(f"Failed to forward FINISHED message: {e}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                ch.stop_consuming()
                return
        
            # logger.info(f"Received message: {message_str}")
            csv_reader = csv.reader(StringIO(message_str))
            movie_data = next(csv_reader)
            movie_id = movie_data[MovieID]
            movie_title = movie_data[MovieTitle]
            overview = movie_data[MovieOverview]
            movie_budget = movie_data[MovieBudget]
            movie_revenue = movie_data[MovieRevenue]

            sentiment_result = self.analyze_sentiment(overview)
            
            csv_line = f"{movie_id},{movie_title},{movie_budget},{movie_revenue},{sentiment_result['label']}\n"
            output_buffer = StringIO()
            csv_writer = csv.writer(output_buffer)
            csv_writer.writerow([movie_id, movie_title, movie_budget, movie_revenue, sentiment_result['label']])
            csv_line = output_buffer.getvalue()
    
            self.output_queue.publish(csv_line)
            # logger.info(f"Processed movie: {movie_title} with sentiment: {sentiment_result['label']}")
            ch.basic_ack(delivery_tag=method.delivery_tag)   
        except e:
            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start(self):
        self.input_queue.set_qos(prefetch_count=1)
        self.input_queue.consume(
            callback=self.process_message,
            auto_ack=False
        )
        
        logger.info("Starting to consume messages...")
        try:
            self.input_queue.start_consuming()
        except KeyboardInterrupt:
            logger.info("Sentiment worker stopped")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")