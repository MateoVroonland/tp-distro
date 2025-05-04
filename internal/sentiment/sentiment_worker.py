import logging
from transformers import pipeline
import csv
from io import StringIO
import signal

NUMBER_OF_SENTIMENT_WORKERS = 3
FINISHED_IDENTIFIER = "FINISHED"

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
        
        self.finished_received = {} 

        signal.signal(signal.SIGTERM, self.signal_handler)
        logger.info("Sentiment worker initialized with transformer model")

    def signal_handler(self, signum, frame):
        logger.info("Received SIGTERM signal, closing connection")
        self.input_queue.close()
        self.output_queue.close()
        exit(0)

    def analyze_sentiment(self, text):
        if not text or len(text.strip()) == 0:
            return {"label": "NEUTRAL", "score": 0.5}

        try:
            result = self.sentiment_analyzer(text)
            return result[0]
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return {"label": "ERROR", "score": 0.0}
        
    def publish_message_wrapper(self, queue, message):
        try:
            queue.publish(message)
        except Exception as e:
            logger.error(f"Failed to forward FINISHED message: {e}")
        
    def parse_and_process_message(self, message_str):
        try:
            csv_reader = csv.reader(StringIO(message_str))
            movie_data = next(csv_reader)
            
            movie_id = movie_data[MovieID]
            movie_title = movie_data[MovieTitle]
            overview = movie_data[MovieOverview]
            movie_budget = movie_data[MovieBudget]
            movie_revenue = movie_data[MovieRevenue]

            sentiment_result = self.analyze_sentiment(overview)
            
            output_buffer = StringIO()
            csv_writer = csv.writer(output_buffer)
            csv_writer.writerow([movie_id, movie_title, movie_budget, movie_revenue, sentiment_result['label']])
            return output_buffer.getvalue(), movie_id
        except Exception as e:
            logger.error(f"CSV parsing error: {e}")
            return None
        
    def handle_message(self, message, ch, method):
        try:
            if message.body.startswith("FINISHED:"):
                client_id = message.client_id
                
                if client_id not in self.finished_received:
                    self.finished_received[client_id] = {}
                
                self.finished_received[client_id][message.body] = True
                
                if len(self.finished_received[client_id]) == self.input_queue.previous_replicas:
                    logger.info(f"Received all FINISHED messages for client {client_id}")
                    del self.finished_received[client_id]
                    self.output_queue.publish_finished(client_id)
                message.ack()
                return
            
            result, movie_id = self.parse_and_process_message(message.body)
            if result:
                self.output_queue.publish(result, message.client_id, movie_id)
                message.ack()
            else:
                logger.error("Failed to process message")
                message.nack(requeue=False)
                
        except Exception as e:
            logger.exception(f"Error processing message: {e}")
            message.nack(requeue=False)

    def process_message(self, ch, method, properties, body):    
        try:
            message_str = body.decode('utf-8').strip()
            self.handle_message(message_str, ch, method)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start(self):
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