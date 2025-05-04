import logging
import os
from dotenv import load_dotenv
import pika

from internal.sentiment.sentiment_worker import SentimentWorker
from internal.utils.queue import ConsumerQueue, ProducerQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        load_dotenv()
        previous_replicas = int(os.environ.get('MOVIES_RECEIVER_AMOUNT', '1'))
        next_replicas = int(os.environ.get('SENTIMENT_REDUCER_AMOUNT', '1'))
        
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        
        input_queue = ConsumerQueue(
            connection, 
            "movies_metadata_q5", 
            "movies_metadata_q5", 
            previous_replicas
        )
        
        output_queue = ProducerQueue(
            connection, 
            "movies_sentiment_processed", 
            next_replicas
        )
        
        worker = SentimentWorker(input_queue, output_queue)
        worker.start()     
    except Exception as e:
        logger.error(f"Failed to initialize worker: {e}")

if __name__ == "__main__":
    main()