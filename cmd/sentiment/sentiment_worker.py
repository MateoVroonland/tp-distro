import logging
import pika

from internal.sentiment.sentiment_worker import SentimentWorker
from internal.utils.queue import ConsumerQueue, ProducerQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(): 
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq')
        )
        
        input_queue = ConsumerQueue(connection, "movies_metadata_q5", "movies")
        output_queue = ProducerQueue(connection, "movies_sentiment_processed")
        
        worker = SentimentWorker(input_queue, output_queue)
        worker.start()
    except Exception as e:
        logger.error(f"Failed to initialize worker: {e}")


if __name__ == "__main__":
    main()