import csv
import socket
import logging
from io import StringIO
import time
import signal
import sys

from internal.utils.communication import CompleteSocket
from internal.utils.csv_formatters import process_credits_row, process_movies_row

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Client:
    BATCH_SIZE = 2 * 1024 * 1024
    SERVER_HOST = "requesthandler"
    SERVER_PORT = 8888
    client_id = None

    def __init__(self):
        self.is_running = True
        self.current_connection = None
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logger.info("Received SIGTERM, initiating graceful shutdown")
        self.is_running = False
        if self.current_connection:
            try:
                self.current_connection.close()
                logger.info("Closed current connection")
            except Exception as e:
                logger.error(f"Error closing current connection: {e}")

    def create_tcp_connection(self, host, port):
        if not self.is_running:
            return None
            
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
            self.current_connection = CompleteSocket(sock)
            return self.current_connection
        except (ConnectionError, OSError) as e:
            logger.error(f"Failed to connect to {host}:{port} - {str(e)}")
            if self.current_connection:
                self.current_connection.close()
            self.current_connection = None
            return None
        

    
    def create_batch_from_csv(self, file_path):
        if not self.is_running:
            return
            
        current_batch = ""
        current_batch_size = 0
        file_name = file_path.split("/")[-1]
        file_processor = None
        
        if "movies.csv" in file_name:
            file_processor = process_movies_row
            logger.info("Processing movies.csv file")
        elif "credits.csv" in file_name:
            file_processor = process_credits_row
            logger.info("Processing credits.csv file")
        else:
            logger.info(f"No specific processor for {file_name}, sending raw data")
        
        with open(file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file) 
            for row in csv_reader:
                if not self.is_running:
                    logger.info("Shutdown requested, stopping CSV processing")
                    break
                    
                if not row:
                    continue
                if file_processor:
                    row = file_processor(row)
                    
                output = StringIO()
                csv_writer = csv.writer(output)
                csv_writer.writerow(row)
                encoded_csv_row = output.getvalue()

                line_size = len(encoded_csv_row.encode('utf-8'))
                
                if current_batch_size + line_size > self.BATCH_SIZE:
                    yield current_batch
                    current_batch = ""
                    current_batch_size = 0
                
                current_batch += encoded_csv_row
                current_batch_size += line_size
        
        if current_batch_size > 0 and self.is_running:
            yield current_batch

    def send_file_through_connection(self, file_path):
        if not self.is_running:
            return False
            
        logger.info(f"Starting to send file: {file_path}")
        
        batch_count = 0
        try:
            for batch in self.create_batch_from_csv(file_path):
                if not self.is_running:
                    logger.info("Shutdown requested, stopping file transmission")
                    return False
                    
                batch_count += 1
                if not self.current_connection:
                    return False
                self.current_connection.send_all(batch)
            
            if self.is_running:
                sent = self.current_connection.send_all("FINISHED_FILE")
                logger.info(f"Sent {sent} bytes of FINISHED_FILE")
                logger.info(f"Completed sending file {file_path} with {batch_count} batches")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to send file {file_path}: {str(e)}")
            return False

    def wait_for_results(self):  
        if not self.is_running:
            return None
            
        try:
            self.create_tcp_connection(self.SERVER_HOST, self.SERVER_PORT)
            if not self.current_connection:
                return None
            
            while self.is_running:
                try:
                    self.current_connection.send_all(f"WAITING_FOR_RESULTS:{self.client_id}")
                    logger.info(f"Sent WAITING_FOR_RESULTS:{self.client_id}")
                    data = self.current_connection.recv_all().decode('utf-8')
                    if not data:
                        logger.info("Server closed the connection, all results received")
                        break

                    if data == "NO_RESULTS":
                        logger.info("No results available yet")
                        for _ in range(20):
                            if not self.is_running:
                                break
                            time.sleep(1)
                        continue
                    
                    logger.info(f"Received result: {data}")
                    return data
                except ConnectionError:
                    logger.info("Server closed the connection")
                    break
        except (ConnectionError, OSError) as e:
            logger.error(f"Error while receiving results: {str(e)}")
            return None
        finally:
            if self.current_connection:
                self.current_connection.close()
                self.current_connection = None

    def run(self):
        files = [
            {"path": "/docs/movies.csv"},
            {"path": "/docs/credits.csv"},
            {"path": "/docs/ratings.csv"}
        ]
        
        logger.info("Waiting 15 seconds before starting...")
        for i in range(15):
            if not self.is_running:
                logger.info("Shutdown requested during initial wait")
                return
            time.sleep(1)

        self.create_tcp_connection(self.SERVER_HOST, self.SERVER_PORT)
        if not self.current_connection:
            logger.error("Failed to create TCP connection, aborting")
            return
        
        self.current_connection.send_all("CLIENT_ID_REQUEST")
        logger.info("Waiting for client ID")
        self.client_id = self.current_connection.recv_all().decode('utf-8')
        if not self.client_id:
            logger.error("Failed to receive client ID, aborting")
            return
        logger.info(f"Client ID: {self.client_id}")
            
        try:
            logger.info("Starting data transmission through single connection")
            self.current_connection.send_all(f"STARTING_FILE")
            for file_info in files:
                if not self.is_running:
                    logger.info("Shutdown requested during file transmission")
                    break
                    
                file_path = file_info["path"]
                success = self.send_file_through_connection(file_path)
                if not success:
                    logger.error(f"Failed to send {file_path}, aborting remaining files")
                    break
        except Exception as e:
            logger.error(f"Error during data transmission: {str(e)}")
        finally:
            if self.current_connection:
                self.current_connection.close()
                self.current_connection = None
                logger.info("Data transmission connection closed")

        if self.is_running:
            logger.info("Waiting for results")
            self.wait_for_results()
        else:
            logger.info("Shutdown requested, skipping results wait")

if __name__ == "__main__":
    client = Client()
    client.run()
