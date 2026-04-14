import asyncio
import logging
import aio_pika
import json
import os
# Configuration
QUEUE_NAME = "transactions"
from worker.logic import process_transaction
from worker.database import init_db

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def main():
    # Initialize DB tables
    logger.info("Initializing database")
    init_db()

    # RabbitMQ Connection
    logger.info("Connecting to RabbitMQ")
    RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT = os.getenv("RABBITMQ_PORT", "5672")
    RABBIT_URL = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"

    connection = await aio_pika.connect_robust(RABBIT_URL)
    
    async with connection:
        # Creating channel
        channel = await connection.channel()
        # Maximum messages to process at once
        await channel.set_qos(prefetch_count=10)

        # Declaring queue
        queue = await channel.declare_queue("transactions", durable=True)

        logger.info("Worker started. Waiting for transactions...")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        data = json.loads(message.body.decode())
                        logger.info(f"Processing transaction: {data.get('transaction_id')}")
                        await process_transaction(data)
                    except Exception as e:
                        logger.error(f"Failed to process message: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped manually")
