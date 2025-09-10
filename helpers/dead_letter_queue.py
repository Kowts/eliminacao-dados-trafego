
from dataclasses import dataclass
from typing import Any
from aiodiskqueue import Queue
from datetime import datetime
from helpers.utils import setup_logger

# Set up logger for the Dead Letter Queue
logger = setup_logger(__name__)

@dataclass
class FailedTask:
    data: Any
    error_message: str
    timestamp: datetime

class DeadLetterQueue:
    def __init__(self, queue_name: str = "dead_letter_queue.sqlite"):
        self.queue_name = queue_name
        self.dlq: Queue = None  # Dead Letter Queue instance

    async def setup(self) -> None:
        self.dlq = await Queue.create(self.queue_name)
        logger.info("Dead Letter Queue initialized.")

    async def add_failed_task(self, data: Any, error_message: str) -> None:
        task = FailedTask(data=data, error_message=error_message, timestamp=datetime.now())
        await self.dlq.put(task)
        logger.warning(f"Task added to DLQ: {task}")

    async def retry_failed_tasks(self, retry_function) -> None:
        """
        Retry tasks from the DLQ using the provided `retry_function`.
        """
        while self.dlq.qsize() > 0:
            failed_task: FailedTask = await self.dlq.get()
            logger.info(f"Retrying failed task: {failed_task}")
            try:
                await retry_function(failed_task.data)
                await self.dlq.task_done()
            except Exception as e:
                logger.error(f"Failed to process DLQ task: {e}. Re-adding to DLQ.")
                await self.add_failed_task(failed_task.data, str(e))

    async def cleanup(self) -> None:
        """
        Cleanup remaining tasks and ensure that DLQ is emptied correctly.
        """
        while self.dlq.qsize() > 0:
            task = await self.dlq.get()
            logger.info(f"Cleaning up remaining DLQ task: {task}")
            await self.dlq.task_done()
        logger.info("DLQ cleanup completed.")
