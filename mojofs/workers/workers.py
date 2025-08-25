import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Workers:
    def __init__(self, n: int):
        """
        Create a Workers object that allows up to n jobs to execute concurrently.
        """
        if n <= 0:
            raise ValueError("n must be > 0")

        self._available = n  # Available working slots
        self.notify = asyncio.Condition()  # Used to notify waiting tasks
        self.limit = n  # Maximum number of concurrent jobs

    async def take(self):
        """
        Give a job a chance to be executed.
        """
        async with self.notify:
            while self._available == 0:
                logging.info(f"worker take, waiting, available: {self._available}")
                await self.notify.wait()
            self._available -= 1
            logging.info(f"worker take, acquired, available: {self._available}")

    async def give(self):
        """
        Release a job's slot.
        """
        async with self.notify:
            self._available += 1  # Increase available slots
            logging.info(f"worker give, released, available: {self._available}")
            self.notify.notify()  # Notify a waiting task

    async def wait(self):
        """
        Wait for all concurrent jobs to complete.
        """
        async with self.notify:
            while self._available != self.limit:
                logging.info(f"worker wait, waiting, available: {self._available}, limit: {self.limit}")
                await self.notify.wait()
        logging.info("worker wait end")

    async def get_available(self) -> int:
        """
        Return the number of available slots.
        """
        return self._available


async def test_workers():
    """
    Test the Workers class.
    """
    workers = Workers(5)

    async def worker_task(worker):
        await worker.take()
        await asyncio.sleep(3)
        await worker.give()

    tasks = []
    for _ in range(5):
        tasks.append(asyncio.create_task(worker_task(workers)))

    await asyncio.sleep(1)  # Wait for tasks to start
    await workers.wait()

    if await workers.get_available() != workers.limit:
        raise AssertionError("Not all workers were released")
    
    logging.info("test_workers passed")


if __name__ == "__main__":
    asyncio.run(test_workers())