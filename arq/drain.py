"""
:mod:`drain`
============

Drain class used by :class:`arq.worker.BaseWorker` and reusable elsewhere.
"""
import asyncio
import logging
from typing import Dict

import async_timeout
from aioredis import Redis

from arq.utils import gen_random

from .jobs import ArqError


__all__ = ["Drain"]

# these loggers could do with more sensible names
work_logger = logging.getLogger("arq.work")
jobs_logger = logging.getLogger("arq.jobs")


class TaskError(ArqError, RuntimeError):
    pass


# pylint: disable=too-many-instance-attributes
class Drain:
    """
    Drains popping jobs from redis lists and managing a set of tasks with a limited size to execute those jobs.
    """

    def __init__(
        self,
        *,
        cancel_queue: bytes,
        redis: Redis,
        max_concurrent_tasks: int = 50,
        shutdown_delay: float = 6,
        timeout_seconds: int = 0,
        burst_mode: bool = True,
        raise_task_exception: bool = False,
        semaphore_timeout: float = 60,
    ) -> None:
        """
        :param redis: redis pool to get connection from to pop items from list, also used to optionally
            re-enqueue pending jobs on termination
        :param max_concurrent_tasks: maximum number of jobs which can be execute at the same time by the event loop
        :param shutdown_delay: number of seconds to wait for tasks to finish
        :param timeout_seconds: maximum duration of a job, after that the job will be cancelled by the event loop
        :param burst_mode: break the iter loop as soon as no more jobs are available by adding an sentinel quit queue
        :param raise_task_exception: whether or not to raise an exception which occurs in a processed task
        """
        self.cancel_queue = cancel_queue
        self.redis = redis
        self.loop = redis._pool_or_conn._loop
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_semaphore = asyncio.Semaphore(
            value=max_concurrent_tasks, loop=self.loop
        )
        self.shutdown_delay = max(shutdown_delay, 0.1)
        self.timeout_seconds = timeout_seconds
        self.burst_mode = burst_mode
        self.raise_task_exception = raise_task_exception

        self.pending_tasks: Dict[str, asyncio.futures.Future] = {}
        self.unique_pending_tasks: Dict[str, asyncio.futures.Future] = {}
        self.timeout_callbacks: Dict[str, asyncio.Handle] = {}

        self.task_exception: Exception = None
        self.semaphore_timeout = semaphore_timeout

        self.jobs_complete, self.jobs_failed, self.jobs_timed_out, self.jobs_expired = (
            0,
            0,
            0,
            0,
        )
        self.running = False
        self._finish_lock = asyncio.Lock(loop=self.loop)

    async def __aenter__(self):
        self.running = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        cancelled_tasks = await self.finish()
        if cancelled_tasks:
            raise TaskError(
                f"finishing the drain required {cancelled_tasks} tasks to be cancelled"
            )

        if self.raise_task_exception and self.task_exception:
            e = self.task_exception
            raise TaskError(
                f"A processed task failed: {e.__class__.__name__}, {e}"
            ) from e

    @property
    def jobs_in_progress(self):
        return self.max_concurrent_tasks - self.task_semaphore._value

    async def check_for_cancel_tasks(self, poll_seconds=1):
        while True:
            if not self.running:
                break

            try:
                async for key in self.redis.iscan(match=f"{self.cancel_queue}:*"):
                    job_id = key.decode().rsplit(":", 1)[-1]
                    if job_id in self.pending_tasks:
                        self._cancel_job(self.pending_tasks[job_id], timed_out=False)
                        await self.redis.delete(key)
            except Exception as exc:
                work_logger.exception(exc)
                break

            await asyncio.sleep(poll_seconds)

    async def iter(self, *raw_queues: bytes, pop_timeout=1):
        """
        blpop jobs from redis queues and yield them. Waits for the number of tasks to drop below max_concurrent_tasks
        before popping.

        :param raw_queues: tuple of bytes defining queue(s) to pop from.
        :param pop_timeout: how long to wait on each blpop before yielding None.
        :yields: tuple ``(raw_queue_name, raw_data)`` or ``(None, None)`` if all jobs are empty
        """
        work_logger.debug("starting main blpop loop")
        quit_queue = None
        assert self.running, "drain iter will only work when the drain is running"
        if self.burst_mode:
            quit_queue = b"arq:quit-" + gen_random()
            work_logger.debug(
                "populating quit queue to prompt exit: %s", quit_queue.decode()
            )
            await self.redis.rpush(quit_queue, b"1")
            raw_queues = tuple(raw_queues) + (quit_queue,)
        while True:
            work_logger.debug("task semaphore locked: %r", self.task_semaphore.locked())
            try:
                with async_timeout.timeout(self.semaphore_timeout):
                    await self.task_semaphore.acquire()
            except asyncio.TimeoutError:
                work_logger.warning(
                    "task semaphore acquisition timed after %0.1fs",
                    self.semaphore_timeout,
                )
                continue

            if not self.running:
                break

            with await self.redis as r:
                msg = await r.blpop(*raw_queues, timeout=pop_timeout)
            if msg is None:
                yield None, None

                self.task_semaphore.release()
                continue

            raw_queue, raw_data = msg
            if self.burst_mode and raw_queue == quit_queue:
                work_logger.debug("got job from the quit queue, stopping")
                break

            work_logger.debug(
                "yielding job, jobs in progress %d", self.jobs_in_progress
            )
            yield raw_queue, raw_data

    def add(self, coro, job, re_enqueue=False):
        """
        Start job and add it to the pending tasks set.
        :param coro: coroutine to execute the job
        :param job: job object, instance of :class:`arq.jobs.Job` or similar
        :param re_enqueue: whether or not to re-enqueue the job on finish if the job won't finish in time.
        """
        job_key = f"{job.class_name}.{job.func_name}"
        if job.unique:
            old_task = self.unique_pending_tasks.get(job_key)
            if old_task and not old_task.done() and not old_task.cancelled():
                self._cancel_job(old_task, timed_out=False)

        task = self.loop.create_task(coro(job))
        task.job = job
        task.re_enqueue = re_enqueue

        task.add_done_callback(self._job_callback)

        if job.timeout_seconds is not None:
            timeout_seconds = job.timeout_seconds
        else:
            timeout_seconds = self.timeout_seconds

        timeout_seconds = job.timeout_seconds or self.timeout_seconds
        if timeout_seconds > 0:
            timeout_callback = self.loop.call_later(
                timeout_seconds, self._cancel_job, task
            )
            self.timeout_callbacks[str(job)] = timeout_callback

        self.pending_tasks[job.id] = task
        if job.unique:
            self.unique_pending_tasks[job_key] = task

    async def finish(self, timeout=None):
        """
        Cancel all pending tasks and optionally re-enqueue jobs which haven't finished after the timeout.

        :param timeout: how long to wait for tasks to finish, defaults to ``shutdown_delay``
        """
        timeout = timeout or self.shutdown_delay
        self.running = False
        cancelled_tasks = 0
        if self.pending_tasks:
            async with self._finish_lock:
                work_logger.info(
                    "drain waiting %0.1fs for %d tasks to finish",
                    timeout,
                    len(self.pending_tasks),
                )
                _, pending = await asyncio.wait(
                    self.pending_tasks.values(), timeout=timeout, loop=self.loop
                )
                if pending:
                    pipe = self.redis.pipeline()
                    for task in pending:
                        if task.re_enqueue:
                            pipe.rpush(task.job.raw_queue, task.job.raw_data)

                        self._cancel_timeout_callback(task)
                        task.cancel()
                        cancelled_tasks += 1

                    if pipe._results:
                        await pipe.execute()
                self.pending_tasks = {}
                self.unique_pending_tasks = {}
                self.timeout_callbacks = {}
        return cancelled_tasks

    def _job_callback(self, task):
        self.task_semaphore.release()
        self.jobs_complete += 1
        self._cancel_timeout_callback(task)

        task_exception = task.exception()
        if task_exception:
            self.running = False
            self.task_exception = task_exception
        elif task.result():
            self.jobs_failed += 1
        self._remove_task(task)
        jobs_logger.debug(
            "task complete, %d jobs done, %d failed",
            self.jobs_complete,
            self.jobs_failed,
        )

    def _cancel_timeout_callback(self, task):
        try:
            self.timeout_callbacks[str(task.job)].cancel()
        except KeyError:
            pass

    def _cancel_job(self, task, timed_out=True):
        self._cancel_timeout_callback(task)
        task.cancel()

        if timed_out:
            self.jobs_timed_out += 1
            jobs_logger.error("task timed out %r", task.job)

        self._remove_task(task)

    def _remove_unique_task(self, task):
        job = task.job
        job_key = f"{job.class_name}.{job.func_name}"
        task = self.unique_pending_tasks.get(job_key)
        if task and (task.done() or task.cancelled()):
            try:
                del self.unique_pending_tasks[job_key]
            except KeyError:
                pass

    def _remove_task(self, task):
        job = task.job
        if job.unique:
            self._remove_unique_task(task)

        try:
            del self.pending_tasks[job.id]
        except KeyError:
            pass
