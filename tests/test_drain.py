import asyncio

import pytest
from arq import Drain
from arq.drain import TaskError
from arq.jobs import Job


async def test_drain(redis):
    await redis.rpush(
        b"foobar",
        Job.encode(
            job_id="1",
            class_name="class",
            func_name="func",
            unique=False,
            timeout_seconds=None,
            args=tuple(),
            kwargs={},
        ),
    )
    await redis.rpush(
        b"foobar",
        Job.encode(
            job_id="2",
            class_name="class",
            func_name="func",
            unique=False,
            timeout_seconds=None,
            args=tuple(),
            kwargs={},
        ),
    )
    await redis.rpush(
        b"foobar",
        Job.encode(
            job_id="3",
            class_name="class",
            func_name="func",
            unique=False,
            timeout_seconds=None,
            args=tuple(),
            kwargs={},
        ),
    )
    await redis.rpush(
        b"foobar",
        Job.encode(
            job_id="4",
            class_name="class",
            func_name="func",
            unique=False,
            timeout_seconds=None,
            args=tuple(),
            kwargs={},
        ),
    )
    total = 0

    async def run(job):
        nonlocal total
        total += int(job.id)

    drain = Drain(cancel_queue=b"arq:q:cancel", redis=redis)
    async with drain:
        async for raw_queue, raw_data in drain.iter(b"foobar"):
            assert raw_queue == b"foobar"
            drain.add(run, Job(raw_data, queue_name="foobar"))
    assert total == 10


async def test_drain_error(redis):
    await redis.rpush(
        b"foobar",
        Job.encode(
            job_id="1",
            class_name="class",
            func_name="func",
            unique=False,
            timeout_seconds=None,
            args=tuple(),
            kwargs={},
        ),
    )

    async def run(job):
        raise RuntimeError("snap")

    drain = Drain(cancel_queue=b"arq:q:cancel", redis=redis, raise_task_exception=True)
    with pytest.raises(TaskError) as exc_info:
        async with drain:
            async for raw_queue, raw_data in drain.iter(b"foobar"):
                assert raw_queue == b"foobar"
                drain.add(run, Job(raw_data, queue_name="foobar"))
                break

    assert "TaskError: A processed task failed: RuntimeError, snap" in str(exc_info)


async def test_drain_timeout(redis, caplog):
    await redis.rpush(
        b"foobar",
        Job.encode(
            job_id="1",
            class_name="class",
            func_name="func",
            unique=False,
            timeout_seconds=None,
            args=tuple(),
            kwargs={},
        ),
    )
    await redis.rpush(
        b"foobar",
        Job.encode(
            job_id="1",
            class_name="class",
            func_name="func",
            unique=False,
            timeout_seconds=None,
            args=tuple(),
            kwargs={},
        ),
    )

    async def run(v):
        assert v.id == "1"
        await asyncio.sleep(0.2)

    drain = Drain(
        cancel_queue=b"arq:q:cancel",
        redis=redis,
        max_concurrent_tasks=1,
        semaphore_timeout=0.11,
    )
    async with drain:
        async for _, raw_data in drain.iter(b"foobar"):
            drain.add(run, Job(raw_data, queue_name="foobar"))

    assert "task semaphore acquisition timed after 0.1s" in caplog.messages
