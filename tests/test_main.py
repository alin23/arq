import logging
import re
from asyncio import Future

import msgpack

import pytest
from arq import Actor, Job, concurrent
from arq.jobs import ArqError


async def test_simple_job_dispatch(tmpworkdir, actor):
    job = await actor.add_numbers(1, 2)
    assert job
    assert job.func_name == "add_numbers"

    assert not tmpworkdir.join("add_numbers").exists()
    assert await actor.redis.llen(b"arq:q:dft") == 1
    data = msgpack.unpackb(await actor.redis.lpop(b"arq:q:dft"), raw=False)
    # timestamp
    assert 1e12 < data.pop(0) < 3e12
    assert data == ["DemoActor", "add_numbers", False, 0, 0, [1, 2], {}, "__id__"]


async def test_concurrency_disabled_job_dispatch(tmpworkdir, actor):
    await actor.add_numbers.direct(1, 2)

    assert tmpworkdir.join("add_numbers").read_text("utf8") == "3"
    assert await actor.redis.llen(b"arq:q:dft") == 0


async def test_enqueue_redis_job(actor, redis_conn):
    assert not await redis_conn.exists(b"arq:q:dft")

    job = await actor.add_numbers(1, 2)
    assert job
    assert job.func_name == "add_numbers"

    assert await redis_conn.exists(b"arq:q:dft")
    dft_queue = await redis_conn.lrange(b"arq:q:dft", 0, -1)
    assert len(dft_queue) == 1
    data = msgpack.unpackb(dft_queue[0], raw=False)
    # timestamp
    assert 1e12 < data.pop(0) < 3e12
    assert data == ["DemoActor", "add_numbers", False, 0, 0, [1, 2], {}, "__id__"]


async def test_dispatch_work(tmpworkdir, caplog, actor, worker):
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, "arq.main")
    caplog.set_level(logging.DEBUG, "arq.utils")
    caplog.set_level(logging.DEBUG, "arq.work")
    caplog.set_level(logging.DEBUG, "arq.control")
    caplog.set_level(logging.DEBUG, "arq.jobs")

    job = await actor.add_numbers(1, 2)
    assert job
    assert job.func_name == "add_numbers"

    job = await actor.high_add_numbers(3, 4, c=5)
    assert job
    assert job.func_name == "high_add_numbers"

    assert await actor.redis.llen(b"arq:q:dft") == 1
    assert await actor.redis.llen(b"arq:q:high") == 1
    assert caplog.messages == [
        "DemoActor.add_numbers → dft",
        "DemoActor.high_add_numbers → high",
    ]

    assert not tmpworkdir.join("add_numbers").exists()
    await worker.run()
    assert tmpworkdir.join("add_numbers").read() == "3"
    log = re.sub(r"0.0\d\ds", "0.0XXs", caplog.text)
    log = re.sub(r"arq:quit-.*", "arq:quit-<random>", log)
    log = re.sub(r"\d{4}-\d+-\d+ \d+:\d+:\d+", "<date time>", log)
    log = re.sub(r"\w{3}-\d+ \d+:\d+:\d+", "<date time2>", log)
    log = re.sub(r".+\.py\s+\d+\s+(DEBUG|INFO)\s+", "", log)
    log = re.sub(r"(DEBUG|INFO|ERROR)\s+.+\.py:\d+\s+", "", log)
    log = re.sub(str(actor.redis.address[1]), "REDIS_PORT", log)
    assert log.strip().split("\n") == [
        "DemoActor.add_numbers → dft",
        "DemoActor.high_add_numbers → high",
        "Initialising work manager, burst mode: True, creating shadows...",
        'Using first shadows job class "JobConstID"',
        "Running worker with 1 shadow listening to 3 queues",
        "shadows: DemoActor | queues: high, dft, low",
        "Creating tcp connection to ('localhost', REDIS_PORT)",
        "recording health: <date time2> j_complete=0 j_failed=0 j_timedout=0 j_expired=0 j_ongoing=0 q_high=1 q_dft=1 q_low=0",
        "starting main blpop loop",
        "populating quit queue to prompt exit: arq:quit-<random>",
        "task semaphore locked: False",
        "yielding job, jobs in progress 1",
        "scheduling job <Job __id__ DemoActor.high_add_numbers(3, 4, c=5) on high>, re-enqueue: False",
        "task semaphore locked: False",
        "high queued  0.0XXs → __id__ DemoActor.high_add_numbers(3, 4, c=5)",
        "high ran in  0.0XXs ← __id__ DemoActor.high_add_numbers ● 12",
        "task complete, 1 jobs done, 0 failed",
        "yielding job, jobs in progress 1",
        "scheduling job <Job __id__ DemoActor.add_numbers(1, 2) on dft>, re-enqueue: False",
        "task semaphore locked: False",
        "dft  queued  0.0XXs → __id__ DemoActor.add_numbers(1, 2)",
        "dft  ran in  0.0XXs ← __id__ DemoActor.add_numbers ● ",
        "task complete, 2 jobs done, 0 failed",
        "got job from the quit queue, stopping",
        "shutting down worker after 0.0XXs ◆ 2 jobs done ◆ 0 failed ◆ 0 timed out ◆ 0 expired",
        "Closed 2 connection(s)",
    ]


async def test_handle_exception(actor, worker, caplog):
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, "arq.main")
    caplog.set_level(logging.DEBUG, "arq.utils")
    caplog.set_level(logging.DEBUG, "arq.work")
    caplog.set_level(logging.DEBUG, "arq.control")
    caplog.set_level(logging.DEBUG, "arq.jobs")
    assert caplog.text == ""

    job = await actor.boom()
    assert job
    assert job.func_name == "boom"

    await worker.run()
    log = re.sub(r"0.0\d\ds", "0.0XXs", caplog.text)
    log = re.sub(r", line \d+,", ", line <no>,", log)
    log = re.sub(r"arq:quit-.*", "arq:quit-<random>", log)
    log = re.sub(r'"/.*?/(\w+/\w+)\.py"', r'"/path/to/\1.py"', log)
    log = re.sub(r"\d{4}-\d+-\d+ \d+:\d+:\d+", "<date time>", log)
    log = re.sub(r"\w{3}-\d+ \d+:\d+:\d+", "<date time2>", log)
    log = re.sub(r".+\.py\s+\d+\s+(DEBUG|INFO|ERROR)\s+", "", log)
    log = re.sub(r"(DEBUG|INFO|ERROR)\s+.+\.py:\d+\s+", "", log)
    log = re.sub(str(actor.redis.address[1]), "REDIS_PORT", log)

    assert log.strip().split("\n") == [
        "DemoActor.boom → dft",
        "Initialising work manager, burst mode: True, creating shadows...",
        'Using first shadows job class "JobConstID"',
        "Running worker with 1 shadow listening to 3 queues",
        "shadows: DemoActor | queues: high, dft, low",
        "Creating tcp connection to ('localhost', REDIS_PORT)",
        "recording health: <date time2> j_complete=0 j_failed=0 j_timedout=0 j_expired=0 j_ongoing=0 q_high=0 q_dft=1 q_low=0",
        "starting main blpop loop",
        "populating quit queue to prompt exit: arq:quit-<random>",
        "task semaphore locked: False",
        "yielding job, jobs in progress 1",
        "scheduling job <Job __id__ DemoActor.boom() on dft>, re-enqueue: False",
        "task semaphore locked: False",
        "dft  queued  0.0XXs → __id__ DemoActor.boom()",
        "dft  ran in  0.0XXs ! __id__ DemoActor.boom(): RuntimeError",
        "Traceback (most recent call last):",
        '  File "/path/to/arq/worker.py", line <no>, in run_job',
        "    result = await func(*j.args, **j.kwargs)",
        '  File "/path/to/arq/main.py", line <no>, in direct',
        "    return await self._func(self._self_obj, *args, **kwargs)",
        '  File "/path/to/tests/fixtures.py", line <no>, in boom',
        '    raise RuntimeError("boom")',
        "RuntimeError: boom",
        "task complete, 1 jobs done, 1 failed",
        "got job from the quit queue, stopping",
        "shutting down worker after 0.0XXs ◆ 1 jobs done ◆ 1 failed ◆ 0 timed out ◆ 0 expired",
        "Closed 2 connection(s)",
    ]


async def test_bad_def():
    with pytest.raises(TypeError) as excinfo:

        # pylint: disable=unused-variable
        class BadActor(Actor):
            @concurrent
            def just_a_function(self):
                pass

    assert (
        excinfo.value.args[0]
        == "test_bad_def.<locals>.BadActor.just_a_function is not a coroutine function"
    )


async def test_repeat_queue():
    with pytest.raises(AssertionError) as excinfo:

        # pylint: disable=unused-variable
        class BadActor(Actor):
            queues = ("a", "a")

    assert (
        excinfo.value.args[0]
        == "BadActor looks like it has duplicated queue names: ('a', 'a')"
    )


async def test_custom_name(foobar_actor, worker_with_shadows, caplog):
    assert re.match(r"^<FoobarActor\(foobar\) at 0x[a-f0-9]+>$", str(foobar_actor))

    job = await foobar_actor.concat("123", "456")
    assert job
    assert job.func_name == "concat"

    worker = worker_with_shadows([foobar_actor.__class__])
    await worker.run()
    assert worker.jobs_failed == 0
    assert "foobar.concat(123, 456)" in caplog.text


async def test_call_direct(actor, worker, caplog):
    caplog.set_level(logging.INFO)
    await actor.enqueue_job("direct_method", 1, 2)
    await worker.run()
    assert worker.jobs_failed == 0
    assert worker.jobs_complete == 1
    log = re.sub(r"0.0\d\ds", "0.0XXs", caplog.text)
    log = re.sub(r".+\.py\s+\d+\s+(DEBUG|INFO)\s+", "", log)
    log = re.sub(r"(DEBUG|INFO|ERROR)\s+.+\.py:\d+\s+", "", log)
    assert (
        "dft  queued  0.0XXs → __id__ DemoActor.direct_method(1, 2)\n"
        "dft  ran in  0.0XXs ← __id__ DemoActor.direct_method ● 3"
    ) in log


async def test_direct_binding(actor, worker, caplog):
    caplog.set_level(logging.INFO)
    worker = worker

    job = await actor.concat("a", "b")
    assert job
    assert job.func_name == "concat"

    assert await actor.concat.direct("a", "b") == "a + b"
    await worker.run()
    assert worker.jobs_failed == 0
    assert worker.jobs_complete == 1
    assert "DemoActor.concat" in caplog.text
    assert "DemoActor.concat.direct" not in caplog.text


async def test_dynamic_worker(tmpworkdir, actor, worker):
    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join("add_numbers").exists()
    await worker.run()
    await actor.close()
    assert tmpworkdir.join("add_numbers").exists()
    assert tmpworkdir.join("add_numbers").read() == "3"


async def test_dynamic_worker_mocked(tmpworkdir, actor, worker):
    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join("add_numbers").exists()
    await worker.run()
    await actor.close()
    assert tmpworkdir.join("add_numbers").exists()
    assert tmpworkdir.join("add_numbers").read() == "3"


async def test_dynamic_worker_custom_queue(
    tmpworkdir, foobar_actor_queue, worker_with_shadows
):
    await foobar_actor_queue.enqueue_job("add_numbers", 1, 1, queue="foobar")
    assert not tmpworkdir.join("add_numbers").exists()
    worker = worker_with_shadows(shadows=[foobar_actor_queue.__class__])
    worker.queues = foobar_actor_queue.queues
    await worker.run()
    await foobar_actor_queue.close()
    assert tmpworkdir.join("add_numbers").exists()
    assert tmpworkdir.join("add_numbers").read() == "2"


async def test_worker_no_shadow(worker):
    worker.shadows = None
    with pytest.raises(TypeError) as excinfo:
        await worker.run()
    assert excinfo.value.args[0] == "shadows not defined on worker"


async def test_mocked_actor(mocker, actor, loop):
    m = mocker.patch("tests.fixtures.DemoActor.direct_method")
    r = Future(loop=loop)
    r.set_result(123)
    m.return_value = r
    v = await actor.direct_method(1, 1)
    assert v == 123


async def test_actor_wrapping(actor):
    assert actor.add_numbers.__doc__ == "add_number docs"
    assert actor.add_numbers.__name__ == "add_numbers"
    assert repr(actor.add_numbers).startswith(
        "<concurrent function DemoActor.add_numbers of <DemoActor(DemoActor) at"
    )


async def test_bind_replication(
    tmpdir, parent_actor, child_actor, parent_child_actor_worker
):
    file1 = tmpdir.join("test1")
    await parent_actor.save_value(str(file1))
    file2 = tmpdir.join("test2")
    await child_actor.save_value(str(file2))
    await parent_child_actor_worker.run()
    assert file1.read() == "Parent"
    assert file2.read() == "Child"


def test_job_no_queue():
    with pytest.raises(ArqError) as exc_info:
        Job(b"foo")
    assert "either queue_name or raw_queue are required" in str(exc_info)


async def test_encode_set(tmpworkdir, actor, worker):
    await actor.subtract({1, 2, 3, 4}, {4, 5})

    await worker.run()
    assert worker.jobs_failed == 0
    assert tmpworkdir.join("subtract").exists()
    assert tmpworkdir.join("subtract").read() == "{1, 2, 3}"

    await worker.close()
