# pylint: disable=redefined-outer-name
import logging
from pathlib import Path

from aioredis import create_redis, create_redis_pool

import pytest
from arq.testing import RaiseWorker

from .example import ActorTest
from .fixtures import (
    ChildActor,
    CronActor,
    CronWorker,
    DemoActor,
    DemoWorker,
    FastShutdownWorker,
    FoobarActor,
    FoobarActorQueue,
    ParentActor,
    ParentChildActorWorker,
    RealJobActor,
    ReEnqueueActor,
    StartupActor,
    StartupWorker,
    WorkerFail,
    WorkerQuit,
)


@pytest.fixture(scope="function")
def example_file(redis_proc):
    filepath = Path(__file__).resolve().parent / "example.py"
    return filepath.read_text().replace("6379", str(redis_proc.port))


@pytest.fixture(scope="function")
def redis_conn(loop, redis_proc):
    """
    yield fixture which creates a redis connection, and flushes redis before the test.

    Note: redis is not flushed after the test both for performance and to allow later debugging.
    """

    async def _get_conn():
        conn = await create_redis(("localhost", redis_proc.port), loop=loop)
        await conn.flushall()
        return conn

    conn = loop.run_until_complete(_get_conn())
    conn.loop = loop
    yield conn

    conn.close()
    try:
        loop.run_until_complete(conn.wait_closed())
    except RuntimeError:
        pass


@pytest.fixture(scope="function")
def redis(loop, redis_proc):
    """
    yield fixture which creates a redis connection, and flushes redis before the test.

    Note: redis is not flushed after the test both for performance and to allow later debugging.
    """

    async def _create_redis():
        r = await create_redis_pool(("localhost", redis_proc.port), loop=loop)
        await r.flushall()
        return r

    async def _close(r):
        r.close()
        await r.wait_closed()

    redis_ = loop.run_until_complete(_create_redis())
    yield redis_

    loop.run_until_complete(_close(redis_))


def init_actor(Actor, *, loop, redis):
    _actor = Actor(loop=loop)
    _actor.redis = redis
    return _actor


@pytest.fixture(scope="function")
def actor(loop, redis):
    _actor = init_actor(DemoActor, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


@pytest.fixture(scope="function")
def actor_test(loop, redis):
    _actor = init_actor(ActorTest, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


@pytest.fixture(scope="function")
def foobar_actor(loop, redis):
    _actor = init_actor(FoobarActor, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


@pytest.fixture(scope="function")
def foobar_actor_queue(loop, redis):
    _actor = init_actor(FoobarActorQueue, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


@pytest.fixture(scope="function")
def real_job_actor(loop, redis):
    _actor = init_actor(RealJobActor, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


@pytest.fixture(scope="function")
def startup_actor(loop, redis):
    _actor = init_actor(StartupActor, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


@pytest.fixture(scope="function")
def parent_actor(loop, redis):
    _actor = init_actor(ParentActor, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


@pytest.fixture(scope="function")
def child_actor(loop, redis):
    _actor = init_actor(ChildActor, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


@pytest.fixture(scope="function")
def re_enqueue_actor(loop, redis):
    _actor = init_actor(ReEnqueueActor, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


@pytest.fixture(scope="function")
def cron_actor(loop, redis):
    _actor = init_actor(CronActor, loop=loop, redis=redis)
    yield _actor

    loop.run_until_complete(_actor.close())


def init_worker(Worker, actor, shadows=None):
    if shadows is not None:
        _worker = Worker(loop=actor.loop, burst=True, shadows=shadows)
    else:
        _worker = Worker(loop=actor.loop, burst=True)
    _worker.redis = actor.redis
    return _worker


@pytest.fixture(scope="function")
def worker(actor):
    yield init_worker(DemoWorker, actor)


@pytest.fixture(scope="function")
def raise_worker(actor):
    yield init_worker(RaiseWorker, actor)


@pytest.fixture(scope="function")
def worker_with_shadows(actor):
    def _make_worker_with_shadows(shadows):
        _worker = init_worker(DemoWorker, actor, shadows=shadows)
        return _worker

    yield _make_worker_with_shadows


@pytest.fixture(scope="function")
def startup_worker(actor):
    yield init_worker(StartupWorker, actor)


@pytest.fixture(scope="function")
def fast_shutdown_worker(actor):
    yield init_worker(FastShutdownWorker, actor)


@pytest.fixture(scope="function")
def parent_child_actor_worker(actor):
    yield init_worker(ParentChildActorWorker, actor)


@pytest.fixture(scope="function")
def cron_worker(actor):
    yield init_worker(CronWorker, actor)


@pytest.fixture(scope="function")
def worker_quit(actor):
    yield init_worker(WorkerQuit, actor)


@pytest.fixture(scope="function")
def worker_fail(actor):
    yield init_worker(WorkerFail, actor)


@pytest.fixture(scope="function")
def caplog(caplog):
    caplog.clear()
    caplog.set_level(logging.INFO)
    return caplog
