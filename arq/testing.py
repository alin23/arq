"""
:mod:`testing`
==============

Utils for testing arq.

See arq's own tests for examples of usage.
"""
import logging
from contextlib import contextmanager

from .worker import BaseWorker


logger = logging.getLogger("arq.mock")


class RaiseWorker(BaseWorker):
    """
    Worker which raises exceptions rather than logging them. Useful for testing.
    """

    async def handle_execute_exc(self, started_at, exc, j):
        raise exc

    def handle_prepare_exc(self, msg):
        raise RuntimeError(msg)


@contextmanager
def redis_context_manager(r):
    yield r
