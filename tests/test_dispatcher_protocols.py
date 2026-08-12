"""Structural dispatcher contracts keep optional capabilities optional."""

from dewey.dispatcher import AsyncDispatchBackend, DispatchBackend


class MinimalBackend:
    def claim(self, limit):
        return []

    def release(self, task_ids):
        return None

    def run_sweep(self):
        return {}

    def wait_for_work(self, timeout):
        return False

    def close(self):
        return None


class MinimalAsyncBackend:
    async def claim(self, limit):
        return []

    async def release(self, task_ids):
        return None

    async def run_sweep(self):
        return {}

    async def wait_for_work(self, timeout):
        return False

    async def close(self):
        return None


def test_minimal_backend_satisfies_runtime_protocol_without_optional_capabilities():
    backend = MinimalBackend()

    assert isinstance(backend, DispatchBackend)
    assert not hasattr(backend, "next_due")
    assert not hasattr(backend, "heartbeat")


def test_minimal_async_backend_satisfies_runtime_protocol_without_optional_capabilities():
    backend = MinimalAsyncBackend()

    assert isinstance(backend, AsyncDispatchBackend)
    assert not hasattr(backend, "next_due")
    assert not hasattr(backend, "heartbeat")
