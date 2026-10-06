"""Tests for run_tasks_parallel.

A single-task pipeline used to return an empty list instead of that task's result,
so the next stage received ``[]`` and the run still reported success.
"""

import pytest

from cognee.modules.pipelines.operations.run_parallel import run_tasks_parallel
from cognee.modules.pipelines.tasks.task import Task


async def _echo(value):
    return value


async def _returns_first(_data):
    return "first"


async def _returns_last(_data):
    return "last"


@pytest.mark.asyncio
async def test_single_task_returns_its_result():
    result = await run_tasks_parallel([Task(_echo)]).run("important-value")

    assert result == "important-value"


@pytest.mark.asyncio
async def test_single_task_forwards_its_result_to_the_next_stage():
    """Pins what the pipeline actually receives, via Task.execute rather than .run."""
    stage = run_tasks_parallel([Task(_echo)])

    forwarded = [item async for item in stage.execute(["payload"], {}, 1)]

    assert forwarded == ["payload"]


@pytest.mark.asyncio
async def test_multiple_tasks_still_return_the_last_result():
    result = await run_tasks_parallel([Task(_returns_first), Task(_returns_last)]).run("x")

    assert result == "last"


@pytest.mark.asyncio
async def test_empty_task_list_returns_empty_list():
    assert await run_tasks_parallel([]).run("anything") == []


@pytest.mark.asyncio
async def test_tasks_receive_the_forwarded_argument():
    seen = []

    async def record(data):
        seen.append(data)
        return data

    await run_tasks_parallel([Task(record)]).run("through")

    assert seen == ["through"]


@pytest.mark.asyncio
async def test_single_task_exception_still_propagates():
    async def boom(_data):
        raise RuntimeError("task failed")

    with pytest.raises(RuntimeError, match="task failed"):
        await run_tasks_parallel([Task(boom)]).run("x")
