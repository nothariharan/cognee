import asyncio

from ..tasks.task import Task


def run_tasks_parallel(tasks: list[Task]) -> Task:
    """Run every task concurrently and return the last-listed task's result.

    ``asyncio.gather`` preserves declaration order, so with several tasks this is the
    result of the last task in ``tasks``, not the last one to finish. A single task
    returns its own result rather than an empty list.

    Known sharp edge, unchanged here: an empty ``tasks`` list returns ``[]``, which
    replaces the incoming pipeline data for the next stage. ``run_tasks_base`` no-ops
    on an empty task list instead. Worth its own issue.

    Returns a ``Task``, not a bare coroutine.
    """

    async def parallel_run(*args, **kwargs):
        parallel_tasks = [asyncio.create_task(task.run(*args, **kwargs)) for task in tasks]

        results = await asyncio.gather(*parallel_tasks)
        return results[-1] if results else []

    return Task(parallel_run)
