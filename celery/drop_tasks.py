import json
import logging
from itertools import chain
from json import JSONDecodeError

from redis import StrictRedis

from celery.app import Celery
from celery.app.control import Control, Inspect


def drop_celery_tasks(
    task_name: str,
    queue_name,
    celery_app: Celery,
    redis_client: StrictRedis,
    in_workers: bool = False,
):
    """
    Drop all **tasks queued** that match the `task_name` and `queue_name` passed as parameter. There is no
    celery command available atm for this purpose, therefore we need to
    read the celery queue (Redis backend), identify the IDs of the tasks and then revoke them.

    Params:
    #:param task_name: Path to the celery task.
    #:param queue_name: Name of the queue from which you which to delete the the queued tasks.
    #:param celery_app: Main celery application.
    #:param redis_client: Redis client.
    #:param in_workers: Specify whether the tasks pre-fetched or fetched by the workers should be revoked. If the value
        is set to `1`, it will revoke active, scheduled, and reserved tasks fetched by the workers.
        The tasks that are currently executing will not be terminated, instead the new tasks in the queue will not be
        accepted. Use with caution, this option might take a while to execute and is not recommended for prod env.
        More information in: https://docs.celeryproject.org/en/stable/userguide/monitoring.html.

    For reference a Redis item on the queue looks like:
    "{\"body\": \"gAIpfXEAfXEBKFgJAAAAY2FsbGJhY2tzcQJOWAgAAABlcnJiYWNrc3EDTlgFAAAAY2hhaW5xBE5YBQAAAGNob3JkcQ
    VOdYdxBi4=\", \"content-encoding\": \"binary\", \"content-type\": \"application/x-python-serialize\",
    \"headers\": {\"lang\": \"py\", \"task\": \"hi.tasks.on\",
    \"id\": \"9fbcc18e-45d5-4b9f-b667-bd351568a361\", \"shadow\": null, \"eta\": null,
    \"expires\": null, \"group\": null, \"retries\": 0, \"timelimit\": [null, null],
    \"root_id\": \"9fbcc18e-45d5-4b9f-b667-bd351568a361\", \"parent_id\": null, \"argsrepr\": \"()\",
    \"kwargsrepr\": \"{}\", \"origin\": \"gen1@c60fdf6f1554\", \"span_map\":
    {\"uber-trace-id\": \"635914c782f0c52f:8a07796eaedf05d1:0:1\"}},
    \"properties\": {\"correlation_id\": \"9fbcc18e-45d5-4b9f-b667-bd351568a361\", \"reply_to\":
    \"ac8ee0ea-4d30-3065-97da-5a527f7a1fc5\", \"delivery_mode\": 2, \"delivery_info\":
    {\"exchange\": \"\", \"routing_key\": \"default\"}, \"priority\": 0,
        \"body_encoding\": \"base64\", \"delivery_tag\": \"5626fd36-bfc6-4ac5-b137-943a6067fcf1\"}}"
    """

    def _get_tasks_id(workers: list, tasks_ids: list, task_name: str):
        """
        Get task ids with the given name included inside the given `workers` tasks.
        {'worker1.example.com': [
             {'name': 'tasks.sleeptask', 'id': '32666e9b-809c-41fa-8e93-5ae0c80afbbf',
              'args': '(8,)', 'kwargs': '{}'}]
        }
        """
        for worker in workers:
            if not workers[worker]:
                continue
            for _task in workers[worker]:
                if _task["name"].split(".")[-1] == task_name:
                    tasks_ids.append(_task["id"])

    i = Inspect(app=celery_app)  # Inspect all nodes.
    registered = i.registered()
    if not registered:
        raise Exception("No registered tasks found")

    if not any(task_name == _task for _task in chain(*list(registered.values()))):
        logging.error(f"Command could not be executed, because task is not registered: {task_name}")
        return

    tasks_ids = []

    # Revoke tasks already in the broker.
    if in_workers:
        _get_tasks_id(i.active(), tasks_ids, task_name)
        _get_tasks_id(i.scheduled(), tasks_ids, task_name)
        _get_tasks_id(i.reserved(), tasks_ids, task_name)

        if tasks_ids:
            for task_id in tasks_ids:
                Control(app=celery_app).revoke(task_id)
        else:
            logging.info(f"No active/scheduled/registered task found with the name {task_name}")

    # revoke tasks in the redis queue.
    queue_length = redis_client.llen(queue_name)
    if queue_length == 0:
        logging.info(f"No items found in queue: {queue_name}")
        return

    n = 0
    batch_size = 10
    while True:
        items = redis_client.lrange(queue_name, n, n + batch_size)
        n += batch_size
        if not items:
            break

        for item in items:
            try:
                queued_item = json.loads(item)
            except JSONDecodeError as e:
                logging.info(f"Error decoding item from queue: {e.msg}")
                continue
            header = queued_item.get("headers")
            if header and header["task"] == task_name:
                task_id = queued_item["headers"]["id"]
                logging.info(f"revoking task id {task_id}")
                Control(app=celery_app).revoke(task_id)
