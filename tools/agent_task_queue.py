"""
Redis-backed task queue for multi-agent systems.
MFS Corp - 15 agents, 24/7 autonomous operation.
"""

import json
import time
import uuid
from typing import Optional, Dict, Any, List
import redis


class TaskQueue:
    def __init__(self, redis_url: str = 'redis://localhost:6379', queue_name: str = 'agent_tasks'):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.queue_name = queue_name
        self.tasks_key = f"{queue_name}:pending"
        self.claimed_key = f"{queue_name}:claimed"
        self.failed_key = f"{queue_name}:failed"
        self.dlq_key = f"{queue_name}:dlq"
    
    def submit(self, task: Dict[str, Any]) -> str:
        task_id = str(uuid.uuid4())
        task['id'] = task_id
        task['created_at'] = time.time()
        task['retries'] = 0
        priority = task.get('priority', 5)
        self.redis.zadd(self.tasks_key, {json.dumps(task): -priority})
        return task_id
    
    def claim(self, agent_id: str, timeout: int = 3600) -> Optional[Dict]:
        tasks = self.redis.zrange(self.tasks_key, 0, 0)
        if not tasks:
            return None
        task_json = tasks[0]
        task = json.loads(task_json)
        claim_key = f"{self.queue_name}:claim:{task['id']}"
        if self.redis.set(claim_key, agent_id, nx=True, ex=timeout):
            self.redis.zrem(self.tasks_key, task_json)
            self.redis.hset(self.claimed_key, task['id'], json.dumps({
                'task': task, 'agent': agent_id, 'claimed_at': time.time()
            }))
            return task
        return None
    
    def complete(self, task_id: str) -> bool:
        self.redis.hdel(self.claimed_key, task_id)
        return True
    
    def fail(self, task_id: str, max_retries: int = 3) -> bool:
        data = self.redis.hget(self.claimed_key, task_id)
        if data:
            task = json.loads(data)['task']
            task['retries'] = task.get('retries', 0) + 1
            if task['retries'] >= max_retries:
                self.redis.lpush(self.dlq_key, json.dumps(task))
            else:
                delay = 60 * (2 ** task['retries'])
                task['retry_at'] = time.time() + delay
                self.redis.zadd(self.tasks_key, {json.dumps(task): -task.get('priority', 5)})
            self.redis.hdel(self.claimed_key, task_id)
            return True
        return False
    
    def get_stats(self) -> Dict:
        return {
            'pending': self.redis.zcard(self.tasks_key),
            'claimed': self.redis.hlen(self.claimed_key),
            'failed': self.redis.scard(self.failed_key),
            'dlq': self.redis.llen(self.dlq_key)
        }
