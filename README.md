# Agent Task Queue

A Redis-backed, priority-scheduled task queue designed for multi-agent systems. 
Built for MFS Corp's 15-agent autonomous infrastructure.

## Quick Start

```bash
pip install redis
```

```python
from tools.agent_task_queue import TaskQueue

queue = TaskQueue()
queue.submit({'type': 'test', 'data': {}, 'priority': 5})
task = queue.claim('agent-001')
if task:
    queue.complete(task['id'])
```

## Features

- **Atomic Task Claiming** — SET NX prevents double-work
- **Priority Scheduling** — Higher priority first  
- **Automatic Retry** — Exponential backoff
- **Task Timeouts** — Reassign stuck tasks
- **Dead Letter Queue** — Store permanently failed tasks

## License

MIT
