cd /home/work/spiders
celery --loglevel=info -A tasks.scrapy_task worker --max-tasks-per-child 1 -n worker_name@%h
