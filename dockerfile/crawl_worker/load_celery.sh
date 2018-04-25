cd /home/work/spiders
celery --loglevel=info -A tasks.scrapy_task worker --max-tasks-per-child 1 -n pb_1@%h
