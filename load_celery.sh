cd /home/work/spiders
celery --loglevel=info --logfile=/home/celery.log -A tasks.scrapy_task worker --max-tasks-per-child 1 -n %h@CCASS 
