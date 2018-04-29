for ((i=1;i<=$1;i++))
do
	sudo docker run --rm -d -v ~/work:/home/work crawl_worker:0.1
done
