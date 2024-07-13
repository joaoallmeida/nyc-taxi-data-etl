build:
	docker build --no-cache -f docker/Dockerfile -t dagster . 
	docker-compose -f docker/docker-compose.yml up -d
	
destroy:
	docker-compose -f docker/docker-compose.yml down
	docker image rmi dagster

setup:
	poetry install
	poetry shell