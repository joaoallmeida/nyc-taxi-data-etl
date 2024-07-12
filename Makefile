build:
	docker build -f docker/Dockerfile -t mydagster . 
	docker-compose -f docker/docker-compose.yml up -d
destroy:
	docker-compose -f docker/docker-compose.yml down
	docker image rmi mydagster
