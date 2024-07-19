build:
	docker build --no-cache -f docker/code.Dockerfile -t dagster .
	docker build --no-cache -f docker/streamlit.Dockerfile -t streamlit .
	docker-compose -f docker/docker-compose.yml up -d
	
destroy:
	docker-compose -f docker/docker-compose.yml down
	docker image rmi dagster
	docker image rmi streamlit

setup:
	poetry install
	poetry shell
