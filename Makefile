.PHONY: install run test docker-up docker-down

install:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

run:
	python app.py

test:
	pytest -q

docker-up:
	docker compose up --build

docker-down:
	docker compose down
