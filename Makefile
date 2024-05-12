init:
	pip3 install poetry
	poetry install

lint-fix:
	@echo
	@echo --- Lint Fix ---
	pre-commit run --all-files
	@echo --- Lint Completed ---

test:
	@echo
	@echo --- Testing ---
	pytest ${TEST_SUBDIR}
	@echo --- Testing Completed ---

# DOCKER
build:
	docker-compose -f docker-compose.yaml up --build 

up:
	docker-compose -f docker-compose.yaml up -d
