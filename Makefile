.PHONY: backend-venv backend-install frontend-install backend-dev frontend-dev frontend-clean dev ports backend-test test docker-up docker-down

PYTHON ?= python3
BACKEND_VENV ?= backend/.venv
BACKEND_PORT ?= 8001
FRONTEND_PORT ?= 3001

backend-venv:
	cd backend && test -d .venv || $(PYTHON) -m venv .venv

backend-install: backend-venv
	cd backend && .venv/bin/pip install -r requirements.txt

frontend-install:
	cd frontend && npm install

frontend-clean:
	cd frontend && rm -rf .next

ports:
	@echo "Backend port:  $(BACKEND_PORT)"
	@echo "Frontend port: $(FRONTEND_PORT)"

backend-dev:
	@if [ ! -x "$(BACKEND_VENV)/bin/uvicorn" ]; then \
		echo "backend dependencies missing. Run: make backend-install"; \
		exit 1; \
	fi
	@echo "backend -> http://localhost:$(BACKEND_PORT)"
	cd backend && NT_CORS_ORIGINS='*' PYTHONPATH=. .venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port $(BACKEND_PORT)

frontend-dev:
	@echo "frontend -> http://localhost:$(FRONTEND_PORT)"
	cd frontend && NEXT_PUBLIC_API_BASE_URL=http://localhost:$(BACKEND_PORT) npm run dev:clean -- -p $(FRONTEND_PORT)

dev:
	@if [ ! -x "$(BACKEND_VENV)/bin/uvicorn" ]; then \
		echo "backend dependencies missing. Run: make backend-install"; \
		exit 1; \
	fi
	@echo "backend  -> http://localhost:$(BACKEND_PORT)"
	@echo "frontend -> http://localhost:$(FRONTEND_PORT)"
	@trap 'kill 0' INT TERM; \
		(cd backend && NT_CORS_ORIGINS='*' PYTHONPATH=. .venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port $(BACKEND_PORT)) & \
		(cd frontend && NEXT_PUBLIC_API_BASE_URL=http://localhost:$(BACKEND_PORT) npm run dev:clean -- -p $(FRONTEND_PORT)) & \
		wait

backend-test:
	@if [ ! -x "$(BACKEND_VENV)/bin/pytest" ]; then \
		echo "backend test dependencies missing. Run: make backend-install"; \
		exit 1; \
	fi
	cd backend && PYTHONPATH=. .venv/bin/pytest app/tests -q

test: backend-test

docker-up:
	docker compose up --build

docker-down:
	docker compose down
