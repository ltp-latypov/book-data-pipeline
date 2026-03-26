-include .env
export

# Variables
VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
SA_FILE = service-account.json
ENV_ENCODED = .env_encoded

.PHONY: all venv install encode up clean

# Runs everything in one go
all: venv install encode up

# 1. Create virtual environment
venv:
	python3 -m venv $(VENV)

# 2. Install requirements
install:
	$(PIP) install -r requirements.txt

# 3. Encode Service Account AND Project ID
# We use '>' for the first line and '>>' for the second to avoid duplication
encode:
	@if [ ! -f $(SA_FILE) ]; then echo "Error: $(SA_FILE) not found"; exit 1; fi
	@if [ -z "$(GCP_PROJECT_ID)" ]; then echo "Error: GCP_PROJECT_ID not set in .env file"; exit 1; fi
	@echo "Encoding Service Account Key and Project ID..."
	@echo "SECRET_GCP_SERVICE_ACCOUNT=$$(cat $(SA_FILE) | base64 | tr -d '\n')" > $(ENV_ENCODED)
	@echo "GCP_PROJECT_ID=$(GCP_PROJECT_ID)" >> $(ENV_ENCODED)

# 4. Docker Compose with the specific env file
up:
	docker compose --env-file $(ENV_ENCODED) up

# Cleanup
clean:
	rm -rf $(VENV)
	rm -f $(ENV_ENCODED)