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

# 2. Install requirements (pointing directly to the venv pip)
install:
	$(PIP) install -r requirements.txt

# 3. YOUR SPECIFIC COMMAND (Encoded for Makefile)
# We use > instead of >> to ensure we don't duplicate the line every time you run it
encode:
	@if [ ! -f $(SA_FILE) ]; then echo "Error: $(SA_FILE) not found"; exit 1; fi
	@echo "Encoding Service Account Key..."
	@echo SECRET_GCP_SERVICE_ACCOUNT=$$(cat $(SA_FILE) | base64 -w 0) > $(ENV_ENCODED)

# 4. Docker Compose pointing to the specific env file
up:
	docker compose up

# Cleanup
# clean:
# 	rm -rf $(VENV)
# 	rm -f $(ENV_ENCODED)