# Variables
SA_FILE=service-account.json
ENV_FILE=.env_encoded

.PHONY: all install encode up clean help

# Default task: Install, Encode, and Start
all: install encode up

# 1. Install Python requirements
install:
	@echo "Installing requirements..."
	pip install -r requirements.txt

# 2. Encode the Service Account key into .env_encoded
encode:
	@if [ ! -f $(SA_FILE) ]; then \
		echo "ERROR: $(SA_FILE) not found! Please place your GCP JSON key in the root folder."; \
		exit 1; \
	fi
	@echo "Encoding $(SA_FILE) to $(ENV_FILE)..."
	@echo "SECRET_GCP_SERVICE_ACCOUNT=$$(cat $(SA_FILE) | base64 | tr -d '\n')" > $(ENV_FILE)
	@echo "Encoding complete."

# 3. Start the project with Docker
up:
	@echo "Starting Docker Compose..."
	docker compose --env-file $(ENV_FILE) up

# Stop and clean up
down:
	docker compose down

clean:
	rm -f $(ENV_FILE)
	@echo "Cleaned up $(ENV_FILE)"

help:
	@echo "Available commands:"
	@echo "  make install - Install python requirements"
	@echo "  make encode  - Encode service-account.json into .env_encoded"
	@echo "  make up      - Run docker-compose with the encoded env"
	@echo "  make all     - Run all of the above"