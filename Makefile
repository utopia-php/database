
ROOT_DIR 	   := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR	 := $(notdir $(patsubst %/,%,$(dir $(ROOT_DIR))))
PROJECT 		 := $(lastword $(PROJECT_DIR))
VERSION_FILE 	= VERSION
VERSION			 	= `cat $(VERSION_FILE)`

RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
$(eval $(RUN_ARGS):;@:)


.PHONY: help
help: ## Print all the available commands
	@echo "" \
	&& grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' \
	&& echo ""

check: ## Validate the project code
	@echo "Noop"

.PHONY up
up:
  @composer update --ignore-platform-reqs --optimize-autoloader --no-plugins --no-scripts --prefer-dist && docker compose up -d

.PHONY down
down:
	@docker compose down --volumes --timeout=0

.PHONY build
build: ## Builds docker
	@docker compose build

test-mariadb:
	@docker compose exec tests vendor/bin/phpunit tests/Database/Adapter/MariaDBTest.php

.PHONY: test
test: test-mariadb ## Run Tests


#@docker compose exec tests ./vendor/bin/phpunit tests

clean: ## Clean project
	@echo "Noop"

# setup: ## Setup Project env
# php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');"
# php -r "if (hash_file('sha384', 'composer-setup.php') === '906a84df04cea2aa72f40b5f787e49f22d4c2f19492ac310e8cba5b96ac8b64115ac402c8cd292b8a03482574915d1a8') { echo 'Installer verified'; } else { echo 'Installer corrupt'; unlink('composer-setup.php'); } echo PHP_EOL;"
# php composer-setup.php
# php -r "unlink('composer-setup.php');"
# mv composer.phar 