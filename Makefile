ROOT_DIR 	   := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR	 := $(notdir $(patsubst %/,%,$(dir $(ROOT_DIR))))
PROJECT 		 := $(lastword $(PROJECT_DIR))
VERSION_FILE 	= VERSION
VERSION			 	= `cat $(VERSION_FILE)`

RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
$(eval $(RUN_ARGS):;@:)

default: test

.PHONY: help
help: ## Print all the available commands
	@echo "" \
	&& grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' \
	&& echo ""

.PHONY: oop
oop:
	@echo $(RUN_ARGS)
	
.PHONY: test
test: ## Run Tests
	@docker compose exec tests vendor/bin/phpunit --configuration phpunit.xml tests/Database/Adapter/MongoDBTest.php

analyze: ## Analyze code using Psalm
	@docker compose exec tests vendor/bin/psalm --show-info=true

.PHONY: restart
restart: docker-down docker-up ## Setup for development
	
docker-down:
	@docker compose down --rmi all

docker-up:
	@docker compose up -d
