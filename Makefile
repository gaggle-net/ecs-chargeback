PROFILE ?= staging

LAMBDA_REQS_IN=ecs_chargeback/requirements.in
LAMBDA_REQS_OUT=ecs_chargeback/requirements.txt
INFRA_REQS_IN=infrastructure/requirements.in
INFRA_REQS_OUT=infrastructure/requirements.txt

.PHONY: help
help: ## Show this help
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

.PHONY: update
update: ## Use pip-compile to update the lambda and infrastructure requirements files
	@pip-compile --no-emit-index-url --output-file $(LAMBDA_REQS_OUT) $(LAMBDA_REQS_IN)
	@pip-compile --no-emit-index-url --output-file $(INFRA_REQS_OUT) $(INFRA_REQS_IN)

.PHONY: upgrade
upgrade: ## Use pip-compile to upgrade the lambda and infrastructure packages
	@pip-compile --upgrade --no-emit-index-url --output-file $(LAMBDA_REQS_OUT) $(LAMBDA_REQS_IN)
	@pip-compile --upgrade --no-emit-index-url --output-file $(INFRA_REQS_OUT) $(INFRA_REQS_IN)

.PHONY: diff
diff: ## Run cdk diff using the AWS profile in PROFILE (default: staging)
	cd infrastructure && cdk diff --profile $(PROFILE)

.PHONY: deploy
deploy: ## Run cdk deploy using the AWS profile in PROFILE (default: staging)
	cd infrastructure && cdk deploy --profile $(PROFILE)
