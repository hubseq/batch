NAME=lambda_run_pipeline
TAG=00.00.07

all: build push

build:
	docker build --no-cache -t $(NAME):$(TAG) -f Dockerfile ../../
	docker tag $(NAME):$(TAG) $(REGISTRY):$(TAG)
	docker tag $(REGISTRY):$(TAG) $(REGISTRY):latest

push:
	docker push $(REGISTRY):$(TAG)
	docker push $(REGISTRY):latest
