NAME=lambda_run_batchjob
TAG=00.00.08

all: build push

build:
	docker build -t $(NAME):$(TAG) -f Dockerfile ../../
	docker tag $(NAME):$(TAG) $(REGISTRY):$(TAG)
	docker tag $(REGISTRY):$(TAG) $(REGISTRY):latest

push:
	docker push $(REGISTRY):$(TAG)
	docker push $(REGISTRY):latest
