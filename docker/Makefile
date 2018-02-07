VERSION= $(shell grep '_version_' ../arthur/_version.py  | awk -F '"' '{print $$2}') 
IMAGE='grimoirelab/arthur'

build: build_standalone build_server build_worker

publish: publish_standalone publish_server publish_worker

build_standalone:
	@docker build -t $(IMAGE):$(VERSION) --build-arg ARTHUR_VERSION=$(VERSION) -f Dockerfile ..

build_server:
	@docker build -t $(IMAGE):server-$(VERSION) --build-arg ARTHUR_VERSION=$(VERSION) -f Dockerfile.server ..

build_worker:
	@docker build -t $(IMAGE):worker-$(VERSION) --build-arg ARTHUR_VERSION=$(VERSION) -f Dockerfile.worker .

publish_standalone:
	@docker push $(IMAGE):$(VERSION)

publish_server:
	@docker push $(IMAGE):server-$(VERSION)

publish_worker:
	@docker push $(IMAGE):worker-$(VERSION)

