# README
This project provide three different docker images.
* Arthur
* Arthur Server
* Arthur Worker

## ARTHUR
This docker image run a standalone Arthur service (server and worker).
It can be configured with environment variables.

### ENVIRONMENT
#### ARTHUR_HOST
Define Arthur listening url, default set to `0.0.0.0`

#### ARTHUR_PORT
Define Arthur server listening port, default set to `8080`

#### ARTHUR_DATABASE
Define Redis database url used by Arthur, default set to `redis://localhost/8`

#### ARTHUR_LOGS_DIR
Define Arthur logs directory, default set to `/arthur/logs`

### EXAMPLE
```
docker run -i -t --rm -e 'ARTHUR_DATABASE=redis://redis' grimoirelab/arthur:0.1.3
```

## ARTHUR SERVER
This docker image run an Arthur server.  
It brings more flexibility when used with Arthur workers.   
It can be configured with environment variables.

### ENVIRONMENT
#### ARTHUR_HOST
Define Arthur listening url, default set to `0.0.0.0`

#### ARTHUR_PORT
Define Arthur server listening port, default set to `8080`

#### ARTHUR_DATABASE
Define Redis database url used by Arthur, default set to `redis://localhost/8`

#### ARTHUR_LOGS_DIR
Define Arthur logs directory, default set to `/arthur/logs`

### EXAMPLE
```
docker run -i -t --rm -e 'ARTHUR_DATABASE=redis://redis' grimoirelab/arthur:server-0.1.3
```

## ARTHUR WORKER
This docker image run an Arthur worker.
This image must be used in an association with an arthur server

### CONFIGURATION
#### ARTHUR_DATABASE
Define the Redis database url used by Arthur, default set to ```redis://localhost/8```

#### ARTHUR_WORKER_QUEUE
Define the listened database queue, default set to 'create, update'.  
Only accept value `create` or `update`

### EXAMPLE
```
docker run -i -t --rm -e 'ARTHUR_DATABASE=redis://redis' -e "ARTHUR_WORKER_QUEUE=update" grimoirelab/arthur:worker-0.1.3
```

## DOCKER
### BUILD
In order to build new docker images, just run `make build` to create those images
with tags based on the version defined in file `../arthur/_version.py`.

### PUBLISH
In order to publish docker images on the registry, just run `make publish` to push
docker images.
The variable 'IMAGE', in the Makefile, defines where the image will be publish.
