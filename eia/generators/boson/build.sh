cp ../../../common_data/states.geoparquet .
docker build -t us-central1-docker.pkg.dev/double-catfish-291717/seerai-docker/images/eia_generators_remote_provider:v0.0.${1} -f Dockerfile .
rm states.geoparquet