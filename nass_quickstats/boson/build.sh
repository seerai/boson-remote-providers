cp ../../common_data/*.geoparquet .
docker build -t us-central1-docker.pkg.dev/double-catfish-291717/seerai-docker/images/nass_quickstats_remote_provider:v0.0.${1} -f Dockerfile .
rm *.geoparquet