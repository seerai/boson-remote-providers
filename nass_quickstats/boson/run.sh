docker run --rm -it -p 8000:8000 -e PORT=8000 -e API_KEY=${1} us-central1-docker.pkg.dev/double-catfish-291717/seerai-docker/images/nass_quickstats_remote_provider:v0.0.${2}
