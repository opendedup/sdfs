VERSION=3.11.0
rm -rf pkgs
mkdir pkgs
docker build -t sdfs-base:${VERSION} -f Dockerfile.base .
docker build -t sdfs-build:${VERSION} --target builder .
docker build -t sdfs:${VERSION} -t sdfs:latest .
docker run --rm sdfs-build:${VERSION} | tar --extract --verbose -C pkgs/
docker run -e TYPE=GOOGLE -e BUCKET_NAME=sdfs-gcp-testing -e GOOGLE_CREDS_FILE=/keys/sdfs-gcp-testing-4762d0dc6cee.json -e PUBSUB_PROJECT=sdfs-gcp-testing -e PUBSUB_CREDS_FILE=/keys/sdfs-gcp-testing-4762d0dc6cee.json -v ../../keys/:/keys -d --name sdfs1 -p 0.0.0.0:6442:6442 sdfs:3.11.0