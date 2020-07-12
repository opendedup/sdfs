VERSION=3.11.0
rm -rf pkgs
mkdir pkgs
docker build -t sdfs-base:${VERSION} -f Dockerfile.base .
docker build -t sdfs-build:${VERSION} --target builder .
docker build -t sdfs:${VERSION} -t sdfs:latest .
docker run --rm sdfs-build:${VERSION} | tar --extract --verbose -C pkgs/