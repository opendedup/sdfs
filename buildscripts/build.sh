VERSION=3.12
docker run --rm gcr.io/hybrics/hybrics-build:${VERSION} | tar --extract --verbose -C pkgs/
gsutil cp pkgs/*.* gs://hybricsbinaries/pkgs/