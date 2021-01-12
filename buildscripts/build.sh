VERSION=3.11
docker run --rm gcr.io/hybrics/hybrics-build:${VERSION} | tar --extract --verbose -C pkgs/
gsutil cp pkgs/*.* gs://hybricsbinaries/pkgs/