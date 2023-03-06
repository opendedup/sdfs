protoc -I=src/proto/ --java_out=src/ src/proto/Storage.proto --grpc-java_out=src/ --plugin=protoc-gen-grpc-java=/usr/sbin/protoc-gen-grpc-java-1.46.0-linux-x86_64.exe
