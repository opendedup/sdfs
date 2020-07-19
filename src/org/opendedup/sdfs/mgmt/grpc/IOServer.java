package org.opendedup.sdfs.mgmt.grpc;

import org.apache.log4j.Logger;
import org.opendedup.logging.SDFSLogger;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.opendedup.grpc.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class IOServer {
    private Server server;
    private Logger logger = SDFSLogger.getLog();
    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        logger.info("Server started, listening on " + port);
        server = ServerBuilder.forPort(port)
            .build()
            .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                IOServer.this.stop();
            } catch (InterruptedException e) {
              e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
          }
        });
      }
    
      private void stop() throws InterruptedException {
        if (server != null) {
          server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
      }
    
      /**
       * Await termination on the main thread since the grpc library uses daemon threads.
       */
      private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
          server.awaitTermination();
        }
      }

      static class VolumeImpl extends VolumeServiceGrpc.VolumeServiceImplBase {

        @Override
        public void getVolumeInfo(VolumeInfoRequest req, StreamObserver<VolumeInfoResponse> responseObserver) {
          VolumeInfoResponse reply = VolumeInfoResponse.newBuilder()
          responseObserver.onNext(reply);
          responseObserver.onCompleted();
        }
      }
    
      /**
       * Main launches the server from the command line.
       */
      public static void main(String[] args) throws IOException, InterruptedException {
        final IOServer server = new IOServer();
        server.start();
        server.blockUntilShutdown();
      }
    
}