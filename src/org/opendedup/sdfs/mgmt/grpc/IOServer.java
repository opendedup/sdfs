package org.opendedup.sdfs.mgmt.grpc;

import org.apache.log4j.Logger;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import org.opendedup.buse.sdfsdev.VolumeShutdownHook;
import org.opendedup.grpc.*;


import java.io.IOException;

import java.util.concurrent.TimeUnit;


public class IOServer {
  private Server server;
  private Logger logger = SDFSLogger.getLog();

  public void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    logger.info("Server started, listening on " + port);
    server = ServerBuilder.forPort(port).addService(new VolumeImpl()).addService(new FileIOServiceImpl()).build().start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown
        // hook.
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

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon
   * threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  static class VolumeImpl extends VolumeServiceGrpc.VolumeServiceImplBase {
    

    @Override
    public void getVolumeInfo(VolumeInfoRequest req, StreamObserver<VolumeInfoResponse> responseObserver) {
      VolumeInfoResponse reply = Main.volume.toProtoc();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }

    

    @Override
    public void shutdownVolume(ShutdownRequest req, StreamObserver<ShutdownResponse> responseObserver) {
      ShutdownResponse.Builder b = ShutdownResponse.newBuilder();
      try {
        SDFSLogger.getLog().info("shutting down volume");
        System.out.println("shutting down volume");
        VolumeShutdownHook.shutdown();
        System.exit(0);
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
      } catch (Exception e) {
        SDFSLogger.getLog().error("unable to fulfill request", e);
        b.setError("unable to fulfill request");
        b.setErrorCode(errorCodes.EIO);
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
      }

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