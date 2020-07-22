package org.opendedup.sdfs.mgmt.grpc;

import org.apache.log4j.Logger;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.mgmt.CloseFile;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import org.opendedup.buse.sdfsdev.VolumeShutdownHook;
import org.opendedup.grpc.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class IOServer {
  private Server server;
  private Logger logger = SDFSLogger.getLog();

  public void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    logger.info("Server started, listening on " + port);
    server = ServerBuilder.forPort(port).addService(new VolumeImpl()).build().start();
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
    private LoadingCache<String, DirectoryStream<Path>> fileListers = CacheBuilder.newBuilder().maximumSize(100)
        .expireAfterAccess(5, TimeUnit.MINUTES).build(new CacheLoader<String, DirectoryStream<Path>>() {

          @Override
          public DirectoryStream<Path> load(String key) throws Exception {
            throw new IOException("Key Not Found [" + key + "]");
          }

        });

    @Override
    public void getVolumeInfo(VolumeInfoRequest req, StreamObserver<VolumeInfoResponse> responseObserver) {
      VolumeInfoResponse reply = Main.volume.toProtoc();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }

    @Override
    public void getFileInfo(FileInfoRequest req, StreamObserver<FileMessageResponse> responseObserver) {
      if (req.getFileName().equals("lastClosedFile")) {
        FileMessageResponse.Builder b = FileMessageResponse.newBuilder();
        try {
          MetaDataDedupFile mf = CloseFile.lastClosedFile;

          b.addResponse(mf.toGRPC(true));
        } catch (Exception e) {
          SDFSLogger.getLog().debug("unable to fulfill request on file " + req.getFileName(), e);
          b.setError("unable to fulfill request on file " + req.getFileName());
          b.setErrorCode(errorCodes.EIO);
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
        return;
      } else {
        FileMessageResponse.Builder b = FileMessageResponse.newBuilder();

        String internalPath = Main.volume.getPath() + File.separator + req.getFileName();
        File f = new File(internalPath);
        if (!f.exists()) {
          b.setError("File not found " + req.getFileName());
          b.setErrorCode(errorCodes.ENOENT);
          responseObserver.onNext(b.build());
          responseObserver.onCompleted();
          return;
        }
        if (f.isDirectory()) {

          try {
            String guid = UUID.randomUUID().toString();
            if (req.getListGuid() != null) {
              guid = req.getListGuid();
            }
            b.setListGuid(guid);
            Path dir = FileSystems.getDefault().getPath(f.getPath());
            DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
            this.fileListers.put(guid, stream);
            int maxFiles = 1000;
            int cf = 0;
            if (req.getNumberOfFiles() > 0) {
              maxFiles = req.getNumberOfFiles();
            }
            b.setMaxNumberOfFiles(maxFiles);
            for (Path p : stream) {
              File _mf = p.toFile();
              MetaDataDedupFile mf = MetaFileStore.getNCMF(_mf);
              try {
                b.addResponse(mf.toGRPC(req.getCompact()));
              } catch (Exception e) {
                SDFSLogger.getLog().error("unable to load file " + _mf.getPath(), e);
              }
              cf++;
              if (cf == maxFiles) {
                b.setListGuid(guid);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
              }
            }
            this.fileListers.invalidate(guid);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
          } catch (Exception e) {
            SDFSLogger.getLog().error("unable to fulfill request on directory " + req.getFileName(), e);
            b.setError("unable to fulfill request on directory " + req.getFileName());
            b.setErrorCode(errorCodes.EIO);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
          }
        } else {

          try {
            MetaDataDedupFile mf = MetaFileStore.getNCMF(new File(internalPath));
            b.addResponse(mf.toGRPC(req.getCompact()));
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
          } catch (Exception e) {
            SDFSLogger.getLog().error("unable to fulfill request on file " + req.getFileName(), e);
            b.setError("unable to fulfill request on file " + req.getFileName());
            b.setErrorCode(errorCodes.EIO);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
          }
        }
      }

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