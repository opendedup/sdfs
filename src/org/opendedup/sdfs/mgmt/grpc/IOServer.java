package org.opendedup.sdfs.mgmt.grpc;

import org.apache.log4j.Logger;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Metadata.Key;
import io.grpc.stub.StreamObserver;
import org.opendedup.grpc.*;
import org.opendedup.hashing.HashFunctions;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContextBuilder;

public class IOServer {
  private Server server;
  private Logger logger = SDFSLogger.getLog();

  private SslContextBuilder getSslContextBuilder(String certChainFilePath,String privateKeyFilePath) {
    SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(new File(certChainFilePath),
            new File(privateKeyFilePath));
    return GrpcSslContexts.configure(sslClientContextBuilder);
}

  public void start(boolean useSSL) throws IOException {
    String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
    String certChainFilePath = keydir + File.separator + "tls_key.pem";
    String privateKeyFilePath = keydir + File.separator + "tls_key.key";
    Map<String, String> env = System.getenv();
    if(env.containsKey("SDFS_PRIVATE_KEY")) {
      privateKeyFilePath = env.get("SDFS_PRIVATE_KEY");
    }
    if (env.containsKey("SDFS_CERT_CHAIN")) {
      certChainFilePath = env.get("SDFS_CERT_CHAIN");
    }
    /* The port on which the server should run */
    int port = 50051;
    logger.info("Server started, listening on " + port);
    NettyServerBuilder b = NettyServerBuilder.forPort(port).addService(new VolumeImpl()).addService(new FileIOServiceImpl())
    .intercept(new AuthorizationInterceptor());
    if(useSSL) {
      b.sslContext(getSslContextBuilder(certChainFilePath,privateKeyFilePath).build());
    }
    server = b.build().start();
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
    private static final int EXPIRY_DAYS = 90;

    @Override
    public void getVolumeInfo(VolumeInfoRequest req, StreamObserver<VolumeInfoResponse> responseObserver) {
      VolumeInfoResponse reply = Main.volume.toProtoc();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }

    @Override
    public void authenticateUser(AuthenticationRequest req, StreamObserver<AuthenticationResponse> responseObserver) {
      AuthenticationResponse.Builder b = AuthenticationResponse.newBuilder();
      try {
        String hash = HashFunctions.getSHAHash(req.getPassword().trim().getBytes(), Main.sdfsPasswordSalt.getBytes());
        //SDFSLogger.getLog().info("username = " + req.getUsername() + " password = " + req.getPassword() + " hash = " + hash );
        if (req.getUsername().equalsIgnoreCase(Main.sdfsUserName) && hash.equals(Main.sdfsPassword)) {
          JSONObject jwtPayload = new JSONObject();
          jwtPayload.put("status", 0);

          JSONArray audArray = new JSONArray();
          audArray.put("admin");
          jwtPayload.put("sub", req.getUsername() );

          jwtPayload.put("aud", audArray);
          LocalDateTime ldt = LocalDateTime.now().plusDays(EXPIRY_DAYS);
          jwtPayload.put("exp", ldt.toEpochSecond(ZoneOffset.UTC)); // this needs to be configured

          String token = new JWebToken(jwtPayload).toString();
          b.setToken(token);
        } else {
          b.setError("Unable to Authenticate User");
          b.setErrorCode(errorCodes.EACCES);
        }
      } catch (Exception e) {
        SDFSLogger.getLog().error("unable to authenticat user", e);
        b.setError("Unknown error authenticating user");
        b.setErrorCode(errorCodes.EIO);
      }
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }

    @Override
    public void shutdownVolume(ShutdownRequest req, StreamObserver<ShutdownResponse> responseObserver) {
      ShutdownResponse.Builder b = ShutdownResponse.newBuilder();
      try {
        SDFSLogger.getLog().info("shutting down volume");
        System.out.println("shutting down volume");
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

  public static class AuthorizationInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
        ServerCallHandler<ReqT, RespT> next) {
      if (Main.sdfsCliRequireAuth) {
        SDFSLogger.getLog().info(call.getMethodDescriptor().getFullMethodName());
        if (call.getMethodDescriptor().getFullMethodName()
            .equals("org.opendedup.grpc.VolumeService/AuthenticateUser")) {
          SDFSLogger.getLog().info("Authenticating User");
        } else {
          final String auth_token = headers.get(Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER));
          SDFSLogger.getLog().info("Token is " + auth_token);
          if (auth_token == null) {
            throw new StatusRuntimeException(Status.UNAUTHENTICATED);
          } else {
            String[] tokens = auth_token.split(" ");
            if (tokens.length == 2 && tokens[0].toLowerCase().equals("bearer")) {
              try {
                JWebToken token = new JWebToken(tokens[1]);
                if (!token.isValid()) {
                  SDFSLogger.getLog().warn("token not valid for user " + token.getAudience());
                  throw new StatusRuntimeException(Status.UNAUTHENTICATED);
                } else {
                  SDFSLogger.getLog().info("authenticated " + token.getAudience());
                }
              } catch (Exception e) {
                SDFSLogger.getLog().error("unable to authenticate user", e);
                throw new StatusRuntimeException(Status.INTERNAL);
              }

            } else {
              SDFSLogger.getLog().error("authorization header must start with bearer and include the token");
              throw new StatusRuntimeException(Status.UNAUTHENTICATED);
            }

          }

        }
      } else {

      }

      return next.startCall(call, headers);
    }
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    final IOServer server = new IOServer();
    server.start(false);
    server.blockUntilShutdown();
  }

}