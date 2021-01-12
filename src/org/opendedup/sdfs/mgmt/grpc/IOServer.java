package org.opendedup.sdfs.mgmt.grpc;

import org.apache.log4j.Logger;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.internal.ServerStream;
import io.grpc.internal.ServerTransportListener;
import io.grpc.ServerCallHandler;
import io.grpc.Metadata.Key;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContextBuilder;

public class IOServer {
  private Server server;
  private Logger logger = SDFSLogger.getLog();

  private SslContextBuilder getSslContextBuilder(String certChainFilePath, String privateKeyFilePath) {
    SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(new File(certChainFilePath),
        new File(privateKeyFilePath));
    return GrpcSslContexts.configure(sslClientContextBuilder);
  }

  public static boolean keyFileExists() {
    String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
    String certChainFilePath = keydir + File.separator + "tls_key.pem";
    String privateKeyFilePath = keydir + File.separator + "tls_key.key";
    Map<String, String> env = System.getenv();
    if (env.containsKey("SDFS_PRIVATE_KEY")) {
      privateKeyFilePath = env.get("SDFS_PRIVATE_KEY");
    }
    if (env.containsKey("SDFS_CERT_CHAIN")) {
      certChainFilePath = env.get("SDFS_CERT_CHAIN");
    }
    if (!new File(certChainFilePath).exists()) {
      return false;
    }
    if (!new File(privateKeyFilePath).exists()) {
      return false;
    }
    return true;
  }

  public void start(boolean useSSL,String host,int port) throws IOException {
    String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
    String certChainFilePath = keydir + File.separator + "tls_key.pem";
    String privateKeyFilePath = keydir + File.separator + "tls_key.key";
    Map<String, String> env = System.getenv();
    if (env.containsKey("SDFS_PRIVATE_KEY")) {
      privateKeyFilePath = env.get("SDFS_PRIVATE_KEY");
    }
    if (env.containsKey("SDFS_CERT_CHAIN")) {
      certChainFilePath = env.get("SDFS_CERT_CHAIN");
    }
    /* The port on which the server should run */
    logger.info("Server started, listening on " + host + ":"+port  + " tls = " + useSSL + " threads=" + Main.writeThreads);
    SocketAddress address = new InetSocketAddress(host, port);
    NettyServerBuilder b = NettyServerBuilder.forAddress(address).addService(new VolumeImpl()).executor(Executors.newFixedThreadPool(Main.writeThreads))
        .addService(new FileIOServiceImpl()).intercept(new AuthorizationInterceptor()).addService(new SDFSEventImpl());
    if (useSSL) {
      b.sslContext(getSslContextBuilder(certChainFilePath, privateKeyFilePath).build());
    }
    server = b.build().start();
    logger.info("Server started, listening on " + host + ":"+port + " tls = " + useSSL);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown
        // hook.
        //System.err.println("*** shutting down gRPC server since JVM is shutting down");
        try {
          IOServer.this.stop();
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
        }
        //System.err.println("*** server shut down");
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

  public static class AuthorizationInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
        ServerCallHandler<ReqT, RespT> next) {
      if (Main.sdfsCliRequireAuth) {
        SDFSLogger.getLog().debug(call.getMethodDescriptor().getFullMethodName());
        if (call.getMethodDescriptor().getFullMethodName()
            .equals("org.opendedup.grpc.VolumeService/AuthenticateUser")) {
          SDFSLogger.getLog().debug("Authenticating User");
        } else {
          final String auth_token = headers.get(Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER));
          SDFSLogger.getLog().debug("Token is " + auth_token);
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
                  SDFSLogger.getLog().debug("authenticated " + token.getAudience());
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


}