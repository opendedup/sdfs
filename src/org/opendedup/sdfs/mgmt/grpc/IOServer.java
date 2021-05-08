package org.opendedup.sdfs.mgmt.grpc;

import org.apache.log4j.Logger;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
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
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;

public class IOServer {
  private Server server;
  private Logger logger = SDFSLogger.getLog();

  private SslContextBuilder getSslContextBuilder(String certChainFilePath, String privateKeyFilePath) {
    SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(new File(certChainFilePath),
        new File(privateKeyFilePath));
    return GrpcSslContexts.configure(sslClientContextBuilder);
  }

  private SslContextBuilder getSslContextBuilder(String certChainFilePath, String privateKeyFilePath,
      String trustCertCollectionFilePath) {
    SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(new File(certChainFilePath),
        new File(privateKeyFilePath));
    sslClientContextBuilder.trustManager(new File(trustCertCollectionFilePath));
    sslClientContextBuilder.clientAuth(ClientAuth.REQUIRE);
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

  public void start(boolean useSSL, boolean useClientTLS, String host, int port) throws IOException {
    String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
    String certChainFilePath = keydir + File.separator + "tls_key.pem";
    String privateKeyFilePath = keydir + File.separator + "tls_key.key";
    String trustCertCollectionFilePath =  keydir + File.separator + "signer_key.crt";
    Map<String, String> env = System.getenv();
    if (env.containsKey("SDFS_PRIVATE_KEY")) {
      privateKeyFilePath = env.get("SDFS_PRIVATE_KEY");
    }
    if (env.containsKey("SDFS_CERT_CHAIN")) {
      certChainFilePath = env.get("SDFS_CERT_CHAIN");
    }

    if (env.containsKey("SDFS_SIGNER_CHAIN")) {
      trustCertCollectionFilePath = env.get("SDFS_SIGNER_CHAIN");
    }

    /* The port on which the server should run */
    logger.info(
        "Server started, listening on " + host + ":" + port + " tls = " + useSSL + " threads=" + Main.writeThreads);
    SocketAddress address = new InetSocketAddress(host, port);
    NettyServerBuilder b = NettyServerBuilder.forAddress(address).addService(new VolumeImpl()).addService(new StorageServiceImpl())
        .executor(Executors.newFixedThreadPool(Main.writeThreads)).addService(new FileIOServiceImpl())
        .intercept(new AuthorizationInterceptor()).addService(new SDFSEventImpl());
    if (useSSL) {
      if (useClientTLS) {
        b.sslContext(getSslContextBuilder(certChainFilePath, privateKeyFilePath, trustCertCollectionFilePath).build());
      } else {
        b.sslContext(getSslContextBuilder(certChainFilePath, privateKeyFilePath).build());
      }

    }

    server = b.build().start();
    logger.info("Server started, listening on " + host + ":" + port + " tls = " + useSSL + " mtls = " + useClientTLS);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown
        // hook.
        // System.err.println("*** shutting down gRPC server since JVM is shutting
        // down");
        try {
          IOServer.this.stop();
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
        }
        // System.err.println("*** server shut down");
      }
    });
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  public static class AuthorizationInterceptor implements ServerInterceptor {
    public static final Context.Key<JWebToken> USER_IDENTITY = Context.key("identity");

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
        ServerCallHandler<ReqT, RespT> next) {
      if (Main.sdfsCliRequireAuth) {
        SDFSLogger.getLog().debug("authenticated call to " + call.getMethodDescriptor().getFullMethodName());
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
                  Context context = Context.current().withValue(USER_IDENTITY, token);
                  SDFSLogger.getLog().debug("authenticated " + token.getAudience());
                  return Contexts.interceptCall(context, call, headers, next);
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
        SDFSLogger.getLog().debug("Unauthenticated Call to " + call.getMethodDescriptor().getFullMethodName());
      }

      return next.startCall(call, headers);
    }
  }

}