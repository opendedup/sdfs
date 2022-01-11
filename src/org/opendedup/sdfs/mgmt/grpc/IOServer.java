package org.opendedup.sdfs.mgmt.grpc;

import static java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory;

import org.apache.log4j.Logger;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.mgmt.grpc.tls.DynamicTrustManager;
import org.opendedup.sdfs.mgmt.grpc.tls.WatchForFile;
import org.opendedup.util.EasyX509TrustManager;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.Metadata.Key;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSession;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.DatatypeConverter;

public class IOServer {
  private Server server;
  private Logger logger = SDFSLogger.getLog();
  private static final String DELIMITER = ";;";
  public static String trustStoreDir;

  private X509Certificate getX509Certificate(String certPath) throws Exception {
    FileInputStream is = null;
    try {
      SDFSLogger.getLog().info("Cert Path  = " + certPath);
      CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
      is = new FileInputStream(certPath);
      X509Certificate cer = (X509Certificate) certFactory.generateCertificate(is);
      return cer;
    } catch (CertificateException e) {
      SDFSLogger.getLog().error("Certification exception caught");
      throw new Exception(e);
    } catch (FileNotFoundException e) {
      SDFSLogger.getLog().error("Certificate file not found");
      throw new Exception(e);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  private SslContextBuilder getSslContextBuilder(String certChainFilePath, String privateKeyFilePath) {
    SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(new File(certChainFilePath),
        new File(privateKeyFilePath));
    return GrpcSslContexts.configure(sslClientContextBuilder);
  }

  private SslContextBuilder getSslContextBuilder(String certChainFilePath, String privateKeyFilePath,
      String trustCertCollectionFilePath) throws Exception {
    SslContextBuilder sslClientContextBuilder = null;
    if (!Main.authJarFilePath.equals("") && !Main.authClassInfo.equals("")) {
      /*
       * Main.classInfo contains class name and its two methods to load separated by
       * ;;
       */
      List<String> classInfo = Arrays.asList(Main.authClassInfo.split(DELIMITER));
      String loadclass = classInfo.get(0);
      String key_method = classInfo.get(1);
      String path_method = classInfo.get(2);
      URL[] classLoaderUrls = new URL[] { new URL("file:" + Main.authJarFilePath) };
      // Create a new URLClassLoader
      URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);
      // Load the target class
      Class<?> beanClass = urlClassLoader.loadClass(loadclass);
      // Create a new instance from the loaded class
      Constructor<?> constructor = beanClass.getConstructor();
      Object beanObj = constructor.newInstance();
      // Getting a method from the loaded class and invoke it
      Method method = beanClass.getMethod(path_method);
      String path = (String) method.invoke(beanObj);
      List<String> prodInfo = Arrays.asList(path.split(DELIMITER));
      certChainFilePath = prodInfo.get(0);
      trustStoreDir = prodInfo.get(1);
      // Getting a method from the loaded class and invoke it
      Method method2 = beanClass.getMethod(key_method);
      PrivateKey pvtKey = (PrivateKey) method2.invoke(beanObj);
      urlClassLoader.close();
      String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
      new File(keydir).mkdirs();
      String _certChainFilePath = keydir + File.separator + "tls_key.pem";
      String _privateKeyFilePath = keydir + File.separator + "tls_key.key";
      FileOutputStream fout = new FileOutputStream(_privateKeyFilePath);
      try {
        writeKey(fout, pvtKey);
      } catch (Exception e) {
        SDFSLogger.getLog().error(e);
      }
      fout = new FileOutputStream(_certChainFilePath);
      X509Certificate serverCertChain = getX509Certificate(certChainFilePath);
      try {
        writeCertificate(fout, serverCertChain);
      } catch (Exception e) {
        SDFSLogger.getLog().error(e);
      }
      EasyX509TrustManager tm = new EasyX509TrustManager();
      sslClientContextBuilder = SslContextBuilder.forServer(pvtKey, serverCertChain)
          .clientAuth(ClientAuth.REQUIRE).trustManager(tm);
    } else {
      sslClientContextBuilder = SslContextBuilder.forServer(new File(certChainFilePath),
          new File(privateKeyFilePath));
      DynamicTrustManager tm = new DynamicTrustManager(new File(trustCertCollectionFilePath).getParent());
      sslClientContextBuilder.trustManager(tm);
      sslClientContextBuilder.clientAuth(ClientAuth.REQUIRE);
      WatchForFile wf = new WatchForFile(tm);
      Thread th = new Thread(wf);
      th.start();
    }
    return GrpcSslContexts.configure(sslClientContextBuilder);
  }

  static void writeBufferBase64(OutputStream out, byte[] bufIn) throws IOException {
    final byte[] buf = DatatypeConverter.printBase64Binary(bufIn).getBytes();
    final int BLOCK_SIZE = 64;
    for (int i = 0; i < buf.length; i += BLOCK_SIZE) {
      out.write(buf, i, Math.min(BLOCK_SIZE, buf.length - i));
      out.write('\r');
      out.write('\n');
    }
  }

  static void writeCertificate(OutputStream out, X509Certificate crt) throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write("-----BEGIN CERTIFICATE-----\r\n".getBytes());
    writeBufferBase64(baos, crt.getEncoded());
    baos.write("-----END CERTIFICATE-----\r\n".getBytes());
    out.write(baos.toByteArray());
    out.flush();
    out.close();
    System.out.println(baos.toString());
  }

  static void writeKey(OutputStream out, PrivateKey pk) throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final String fmt = pk.getFormat();
    if ("PKCS#8".equals(fmt)) {
      baos.write("-----BEGIN PRIVATE KEY-----\r\n".getBytes());
      writeBufferBase64(baos, pk.getEncoded());
      baos.write("-----END PRIVATE KEY-----\r\n".getBytes());
    } else if ("PKCS#1".equals(fmt)) {
      baos.write("-----BEGIN RSA PRIVATE KEY-----\r\n".getBytes());
      writeBufferBase64(baos, pk.getEncoded());
      baos.write("-----END RSA PRIVATE KEY-----\r\n".getBytes());
    }
    out.write(baos.toByteArray());
    out.flush();
    out.close();
    System.out.println(baos.toString());
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
    String trustCertCollectionFilePath = keydir + File.separator + "signer_key.crt";
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

    NettyServerBuilder b = NettyServerBuilder.forAddress(address).addService(new VolumeImpl())
        .addService(new StorageServiceImpl()).executor(getExecutor(Main.writeThreads))
        .maxInboundMessageSize(Main.CHUNK_LENGTH * 3).maxInboundMetadataSize(Main.CHUNK_LENGTH * 3)
        .addService(new FileIOServiceImpl())
        .intercept(new AuthorizationInterceptor()).addService(new SDFSEventImpl())
        .addService(new SdfsUserServiceImpl()).addService(new EncryptionService());
    SDFSLogger.getLog().info("Set Max Message Size to " + (Main.CHUNK_LENGTH * 2));
    if (useSSL) {
      if (useClientTLS) {
        try {
          b.sslContext(
              getSslContextBuilder(certChainFilePath, privateKeyFilePath, trustCertCollectionFilePath).build());
        } catch (Exception e) {
          SDFSLogger.getLog().error("Unable to build ssl context" + e);
          throw new IOException(e);
        }
      } else if (!Main.authJarFilePath.equals("") && !Main.authClassInfo.equals("")) {
        try {
          //Export Server Cert
              getSslContextBuilder(certChainFilePath, privateKeyFilePath, trustCertCollectionFilePath);
        } catch (Exception e) {
          SDFSLogger.getLog().error("Unable to build ssl context" + e);
          throw new IOException(e);
        }
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
    public final static Context.Key<SSLSession> SSL_SESSION_CONTEXT = Context.key("SSLSession");

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
            if (Main.sdfsCliRequireMutualTLSAuth) {
              SSLSession sslSession = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
              if (sslSession == null) {
                throw new StatusRuntimeException(Status.UNAUTHENTICATED);
              }
              Context context = Context.current().withValue(SSL_SESSION_CONTEXT, sslSession);
              return Contexts.interceptCall(context, call, headers, next);
            } else {
              throw new StatusRuntimeException(Status.UNAUTHENTICATED);
            }

          } else {
            String[] tokens = auth_token.split(" ");
            if (tokens.length == 2 && tokens[0].toLowerCase().equals("bearer")) {
              try {
                JWebToken token = new JWebToken(tokens[1]);
                if (!token.isValid()) {
                  SDFSLogger.getLog().warn("token not valid for user " + token.getSubject());
                  throw new StatusRuntimeException(Status.UNAUTHENTICATED);
                } else {
                  Context context = Context.current().withValue(USER_IDENTITY, token);
                  SDFSLogger.getLog().debug("authenticated " + token.getSubject());
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

  ExecutorService getExecutor(int asyncThreads) {
    // TODO(carl-mastrangelo): This should not be necessary. I don't know where this
    // should be
    // put. Move it somewhere else, or remove it if no longer necessary.
    // See: https://github.com/grpc/grpc-java/issues/2119
    return new ForkJoinPool(asyncThreads,
        new ForkJoinWorkerThreadFactory() {
          final AtomicInteger num = new AtomicInteger();

          @Override
          public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            ForkJoinWorkerThread thread = defaultForkJoinWorkerThreadFactory.newThread(pool);
            thread.setDaemon(true);
            thread.setName("server-worker-" + "-" + num.getAndIncrement());
            return thread;
          }
        }, new EHanderler(), true /* async */);
  }

  private static class EHanderler implements Thread.UncaughtExceptionHandler {

    @Override
    public void uncaughtException(Thread arg0, Throwable arg1) {
      SDFSLogger.getLog().warn("in thread " + arg0.toString(), arg1);

    }

  }
}