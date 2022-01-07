package org.opendedup.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 * Defining a Service, a Service can have multiple RPC operations
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.30.2)",
    comments = "Source: EncryptionService.proto")
public final class EncryptionServiceGrpc {

  private EncryptionServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.EncryptionService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest,
      org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse> getValidateCertificateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ValidateCertificate",
      requestType = org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest.class,
      responseType = org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest,
      org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse> getValidateCertificateMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest, org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse> getValidateCertificateMethod;
    if ((getValidateCertificateMethod = EncryptionServiceGrpc.getValidateCertificateMethod) == null) {
      synchronized (EncryptionServiceGrpc.class) {
        if ((getValidateCertificateMethod = EncryptionServiceGrpc.getValidateCertificateMethod) == null) {
          EncryptionServiceGrpc.getValidateCertificateMethod = getValidateCertificateMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest, org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ValidateCertificate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EncryptionServiceMethodDescriptorSupplier("ValidateCertificate"))
              .build();
        }
      }
    }
    return getValidateCertificateMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static EncryptionServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EncryptionServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EncryptionServiceStub>() {
        @java.lang.Override
        public EncryptionServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EncryptionServiceStub(channel, callOptions);
        }
      };
    return EncryptionServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EncryptionServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EncryptionServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EncryptionServiceBlockingStub>() {
        @java.lang.Override
        public EncryptionServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EncryptionServiceBlockingStub(channel, callOptions);
        }
      };
    return EncryptionServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static EncryptionServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EncryptionServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EncryptionServiceFutureStub>() {
        @java.lang.Override
        public EncryptionServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EncryptionServiceFutureStub(channel, callOptions);
        }
      };
    return EncryptionServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static abstract class EncryptionServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void validateCertificate(org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getValidateCertificateMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getValidateCertificateMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest,
                org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse>(
                  this, METHODID_VALIDATE_CERTIFICATE)))
          .build();
    }
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static final class EncryptionServiceStub extends io.grpc.stub.AbstractAsyncStub<EncryptionServiceStub> {
    private EncryptionServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EncryptionServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EncryptionServiceStub(channel, callOptions);
    }

    /**
     */
    public void validateCertificate(org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getValidateCertificateMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static final class EncryptionServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<EncryptionServiceBlockingStub> {
    private EncryptionServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EncryptionServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EncryptionServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse validateCertificate(org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest request) {
      return blockingUnaryCall(
          getChannel(), getValidateCertificateMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static final class EncryptionServiceFutureStub extends io.grpc.stub.AbstractFutureStub<EncryptionServiceFutureStub> {
    private EncryptionServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EncryptionServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EncryptionServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse> validateCertificate(
        org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getValidateCertificateMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_VALIDATE_CERTIFICATE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final EncryptionServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(EncryptionServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_VALIDATE_CERTIFICATE:
          serviceImpl.validateCertificate((org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class EncryptionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EncryptionServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.opendedup.grpc.EncryptionServiceOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EncryptionService");
    }
  }

  private static final class EncryptionServiceFileDescriptorSupplier
      extends EncryptionServiceBaseDescriptorSupplier {
    EncryptionServiceFileDescriptorSupplier() {}
  }

  private static final class EncryptionServiceMethodDescriptorSupplier
      extends EncryptionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    EncryptionServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (EncryptionServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new EncryptionServiceFileDescriptorSupplier())
              .addMethod(getValidateCertificateMethod())
              .build();
        }
      }
    }
    return result;
  }
}
