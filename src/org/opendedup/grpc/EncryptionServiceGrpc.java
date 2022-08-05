package org.opendedup.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.46.0)",
    comments = "Source: EncryptionService.proto")
@io.grpc.stub.annotations.GrpcGenerated
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

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest,
      org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse> getExportServerCertificateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExportServerCertificate",
      requestType = org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest.class,
      responseType = org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest,
      org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse> getExportServerCertificateMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest, org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse> getExportServerCertificateMethod;
    if ((getExportServerCertificateMethod = EncryptionServiceGrpc.getExportServerCertificateMethod) == null) {
      synchronized (EncryptionServiceGrpc.class) {
        if ((getExportServerCertificateMethod = EncryptionServiceGrpc.getExportServerCertificateMethod) == null) {
          EncryptionServiceGrpc.getExportServerCertificateMethod = getExportServerCertificateMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest, org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExportServerCertificate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EncryptionServiceMethodDescriptorSupplier("ExportServerCertificate"))
              .build();
        }
      }
    }
    return getExportServerCertificateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest,
      org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse> getDeleteExportedCertMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteExportedCert",
      requestType = org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest.class,
      responseType = org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest,
      org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse> getDeleteExportedCertMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest, org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse> getDeleteExportedCertMethod;
    if ((getDeleteExportedCertMethod = EncryptionServiceGrpc.getDeleteExportedCertMethod) == null) {
      synchronized (EncryptionServiceGrpc.class) {
        if ((getDeleteExportedCertMethod = EncryptionServiceGrpc.getDeleteExportedCertMethod) == null) {
          EncryptionServiceGrpc.getDeleteExportedCertMethod = getDeleteExportedCertMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest, org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteExportedCert"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EncryptionServiceMethodDescriptorSupplier("DeleteExportedCert"))
              .build();
        }
      }
    }
    return getDeleteExportedCertMethod;
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
   */
  public static abstract class EncryptionServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void validateCertificate(org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getValidateCertificateMethod(), responseObserver);
    }

    /**
     */
    public void exportServerCertificate(org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExportServerCertificateMethod(), responseObserver);
    }

    /**
     */
    public void deleteExportedCert(org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteExportedCertMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getValidateCertificateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest,
                org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse>(
                  this, METHODID_VALIDATE_CERTIFICATE)))
          .addMethod(
            getExportServerCertificateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest,
                org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse>(
                  this, METHODID_EXPORT_SERVER_CERTIFICATE)))
          .addMethod(
            getDeleteExportedCertMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest,
                org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse>(
                  this, METHODID_DELETE_EXPORTED_CERT)))
          .build();
    }
  }

  /**
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
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getValidateCertificateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void exportServerCertificate(org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExportServerCertificateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteExportedCert(org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteExportedCertMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
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
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getValidateCertificateMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse exportServerCertificate(org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExportServerCertificateMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse deleteExportedCert(org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteExportedCertMethod(), getCallOptions(), request);
    }
  }

  /**
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
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getValidateCertificateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse> exportServerCertificate(
        org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExportServerCertificateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse> deleteExportedCert(
        org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteExportedCertMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_VALIDATE_CERTIFICATE = 0;
  private static final int METHODID_EXPORT_SERVER_CERTIFICATE = 1;
  private static final int METHODID_DELETE_EXPORTED_CERT = 2;

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
        case METHODID_EXPORT_SERVER_CERTIFICATE:
          serviceImpl.exportServerCertificate((org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse>) responseObserver);
          break;
        case METHODID_DELETE_EXPORTED_CERT:
          serviceImpl.deleteExportedCert((org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse>) responseObserver);
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
              .addMethod(getExportServerCertificateMethod())
              .addMethod(getDeleteExportedCertMethod())
              .build();
        }
      }
    }
    return result;
  }
}
