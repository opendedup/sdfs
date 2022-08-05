package org.opendedup.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.46.0)",
    comments = "Source: PortRedirector.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class PortRedirectorServiceGrpc {

  private PortRedirectorServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.PortRedirectorService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest,
      org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse> getGetProxyVolumesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetProxyVolumes",
      requestType = org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest.class,
      responseType = org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest,
      org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse> getGetProxyVolumesMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest, org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse> getGetProxyVolumesMethod;
    if ((getGetProxyVolumesMethod = PortRedirectorServiceGrpc.getGetProxyVolumesMethod) == null) {
      synchronized (PortRedirectorServiceGrpc.class) {
        if ((getGetProxyVolumesMethod = PortRedirectorServiceGrpc.getGetProxyVolumesMethod) == null) {
          PortRedirectorServiceGrpc.getGetProxyVolumesMethod = getGetProxyVolumesMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest, org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetProxyVolumes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PortRedirectorServiceMethodDescriptorSupplier("GetProxyVolumes"))
              .build();
        }
      }
    }
    return getGetProxyVolumesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.PortRedirector.ReloadConfigRequest,
      org.opendedup.grpc.PortRedirector.ReloadConfigResponse> getReloadConfigMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReloadConfig",
      requestType = org.opendedup.grpc.PortRedirector.ReloadConfigRequest.class,
      responseType = org.opendedup.grpc.PortRedirector.ReloadConfigResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.PortRedirector.ReloadConfigRequest,
      org.opendedup.grpc.PortRedirector.ReloadConfigResponse> getReloadConfigMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.PortRedirector.ReloadConfigRequest, org.opendedup.grpc.PortRedirector.ReloadConfigResponse> getReloadConfigMethod;
    if ((getReloadConfigMethod = PortRedirectorServiceGrpc.getReloadConfigMethod) == null) {
      synchronized (PortRedirectorServiceGrpc.class) {
        if ((getReloadConfigMethod = PortRedirectorServiceGrpc.getReloadConfigMethod) == null) {
          PortRedirectorServiceGrpc.getReloadConfigMethod = getReloadConfigMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.PortRedirector.ReloadConfigRequest, org.opendedup.grpc.PortRedirector.ReloadConfigResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReloadConfig"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.PortRedirector.ReloadConfigRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.PortRedirector.ReloadConfigResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PortRedirectorServiceMethodDescriptorSupplier("ReloadConfig"))
              .build();
        }
      }
    }
    return getReloadConfigMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PortRedirectorServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PortRedirectorServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PortRedirectorServiceStub>() {
        @java.lang.Override
        public PortRedirectorServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PortRedirectorServiceStub(channel, callOptions);
        }
      };
    return PortRedirectorServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PortRedirectorServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PortRedirectorServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PortRedirectorServiceBlockingStub>() {
        @java.lang.Override
        public PortRedirectorServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PortRedirectorServiceBlockingStub(channel, callOptions);
        }
      };
    return PortRedirectorServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PortRedirectorServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PortRedirectorServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PortRedirectorServiceFutureStub>() {
        @java.lang.Override
        public PortRedirectorServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PortRedirectorServiceFutureStub(channel, callOptions);
        }
      };
    return PortRedirectorServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class PortRedirectorServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getProxyVolumes(org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetProxyVolumesMethod(), responseObserver);
    }

    /**
     */
    public void reloadConfig(org.opendedup.grpc.PortRedirector.ReloadConfigRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.PortRedirector.ReloadConfigResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReloadConfigMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetProxyVolumesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest,
                org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse>(
                  this, METHODID_GET_PROXY_VOLUMES)))
          .addMethod(
            getReloadConfigMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.PortRedirector.ReloadConfigRequest,
                org.opendedup.grpc.PortRedirector.ReloadConfigResponse>(
                  this, METHODID_RELOAD_CONFIG)))
          .build();
    }
  }

  /**
   */
  public static final class PortRedirectorServiceStub extends io.grpc.stub.AbstractAsyncStub<PortRedirectorServiceStub> {
    private PortRedirectorServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PortRedirectorServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PortRedirectorServiceStub(channel, callOptions);
    }

    /**
     */
    public void getProxyVolumes(org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetProxyVolumesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reloadConfig(org.opendedup.grpc.PortRedirector.ReloadConfigRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.PortRedirector.ReloadConfigResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReloadConfigMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class PortRedirectorServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<PortRedirectorServiceBlockingStub> {
    private PortRedirectorServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PortRedirectorServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PortRedirectorServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse getProxyVolumes(org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetProxyVolumesMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.PortRedirector.ReloadConfigResponse reloadConfig(org.opendedup.grpc.PortRedirector.ReloadConfigRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReloadConfigMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class PortRedirectorServiceFutureStub extends io.grpc.stub.AbstractFutureStub<PortRedirectorServiceFutureStub> {
    private PortRedirectorServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PortRedirectorServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PortRedirectorServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse> getProxyVolumes(
        org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetProxyVolumesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.PortRedirector.ReloadConfigResponse> reloadConfig(
        org.opendedup.grpc.PortRedirector.ReloadConfigRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReloadConfigMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_PROXY_VOLUMES = 0;
  private static final int METHODID_RELOAD_CONFIG = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PortRedirectorServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(PortRedirectorServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_PROXY_VOLUMES:
          serviceImpl.getProxyVolumes((org.opendedup.grpc.PortRedirector.ProxyVolumeInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.PortRedirector.ProxyVolumeInfoResponse>) responseObserver);
          break;
        case METHODID_RELOAD_CONFIG:
          serviceImpl.reloadConfig((org.opendedup.grpc.PortRedirector.ReloadConfigRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.PortRedirector.ReloadConfigResponse>) responseObserver);
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

  private static abstract class PortRedirectorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PortRedirectorServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.opendedup.grpc.PortRedirector.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PortRedirectorService");
    }
  }

  private static final class PortRedirectorServiceFileDescriptorSupplier
      extends PortRedirectorServiceBaseDescriptorSupplier {
    PortRedirectorServiceFileDescriptorSupplier() {}
  }

  private static final class PortRedirectorServiceMethodDescriptorSupplier
      extends PortRedirectorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    PortRedirectorServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (PortRedirectorServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PortRedirectorServiceFileDescriptorSupplier())
              .addMethod(getGetProxyVolumesMethod())
              .addMethod(getReloadConfigMethod())
              .build();
        }
      }
    }
    return result;
  }
}
