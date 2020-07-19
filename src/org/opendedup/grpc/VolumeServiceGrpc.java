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
    comments = "Source: VolumeService.proto")
public final class VolumeServiceGrpc {

  private VolumeServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.VolumeService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeInfoRequest,
      org.opendedup.grpc.VolumeInfoResponse> getGetVolumeInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetVolumeInfo",
      requestType = org.opendedup.grpc.VolumeInfoRequest.class,
      responseType = org.opendedup.grpc.VolumeInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeInfoRequest,
      org.opendedup.grpc.VolumeInfoResponse> getGetVolumeInfoMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeInfoRequest, org.opendedup.grpc.VolumeInfoResponse> getGetVolumeInfoMethod;
    if ((getGetVolumeInfoMethod = VolumeServiceGrpc.getGetVolumeInfoMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getGetVolumeInfoMethod = VolumeServiceGrpc.getGetVolumeInfoMethod) == null) {
          VolumeServiceGrpc.getGetVolumeInfoMethod = getGetVolumeInfoMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeInfoRequest, org.opendedup.grpc.VolumeInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetVolumeInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("GetVolumeInfo"))
              .build();
        }
      }
    }
    return getGetVolumeInfoMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static VolumeServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VolumeServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VolumeServiceStub>() {
        @java.lang.Override
        public VolumeServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VolumeServiceStub(channel, callOptions);
        }
      };
    return VolumeServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static VolumeServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VolumeServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VolumeServiceBlockingStub>() {
        @java.lang.Override
        public VolumeServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VolumeServiceBlockingStub(channel, callOptions);
        }
      };
    return VolumeServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static VolumeServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VolumeServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VolumeServiceFutureStub>() {
        @java.lang.Override
        public VolumeServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VolumeServiceFutureStub(channel, callOptions);
        }
      };
    return VolumeServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static abstract class VolumeServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Define a RPC operation
     * </pre>
     */
    public void getVolumeInfo(org.opendedup.grpc.VolumeInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeInfoResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetVolumeInfoMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetVolumeInfoMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeInfoRequest,
                org.opendedup.grpc.VolumeInfoResponse>(
                  this, METHODID_GET_VOLUME_INFO)))
          .build();
    }
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static final class VolumeServiceStub extends io.grpc.stub.AbstractAsyncStub<VolumeServiceStub> {
    private VolumeServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VolumeServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VolumeServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Define a RPC operation
     * </pre>
     */
    public void getVolumeInfo(org.opendedup.grpc.VolumeInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeInfoResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetVolumeInfoMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static final class VolumeServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<VolumeServiceBlockingStub> {
    private VolumeServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VolumeServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VolumeServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Define a RPC operation
     * </pre>
     */
    public org.opendedup.grpc.VolumeInfoResponse getVolumeInfo(org.opendedup.grpc.VolumeInfoRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetVolumeInfoMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static final class VolumeServiceFutureStub extends io.grpc.stub.AbstractFutureStub<VolumeServiceFutureStub> {
    private VolumeServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VolumeServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VolumeServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Define a RPC operation
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeInfoResponse> getVolumeInfo(
        org.opendedup.grpc.VolumeInfoRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetVolumeInfoMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_VOLUME_INFO = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final VolumeServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(VolumeServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_VOLUME_INFO:
          serviceImpl.getVolumeInfo((org.opendedup.grpc.VolumeInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeInfoResponse>) responseObserver);
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

  private static abstract class VolumeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    VolumeServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.opendedup.grpc.VolumeServiceOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("VolumeService");
    }
  }

  private static final class VolumeServiceFileDescriptorSupplier
      extends VolumeServiceBaseDescriptorSupplier {
    VolumeServiceFileDescriptorSupplier() {}
  }

  private static final class VolumeServiceMethodDescriptorSupplier
      extends VolumeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    VolumeServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (VolumeServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new VolumeServiceFileDescriptorSupplier())
              .addMethod(getGetVolumeInfoMethod())
              .build();
        }
      }
    }
    return result;
  }
}
