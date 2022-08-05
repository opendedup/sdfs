package org.opendedup.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.46.0)",
    comments = "Source: SDFSEvent.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class SDFSEventServiceGrpc {

  private SDFSEventServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.SDFSEventService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest,
      org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> getGetEventMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetEvent",
      requestType = org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest.class,
      responseType = org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest,
      org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> getGetEventMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest, org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> getGetEventMethod;
    if ((getGetEventMethod = SDFSEventServiceGrpc.getGetEventMethod) == null) {
      synchronized (SDFSEventServiceGrpc.class) {
        if ((getGetEventMethod = SDFSEventServiceGrpc.getGetEventMethod) == null) {
          SDFSEventServiceGrpc.getGetEventMethod = getGetEventMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest, org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetEvent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SDFSEventServiceMethodDescriptorSupplier("GetEvent"))
              .build();
        }
      }
    }
    return getGetEventMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest,
      org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse> getListEventsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListEvents",
      requestType = org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest.class,
      responseType = org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest,
      org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse> getListEventsMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest, org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse> getListEventsMethod;
    if ((getListEventsMethod = SDFSEventServiceGrpc.getListEventsMethod) == null) {
      synchronized (SDFSEventServiceGrpc.class) {
        if ((getListEventsMethod = SDFSEventServiceGrpc.getListEventsMethod) == null) {
          SDFSEventServiceGrpc.getListEventsMethod = getListEventsMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest, org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListEvents"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SDFSEventServiceMethodDescriptorSupplier("ListEvents"))
              .build();
        }
      }
    }
    return getListEventsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest,
      org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> getSubscribeEventMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SubscribeEvent",
      requestType = org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest.class,
      responseType = org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest,
      org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> getSubscribeEventMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest, org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> getSubscribeEventMethod;
    if ((getSubscribeEventMethod = SDFSEventServiceGrpc.getSubscribeEventMethod) == null) {
      synchronized (SDFSEventServiceGrpc.class) {
        if ((getSubscribeEventMethod = SDFSEventServiceGrpc.getSubscribeEventMethod) == null) {
          SDFSEventServiceGrpc.getSubscribeEventMethod = getSubscribeEventMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest, org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SubscribeEvent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SDFSEventServiceMethodDescriptorSupplier("SubscribeEvent"))
              .build();
        }
      }
    }
    return getSubscribeEventMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SDFSEventServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SDFSEventServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SDFSEventServiceStub>() {
        @java.lang.Override
        public SDFSEventServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SDFSEventServiceStub(channel, callOptions);
        }
      };
    return SDFSEventServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SDFSEventServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SDFSEventServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SDFSEventServiceBlockingStub>() {
        @java.lang.Override
        public SDFSEventServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SDFSEventServiceBlockingStub(channel, callOptions);
        }
      };
    return SDFSEventServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SDFSEventServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SDFSEventServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SDFSEventServiceFutureStub>() {
        @java.lang.Override
        public SDFSEventServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SDFSEventServiceFutureStub(channel, callOptions);
        }
      };
    return SDFSEventServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class SDFSEventServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getEvent(org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetEventMethod(), responseObserver);
    }

    /**
     */
    public void listEvents(org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListEventsMethod(), responseObserver);
    }

    /**
     */
    public void subscribeEvent(org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubscribeEventMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetEventMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest,
                org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse>(
                  this, METHODID_GET_EVENT)))
          .addMethod(
            getListEventsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest,
                org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse>(
                  this, METHODID_LIST_EVENTS)))
          .addMethod(
            getSubscribeEventMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest,
                org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse>(
                  this, METHODID_SUBSCRIBE_EVENT)))
          .build();
    }
  }

  /**
   */
  public static final class SDFSEventServiceStub extends io.grpc.stub.AbstractAsyncStub<SDFSEventServiceStub> {
    private SDFSEventServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SDFSEventServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SDFSEventServiceStub(channel, callOptions);
    }

    /**
     */
    public void getEvent(org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetEventMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listEvents(org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListEventsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void subscribeEvent(org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getSubscribeEventMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class SDFSEventServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<SDFSEventServiceBlockingStub> {
    private SDFSEventServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SDFSEventServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SDFSEventServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse getEvent(org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetEventMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse listEvents(org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListEventsMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> subscribeEvent(
        org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getSubscribeEventMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class SDFSEventServiceFutureStub extends io.grpc.stub.AbstractFutureStub<SDFSEventServiceFutureStub> {
    private SDFSEventServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SDFSEventServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SDFSEventServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse> getEvent(
        org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetEventMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse> listEvents(
        org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListEventsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_EVENT = 0;
  private static final int METHODID_LIST_EVENTS = 1;
  private static final int METHODID_SUBSCRIBE_EVENT = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final SDFSEventServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(SDFSEventServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_EVENT:
          serviceImpl.getEvent((org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse>) responseObserver);
          break;
        case METHODID_LIST_EVENTS:
          serviceImpl.listEvents((org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse>) responseObserver);
          break;
        case METHODID_SUBSCRIBE_EVENT:
          serviceImpl.subscribeEvent((org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse>) responseObserver);
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

  private static abstract class SDFSEventServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SDFSEventServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.opendedup.grpc.SDFSEventOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("SDFSEventService");
    }
  }

  private static final class SDFSEventServiceFileDescriptorSupplier
      extends SDFSEventServiceBaseDescriptorSupplier {
    SDFSEventServiceFileDescriptorSupplier() {}
  }

  private static final class SDFSEventServiceMethodDescriptorSupplier
      extends SDFSEventServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    SDFSEventServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (SDFSEventServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SDFSEventServiceFileDescriptorSupplier())
              .addMethod(getGetEventMethod())
              .addMethod(getListEventsMethod())
              .addMethod(getSubscribeEventMethod())
              .build();
        }
      }
    }
    return result;
  }
}
