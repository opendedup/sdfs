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
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.AuthenticationRequest,
      org.opendedup.grpc.AuthenticationResponse> getAuthenticateUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AuthenticateUser",
      requestType = org.opendedup.grpc.AuthenticationRequest.class,
      responseType = org.opendedup.grpc.AuthenticationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.AuthenticationRequest,
      org.opendedup.grpc.AuthenticationResponse> getAuthenticateUserMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.AuthenticationRequest, org.opendedup.grpc.AuthenticationResponse> getAuthenticateUserMethod;
    if ((getAuthenticateUserMethod = VolumeServiceGrpc.getAuthenticateUserMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getAuthenticateUserMethod = VolumeServiceGrpc.getAuthenticateUserMethod) == null) {
          VolumeServiceGrpc.getAuthenticateUserMethod = getAuthenticateUserMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.AuthenticationRequest, org.opendedup.grpc.AuthenticationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AuthenticateUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.AuthenticationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.AuthenticationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("AuthenticateUser"))
              .build();
        }
      }
    }
    return getAuthenticateUserMethod;
  }

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

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.ShutdownRequest,
      org.opendedup.grpc.ShutdownResponse> getShutdownVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ShutdownVolume",
      requestType = org.opendedup.grpc.ShutdownRequest.class,
      responseType = org.opendedup.grpc.ShutdownResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.ShutdownRequest,
      org.opendedup.grpc.ShutdownResponse> getShutdownVolumeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.ShutdownRequest, org.opendedup.grpc.ShutdownResponse> getShutdownVolumeMethod;
    if ((getShutdownVolumeMethod = VolumeServiceGrpc.getShutdownVolumeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getShutdownVolumeMethod = VolumeServiceGrpc.getShutdownVolumeMethod) == null) {
          VolumeServiceGrpc.getShutdownVolumeMethod = getShutdownVolumeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.ShutdownRequest, org.opendedup.grpc.ShutdownResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShutdownVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.ShutdownRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.ShutdownResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("ShutdownVolume"))
              .build();
        }
      }
    }
    return getShutdownVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.CleanStoreRequest,
      org.opendedup.grpc.CleanStoreResponse> getCleanStoreMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CleanStore",
      requestType = org.opendedup.grpc.CleanStoreRequest.class,
      responseType = org.opendedup.grpc.CleanStoreResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.CleanStoreRequest,
      org.opendedup.grpc.CleanStoreResponse> getCleanStoreMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.CleanStoreRequest, org.opendedup.grpc.CleanStoreResponse> getCleanStoreMethod;
    if ((getCleanStoreMethod = VolumeServiceGrpc.getCleanStoreMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getCleanStoreMethod = VolumeServiceGrpc.getCleanStoreMethod) == null) {
          VolumeServiceGrpc.getCleanStoreMethod = getCleanStoreMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.CleanStoreRequest, org.opendedup.grpc.CleanStoreResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CleanStore"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.CleanStoreRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.CleanStoreResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("CleanStore"))
              .build();
        }
      }
    }
    return getCleanStoreMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.DeleteCloudVolumeRequest,
      org.opendedup.grpc.DeleteCloudVolumeResponse> getDeleteCloudVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteCloudVolume",
      requestType = org.opendedup.grpc.DeleteCloudVolumeRequest.class,
      responseType = org.opendedup.grpc.DeleteCloudVolumeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.DeleteCloudVolumeRequest,
      org.opendedup.grpc.DeleteCloudVolumeResponse> getDeleteCloudVolumeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.DeleteCloudVolumeRequest, org.opendedup.grpc.DeleteCloudVolumeResponse> getDeleteCloudVolumeMethod;
    if ((getDeleteCloudVolumeMethod = VolumeServiceGrpc.getDeleteCloudVolumeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getDeleteCloudVolumeMethod = VolumeServiceGrpc.getDeleteCloudVolumeMethod) == null) {
          VolumeServiceGrpc.getDeleteCloudVolumeMethod = getDeleteCloudVolumeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.DeleteCloudVolumeRequest, org.opendedup.grpc.DeleteCloudVolumeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteCloudVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.DeleteCloudVolumeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.DeleteCloudVolumeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("DeleteCloudVolume"))
              .build();
        }
      }
    }
    return getDeleteCloudVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.DSERequest,
      org.opendedup.grpc.DSEResponse> getDSEInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DSEInfo",
      requestType = org.opendedup.grpc.DSERequest.class,
      responseType = org.opendedup.grpc.DSEResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.DSERequest,
      org.opendedup.grpc.DSEResponse> getDSEInfoMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.DSERequest, org.opendedup.grpc.DSEResponse> getDSEInfoMethod;
    if ((getDSEInfoMethod = VolumeServiceGrpc.getDSEInfoMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getDSEInfoMethod = VolumeServiceGrpc.getDSEInfoMethod) == null) {
          VolumeServiceGrpc.getDSEInfoMethod = getDSEInfoMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.DSERequest, org.opendedup.grpc.DSEResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DSEInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.DSERequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.DSEResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("DSEInfo"))
              .build();
        }
      }
    }
    return getDSEInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SystemInfoRequest,
      org.opendedup.grpc.SystemInfoResponse> getSystemInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SystemInfo",
      requestType = org.opendedup.grpc.SystemInfoRequest.class,
      responseType = org.opendedup.grpc.SystemInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SystemInfoRequest,
      org.opendedup.grpc.SystemInfoResponse> getSystemInfoMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SystemInfoRequest, org.opendedup.grpc.SystemInfoResponse> getSystemInfoMethod;
    if ((getSystemInfoMethod = VolumeServiceGrpc.getSystemInfoMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSystemInfoMethod = VolumeServiceGrpc.getSystemInfoMethod) == null) {
          VolumeServiceGrpc.getSystemInfoMethod = getSystemInfoMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SystemInfoRequest, org.opendedup.grpc.SystemInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SystemInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SystemInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SystemInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SystemInfo"))
              .build();
        }
      }
    }
    return getSystemInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SetLogicalVolumeCapacityRequest,
      org.opendedup.grpc.SetLogicalVolumeCapacityResponse> getSetLogicalVolumeCapacityMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetLogicalVolumeCapacity",
      requestType = org.opendedup.grpc.SetLogicalVolumeCapacityRequest.class,
      responseType = org.opendedup.grpc.SetLogicalVolumeCapacityResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SetLogicalVolumeCapacityRequest,
      org.opendedup.grpc.SetLogicalVolumeCapacityResponse> getSetLogicalVolumeCapacityMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SetLogicalVolumeCapacityRequest, org.opendedup.grpc.SetLogicalVolumeCapacityResponse> getSetLogicalVolumeCapacityMethod;
    if ((getSetLogicalVolumeCapacityMethod = VolumeServiceGrpc.getSetLogicalVolumeCapacityMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetLogicalVolumeCapacityMethod = VolumeServiceGrpc.getSetLogicalVolumeCapacityMethod) == null) {
          VolumeServiceGrpc.getSetLogicalVolumeCapacityMethod = getSetLogicalVolumeCapacityMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SetLogicalVolumeCapacityRequest, org.opendedup.grpc.SetLogicalVolumeCapacityResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetLogicalVolumeCapacity"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetLogicalVolumeCapacityRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetLogicalVolumeCapacityResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetLogicalVolumeCapacity"))
              .build();
        }
      }
    }
    return getSetLogicalVolumeCapacityMethod;
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
     */
    public void authenticateUser(org.opendedup.grpc.AuthenticationRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.AuthenticationResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getAuthenticateUserMethod(), responseObserver);
    }

    /**
     */
    public void getVolumeInfo(org.opendedup.grpc.VolumeInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeInfoResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetVolumeInfoMethod(), responseObserver);
    }

    /**
     */
    public void shutdownVolume(org.opendedup.grpc.ShutdownRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.ShutdownResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getShutdownVolumeMethod(), responseObserver);
    }

    /**
     */
    public void cleanStore(org.opendedup.grpc.CleanStoreRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.CleanStoreResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCleanStoreMethod(), responseObserver);
    }

    /**
     */
    public void deleteCloudVolume(org.opendedup.grpc.DeleteCloudVolumeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.DeleteCloudVolumeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDeleteCloudVolumeMethod(), responseObserver);
    }

    /**
     */
    public void dSEInfo(org.opendedup.grpc.DSERequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.DSEResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getDSEInfoMethod(), responseObserver);
    }

    /**
     */
    public void systemInfo(org.opendedup.grpc.SystemInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SystemInfoResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSystemInfoMethod(), responseObserver);
    }

    /**
     */
    public void setLogicalVolumeCapacity(org.opendedup.grpc.SetLogicalVolumeCapacityRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetLogicalVolumeCapacityResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetLogicalVolumeCapacityMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getAuthenticateUserMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.AuthenticationRequest,
                org.opendedup.grpc.AuthenticationResponse>(
                  this, METHODID_AUTHENTICATE_USER)))
          .addMethod(
            getGetVolumeInfoMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeInfoRequest,
                org.opendedup.grpc.VolumeInfoResponse>(
                  this, METHODID_GET_VOLUME_INFO)))
          .addMethod(
            getShutdownVolumeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.ShutdownRequest,
                org.opendedup.grpc.ShutdownResponse>(
                  this, METHODID_SHUTDOWN_VOLUME)))
          .addMethod(
            getCleanStoreMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.CleanStoreRequest,
                org.opendedup.grpc.CleanStoreResponse>(
                  this, METHODID_CLEAN_STORE)))
          .addMethod(
            getDeleteCloudVolumeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.DeleteCloudVolumeRequest,
                org.opendedup.grpc.DeleteCloudVolumeResponse>(
                  this, METHODID_DELETE_CLOUD_VOLUME)))
          .addMethod(
            getDSEInfoMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.DSERequest,
                org.opendedup.grpc.DSEResponse>(
                  this, METHODID_DSEINFO)))
          .addMethod(
            getSystemInfoMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SystemInfoRequest,
                org.opendedup.grpc.SystemInfoResponse>(
                  this, METHODID_SYSTEM_INFO)))
          .addMethod(
            getSetLogicalVolumeCapacityMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SetLogicalVolumeCapacityRequest,
                org.opendedup.grpc.SetLogicalVolumeCapacityResponse>(
                  this, METHODID_SET_LOGICAL_VOLUME_CAPACITY)))
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
     */
    public void authenticateUser(org.opendedup.grpc.AuthenticationRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.AuthenticationResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAuthenticateUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getVolumeInfo(org.opendedup.grpc.VolumeInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeInfoResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetVolumeInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void shutdownVolume(org.opendedup.grpc.ShutdownRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.ShutdownResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getShutdownVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cleanStore(org.opendedup.grpc.CleanStoreRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.CleanStoreResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCleanStoreMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteCloudVolume(org.opendedup.grpc.DeleteCloudVolumeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.DeleteCloudVolumeResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDeleteCloudVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dSEInfo(org.opendedup.grpc.DSERequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.DSEResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDSEInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void systemInfo(org.opendedup.grpc.SystemInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SystemInfoResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSystemInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setLogicalVolumeCapacity(org.opendedup.grpc.SetLogicalVolumeCapacityRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetLogicalVolumeCapacityResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetLogicalVolumeCapacityMethod(), getCallOptions()), request, responseObserver);
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
     */
    public org.opendedup.grpc.AuthenticationResponse authenticateUser(org.opendedup.grpc.AuthenticationRequest request) {
      return blockingUnaryCall(
          getChannel(), getAuthenticateUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeInfoResponse getVolumeInfo(org.opendedup.grpc.VolumeInfoRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetVolumeInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.ShutdownResponse shutdownVolume(org.opendedup.grpc.ShutdownRequest request) {
      return blockingUnaryCall(
          getChannel(), getShutdownVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.CleanStoreResponse cleanStore(org.opendedup.grpc.CleanStoreRequest request) {
      return blockingUnaryCall(
          getChannel(), getCleanStoreMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.DeleteCloudVolumeResponse deleteCloudVolume(org.opendedup.grpc.DeleteCloudVolumeRequest request) {
      return blockingUnaryCall(
          getChannel(), getDeleteCloudVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.DSEResponse dSEInfo(org.opendedup.grpc.DSERequest request) {
      return blockingUnaryCall(
          getChannel(), getDSEInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SystemInfoResponse systemInfo(org.opendedup.grpc.SystemInfoRequest request) {
      return blockingUnaryCall(
          getChannel(), getSystemInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SetLogicalVolumeCapacityResponse setLogicalVolumeCapacity(org.opendedup.grpc.SetLogicalVolumeCapacityRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetLogicalVolumeCapacityMethod(), getCallOptions(), request);
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
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.AuthenticationResponse> authenticateUser(
        org.opendedup.grpc.AuthenticationRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getAuthenticateUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeInfoResponse> getVolumeInfo(
        org.opendedup.grpc.VolumeInfoRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetVolumeInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.ShutdownResponse> shutdownVolume(
        org.opendedup.grpc.ShutdownRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getShutdownVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.CleanStoreResponse> cleanStore(
        org.opendedup.grpc.CleanStoreRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCleanStoreMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.DeleteCloudVolumeResponse> deleteCloudVolume(
        org.opendedup.grpc.DeleteCloudVolumeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDeleteCloudVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.DSEResponse> dSEInfo(
        org.opendedup.grpc.DSERequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDSEInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SystemInfoResponse> systemInfo(
        org.opendedup.grpc.SystemInfoRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSystemInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SetLogicalVolumeCapacityResponse> setLogicalVolumeCapacity(
        org.opendedup.grpc.SetLogicalVolumeCapacityRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSetLogicalVolumeCapacityMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_AUTHENTICATE_USER = 0;
  private static final int METHODID_GET_VOLUME_INFO = 1;
  private static final int METHODID_SHUTDOWN_VOLUME = 2;
  private static final int METHODID_CLEAN_STORE = 3;
  private static final int METHODID_DELETE_CLOUD_VOLUME = 4;
  private static final int METHODID_DSEINFO = 5;
  private static final int METHODID_SYSTEM_INFO = 6;
  private static final int METHODID_SET_LOGICAL_VOLUME_CAPACITY = 7;

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
        case METHODID_AUTHENTICATE_USER:
          serviceImpl.authenticateUser((org.opendedup.grpc.AuthenticationRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.AuthenticationResponse>) responseObserver);
          break;
        case METHODID_GET_VOLUME_INFO:
          serviceImpl.getVolumeInfo((org.opendedup.grpc.VolumeInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeInfoResponse>) responseObserver);
          break;
        case METHODID_SHUTDOWN_VOLUME:
          serviceImpl.shutdownVolume((org.opendedup.grpc.ShutdownRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.ShutdownResponse>) responseObserver);
          break;
        case METHODID_CLEAN_STORE:
          serviceImpl.cleanStore((org.opendedup.grpc.CleanStoreRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.CleanStoreResponse>) responseObserver);
          break;
        case METHODID_DELETE_CLOUD_VOLUME:
          serviceImpl.deleteCloudVolume((org.opendedup.grpc.DeleteCloudVolumeRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.DeleteCloudVolumeResponse>) responseObserver);
          break;
        case METHODID_DSEINFO:
          serviceImpl.dSEInfo((org.opendedup.grpc.DSERequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.DSEResponse>) responseObserver);
          break;
        case METHODID_SYSTEM_INFO:
          serviceImpl.systemInfo((org.opendedup.grpc.SystemInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SystemInfoResponse>) responseObserver);
          break;
        case METHODID_SET_LOGICAL_VOLUME_CAPACITY:
          serviceImpl.setLogicalVolumeCapacity((org.opendedup.grpc.SetLogicalVolumeCapacityRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SetLogicalVolumeCapacityResponse>) responseObserver);
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
              .addMethod(getAuthenticateUserMethod())
              .addMethod(getGetVolumeInfoMethod())
              .addMethod(getShutdownVolumeMethod())
              .addMethod(getCleanStoreMethod())
              .addMethod(getDeleteCloudVolumeMethod())
              .addMethod(getDSEInfoMethod())
              .addMethod(getSystemInfoMethod())
              .addMethod(getSetLogicalVolumeCapacityMethod())
              .build();
        }
      }
    }
    return result;
  }
}
