package org.opendedup.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.46.0)",
    comments = "Source: SDFSCli.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class SdfsUserServiceGrpc {

  private SdfsUserServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.SdfsUserService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.AddUserRequest,
      org.opendedup.grpc.SDFSCli.AddUserResponse> getAddUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AddUser",
      requestType = org.opendedup.grpc.SDFSCli.AddUserRequest.class,
      responseType = org.opendedup.grpc.SDFSCli.AddUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.AddUserRequest,
      org.opendedup.grpc.SDFSCli.AddUserResponse> getAddUserMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.AddUserRequest, org.opendedup.grpc.SDFSCli.AddUserResponse> getAddUserMethod;
    if ((getAddUserMethod = SdfsUserServiceGrpc.getAddUserMethod) == null) {
      synchronized (SdfsUserServiceGrpc.class) {
        if ((getAddUserMethod = SdfsUserServiceGrpc.getAddUserMethod) == null) {
          SdfsUserServiceGrpc.getAddUserMethod = getAddUserMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SDFSCli.AddUserRequest, org.opendedup.grpc.SDFSCli.AddUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AddUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.AddUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.AddUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SdfsUserServiceMethodDescriptorSupplier("AddUser"))
              .build();
        }
      }
    }
    return getAddUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.DeleteUserRequest,
      org.opendedup.grpc.SDFSCli.DeleteUserResponse> getDeleteUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteUser",
      requestType = org.opendedup.grpc.SDFSCli.DeleteUserRequest.class,
      responseType = org.opendedup.grpc.SDFSCli.DeleteUserResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.DeleteUserRequest,
      org.opendedup.grpc.SDFSCli.DeleteUserResponse> getDeleteUserMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.DeleteUserRequest, org.opendedup.grpc.SDFSCli.DeleteUserResponse> getDeleteUserMethod;
    if ((getDeleteUserMethod = SdfsUserServiceGrpc.getDeleteUserMethod) == null) {
      synchronized (SdfsUserServiceGrpc.class) {
        if ((getDeleteUserMethod = SdfsUserServiceGrpc.getDeleteUserMethod) == null) {
          SdfsUserServiceGrpc.getDeleteUserMethod = getDeleteUserMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SDFSCli.DeleteUserRequest, org.opendedup.grpc.SDFSCli.DeleteUserResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.DeleteUserRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.DeleteUserResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SdfsUserServiceMethodDescriptorSupplier("DeleteUser"))
              .build();
        }
      }
    }
    return getDeleteUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.SetPermissionsRequest,
      org.opendedup.grpc.SDFSCli.SetPermissionsResponse> getSetSdfsPermissionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetSdfsPermissions",
      requestType = org.opendedup.grpc.SDFSCli.SetPermissionsRequest.class,
      responseType = org.opendedup.grpc.SDFSCli.SetPermissionsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.SetPermissionsRequest,
      org.opendedup.grpc.SDFSCli.SetPermissionsResponse> getSetSdfsPermissionsMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.SetPermissionsRequest, org.opendedup.grpc.SDFSCli.SetPermissionsResponse> getSetSdfsPermissionsMethod;
    if ((getSetSdfsPermissionsMethod = SdfsUserServiceGrpc.getSetSdfsPermissionsMethod) == null) {
      synchronized (SdfsUserServiceGrpc.class) {
        if ((getSetSdfsPermissionsMethod = SdfsUserServiceGrpc.getSetSdfsPermissionsMethod) == null) {
          SdfsUserServiceGrpc.getSetSdfsPermissionsMethod = getSetSdfsPermissionsMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SDFSCli.SetPermissionsRequest, org.opendedup.grpc.SDFSCli.SetPermissionsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetSdfsPermissions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.SetPermissionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.SetPermissionsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SdfsUserServiceMethodDescriptorSupplier("SetSdfsPermissions"))
              .build();
        }
      }
    }
    return getSetSdfsPermissionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.SetUserPasswordRequest,
      org.opendedup.grpc.SDFSCli.SetUserPasswordResponse> getSetSdfsPasswordMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetSdfsPassword",
      requestType = org.opendedup.grpc.SDFSCli.SetUserPasswordRequest.class,
      responseType = org.opendedup.grpc.SDFSCli.SetUserPasswordResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.SetUserPasswordRequest,
      org.opendedup.grpc.SDFSCli.SetUserPasswordResponse> getSetSdfsPasswordMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.SetUserPasswordRequest, org.opendedup.grpc.SDFSCli.SetUserPasswordResponse> getSetSdfsPasswordMethod;
    if ((getSetSdfsPasswordMethod = SdfsUserServiceGrpc.getSetSdfsPasswordMethod) == null) {
      synchronized (SdfsUserServiceGrpc.class) {
        if ((getSetSdfsPasswordMethod = SdfsUserServiceGrpc.getSetSdfsPasswordMethod) == null) {
          SdfsUserServiceGrpc.getSetSdfsPasswordMethod = getSetSdfsPasswordMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SDFSCli.SetUserPasswordRequest, org.opendedup.grpc.SDFSCli.SetUserPasswordResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetSdfsPassword"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.SetUserPasswordRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.SetUserPasswordResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SdfsUserServiceMethodDescriptorSupplier("SetSdfsPassword"))
              .build();
        }
      }
    }
    return getSetSdfsPasswordMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.ListUsersRequest,
      org.opendedup.grpc.SDFSCli.ListUsersResponse> getListUsersMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListUsers",
      requestType = org.opendedup.grpc.SDFSCli.ListUsersRequest.class,
      responseType = org.opendedup.grpc.SDFSCli.ListUsersResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.ListUsersRequest,
      org.opendedup.grpc.SDFSCli.ListUsersResponse> getListUsersMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SDFSCli.ListUsersRequest, org.opendedup.grpc.SDFSCli.ListUsersResponse> getListUsersMethod;
    if ((getListUsersMethod = SdfsUserServiceGrpc.getListUsersMethod) == null) {
      synchronized (SdfsUserServiceGrpc.class) {
        if ((getListUsersMethod = SdfsUserServiceGrpc.getListUsersMethod) == null) {
          SdfsUserServiceGrpc.getListUsersMethod = getListUsersMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SDFSCli.ListUsersRequest, org.opendedup.grpc.SDFSCli.ListUsersResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListUsers"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.ListUsersRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SDFSCli.ListUsersResponse.getDefaultInstance()))
              .setSchemaDescriptor(new SdfsUserServiceMethodDescriptorSupplier("ListUsers"))
              .build();
        }
      }
    }
    return getListUsersMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SdfsUserServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SdfsUserServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SdfsUserServiceStub>() {
        @java.lang.Override
        public SdfsUserServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SdfsUserServiceStub(channel, callOptions);
        }
      };
    return SdfsUserServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SdfsUserServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SdfsUserServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SdfsUserServiceBlockingStub>() {
        @java.lang.Override
        public SdfsUserServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SdfsUserServiceBlockingStub(channel, callOptions);
        }
      };
    return SdfsUserServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SdfsUserServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<SdfsUserServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<SdfsUserServiceFutureStub>() {
        @java.lang.Override
        public SdfsUserServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new SdfsUserServiceFutureStub(channel, callOptions);
        }
      };
    return SdfsUserServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class SdfsUserServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void addUser(org.opendedup.grpc.SDFSCli.AddUserRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.AddUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAddUserMethod(), responseObserver);
    }

    /**
     */
    public void deleteUser(org.opendedup.grpc.SDFSCli.DeleteUserRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.DeleteUserResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteUserMethod(), responseObserver);
    }

    /**
     */
    public void setSdfsPermissions(org.opendedup.grpc.SDFSCli.SetPermissionsRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.SetPermissionsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetSdfsPermissionsMethod(), responseObserver);
    }

    /**
     */
    public void setSdfsPassword(org.opendedup.grpc.SDFSCli.SetUserPasswordRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.SetUserPasswordResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetSdfsPasswordMethod(), responseObserver);
    }

    /**
     */
    public void listUsers(org.opendedup.grpc.SDFSCli.ListUsersRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.ListUsersResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListUsersMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getAddUserMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SDFSCli.AddUserRequest,
                org.opendedup.grpc.SDFSCli.AddUserResponse>(
                  this, METHODID_ADD_USER)))
          .addMethod(
            getDeleteUserMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SDFSCli.DeleteUserRequest,
                org.opendedup.grpc.SDFSCli.DeleteUserResponse>(
                  this, METHODID_DELETE_USER)))
          .addMethod(
            getSetSdfsPermissionsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SDFSCli.SetPermissionsRequest,
                org.opendedup.grpc.SDFSCli.SetPermissionsResponse>(
                  this, METHODID_SET_SDFS_PERMISSIONS)))
          .addMethod(
            getSetSdfsPasswordMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SDFSCli.SetUserPasswordRequest,
                org.opendedup.grpc.SDFSCli.SetUserPasswordResponse>(
                  this, METHODID_SET_SDFS_PASSWORD)))
          .addMethod(
            getListUsersMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SDFSCli.ListUsersRequest,
                org.opendedup.grpc.SDFSCli.ListUsersResponse>(
                  this, METHODID_LIST_USERS)))
          .build();
    }
  }

  /**
   */
  public static final class SdfsUserServiceStub extends io.grpc.stub.AbstractAsyncStub<SdfsUserServiceStub> {
    private SdfsUserServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SdfsUserServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SdfsUserServiceStub(channel, callOptions);
    }

    /**
     */
    public void addUser(org.opendedup.grpc.SDFSCli.AddUserRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.AddUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAddUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteUser(org.opendedup.grpc.SDFSCli.DeleteUserRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.DeleteUserResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setSdfsPermissions(org.opendedup.grpc.SDFSCli.SetPermissionsRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.SetPermissionsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetSdfsPermissionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setSdfsPassword(org.opendedup.grpc.SDFSCli.SetUserPasswordRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.SetUserPasswordResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetSdfsPasswordMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listUsers(org.opendedup.grpc.SDFSCli.ListUsersRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.ListUsersResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListUsersMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class SdfsUserServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<SdfsUserServiceBlockingStub> {
    private SdfsUserServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SdfsUserServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SdfsUserServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.opendedup.grpc.SDFSCli.AddUserResponse addUser(org.opendedup.grpc.SDFSCli.AddUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAddUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SDFSCli.DeleteUserResponse deleteUser(org.opendedup.grpc.SDFSCli.DeleteUserRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SDFSCli.SetPermissionsResponse setSdfsPermissions(org.opendedup.grpc.SDFSCli.SetPermissionsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetSdfsPermissionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SDFSCli.SetUserPasswordResponse setSdfsPassword(org.opendedup.grpc.SDFSCli.SetUserPasswordRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetSdfsPasswordMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SDFSCli.ListUsersResponse listUsers(org.opendedup.grpc.SDFSCli.ListUsersRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListUsersMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class SdfsUserServiceFutureStub extends io.grpc.stub.AbstractFutureStub<SdfsUserServiceFutureStub> {
    private SdfsUserServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SdfsUserServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new SdfsUserServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SDFSCli.AddUserResponse> addUser(
        org.opendedup.grpc.SDFSCli.AddUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAddUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SDFSCli.DeleteUserResponse> deleteUser(
        org.opendedup.grpc.SDFSCli.DeleteUserRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SDFSCli.SetPermissionsResponse> setSdfsPermissions(
        org.opendedup.grpc.SDFSCli.SetPermissionsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetSdfsPermissionsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SDFSCli.SetUserPasswordResponse> setSdfsPassword(
        org.opendedup.grpc.SDFSCli.SetUserPasswordRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetSdfsPasswordMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SDFSCli.ListUsersResponse> listUsers(
        org.opendedup.grpc.SDFSCli.ListUsersRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListUsersMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ADD_USER = 0;
  private static final int METHODID_DELETE_USER = 1;
  private static final int METHODID_SET_SDFS_PERMISSIONS = 2;
  private static final int METHODID_SET_SDFS_PASSWORD = 3;
  private static final int METHODID_LIST_USERS = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final SdfsUserServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(SdfsUserServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ADD_USER:
          serviceImpl.addUser((org.opendedup.grpc.SDFSCli.AddUserRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.AddUserResponse>) responseObserver);
          break;
        case METHODID_DELETE_USER:
          serviceImpl.deleteUser((org.opendedup.grpc.SDFSCli.DeleteUserRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.DeleteUserResponse>) responseObserver);
          break;
        case METHODID_SET_SDFS_PERMISSIONS:
          serviceImpl.setSdfsPermissions((org.opendedup.grpc.SDFSCli.SetPermissionsRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.SetPermissionsResponse>) responseObserver);
          break;
        case METHODID_SET_SDFS_PASSWORD:
          serviceImpl.setSdfsPassword((org.opendedup.grpc.SDFSCli.SetUserPasswordRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.SetUserPasswordResponse>) responseObserver);
          break;
        case METHODID_LIST_USERS:
          serviceImpl.listUsers((org.opendedup.grpc.SDFSCli.ListUsersRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SDFSCli.ListUsersResponse>) responseObserver);
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

  private static abstract class SdfsUserServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SdfsUserServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.opendedup.grpc.SDFSCli.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("SdfsUserService");
    }
  }

  private static final class SdfsUserServiceFileDescriptorSupplier
      extends SdfsUserServiceBaseDescriptorSupplier {
    SdfsUserServiceFileDescriptorSupplier() {}
  }

  private static final class SdfsUserServiceMethodDescriptorSupplier
      extends SdfsUserServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    SdfsUserServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (SdfsUserServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SdfsUserServiceFileDescriptorSupplier())
              .addMethod(getAddUserMethod())
              .addMethod(getDeleteUserMethod())
              .addMethod(getSetSdfsPermissionsMethod())
              .addMethod(getSetSdfsPasswordMethod())
              .addMethod(getListUsersMethod())
              .build();
        }
      }
    }
    return result;
  }
}
