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
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.30.2)",
    comments = "Source: IOService.proto")
public final class FileIOServiceGrpc {

  private FileIOServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.FileIOService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.mkDirRequest,
      org.opendedup.grpc.mkDirResponse> getMkDirMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MkDir",
      requestType = org.opendedup.grpc.mkDirRequest.class,
      responseType = org.opendedup.grpc.mkDirResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.mkDirRequest,
      org.opendedup.grpc.mkDirResponse> getMkDirMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.mkDirRequest, org.opendedup.grpc.mkDirResponse> getMkDirMethod;
    if ((getMkDirMethod = FileIOServiceGrpc.getMkDirMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getMkDirMethod = FileIOServiceGrpc.getMkDirMethod) == null) {
          FileIOServiceGrpc.getMkDirMethod = getMkDirMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.mkDirRequest, org.opendedup.grpc.mkDirResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MkDir"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.mkDirRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.mkDirResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("MkDir"))
              .build();
        }
      }
    }
    return getMkDirMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.rmDirRequest,
      org.opendedup.grpc.rmDirResponse> getRmDirMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "rmDir",
      requestType = org.opendedup.grpc.rmDirRequest.class,
      responseType = org.opendedup.grpc.rmDirResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.rmDirRequest,
      org.opendedup.grpc.rmDirResponse> getRmDirMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.rmDirRequest, org.opendedup.grpc.rmDirResponse> getRmDirMethod;
    if ((getRmDirMethod = FileIOServiceGrpc.getRmDirMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getRmDirMethod = FileIOServiceGrpc.getRmDirMethod) == null) {
          FileIOServiceGrpc.getRmDirMethod = getRmDirMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.rmDirRequest, org.opendedup.grpc.rmDirResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "rmDir"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.rmDirRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.rmDirResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("rmDir"))
              .build();
        }
      }
    }
    return getRmDirMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.unlinkRequest,
      org.opendedup.grpc.unlinkResponse> getUnlinkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "unlink",
      requestType = org.opendedup.grpc.unlinkRequest.class,
      responseType = org.opendedup.grpc.unlinkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.unlinkRequest,
      org.opendedup.grpc.unlinkResponse> getUnlinkMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.unlinkRequest, org.opendedup.grpc.unlinkResponse> getUnlinkMethod;
    if ((getUnlinkMethod = FileIOServiceGrpc.getUnlinkMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getUnlinkMethod = FileIOServiceGrpc.getUnlinkMethod) == null) {
          FileIOServiceGrpc.getUnlinkMethod = getUnlinkMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.unlinkRequest, org.opendedup.grpc.unlinkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "unlink"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.unlinkRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.unlinkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("unlink"))
              .build();
        }
      }
    }
    return getUnlinkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.dataWriteRequest,
      org.opendedup.grpc.dataWriteResponse> getWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "write",
      requestType = org.opendedup.grpc.dataWriteRequest.class,
      responseType = org.opendedup.grpc.dataWriteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.dataWriteRequest,
      org.opendedup.grpc.dataWriteResponse> getWriteMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.dataWriteRequest, org.opendedup.grpc.dataWriteResponse> getWriteMethod;
    if ((getWriteMethod = FileIOServiceGrpc.getWriteMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getWriteMethod = FileIOServiceGrpc.getWriteMethod) == null) {
          FileIOServiceGrpc.getWriteMethod = getWriteMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.dataWriteRequest, org.opendedup.grpc.dataWriteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "write"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.dataWriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.dataWriteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("write"))
              .build();
        }
      }
    }
    return getWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.dataReadRequest,
      org.opendedup.grpc.dataReadResponse> getReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "read",
      requestType = org.opendedup.grpc.dataReadRequest.class,
      responseType = org.opendedup.grpc.dataReadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.dataReadRequest,
      org.opendedup.grpc.dataReadResponse> getReadMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.dataReadRequest, org.opendedup.grpc.dataReadResponse> getReadMethod;
    if ((getReadMethod = FileIOServiceGrpc.getReadMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getReadMethod = FileIOServiceGrpc.getReadMethod) == null) {
          FileIOServiceGrpc.getReadMethod = getReadMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.dataReadRequest, org.opendedup.grpc.dataReadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "read"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.dataReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.dataReadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("read"))
              .build();
        }
      }
    }
    return getReadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.fileCloseRequest,
      org.opendedup.grpc.fileCloseResponse> getReleaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "release",
      requestType = org.opendedup.grpc.fileCloseRequest.class,
      responseType = org.opendedup.grpc.fileCloseResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.fileCloseRequest,
      org.opendedup.grpc.fileCloseResponse> getReleaseMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.fileCloseRequest, org.opendedup.grpc.fileCloseResponse> getReleaseMethod;
    if ((getReleaseMethod = FileIOServiceGrpc.getReleaseMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getReleaseMethod = FileIOServiceGrpc.getReleaseMethod) == null) {
          FileIOServiceGrpc.getReleaseMethod = getReleaseMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.fileCloseRequest, org.opendedup.grpc.fileCloseResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "release"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.fileCloseRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.fileCloseResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("release"))
              .build();
        }
      }
    }
    return getReleaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.mkNodRequest,
      org.opendedup.grpc.mkNodResponse> getMknodMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "mknod",
      requestType = org.opendedup.grpc.mkNodRequest.class,
      responseType = org.opendedup.grpc.mkNodResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.mkNodRequest,
      org.opendedup.grpc.mkNodResponse> getMknodMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.mkNodRequest, org.opendedup.grpc.mkNodResponse> getMknodMethod;
    if ((getMknodMethod = FileIOServiceGrpc.getMknodMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getMknodMethod = FileIOServiceGrpc.getMknodMethod) == null) {
          FileIOServiceGrpc.getMknodMethod = getMknodMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.mkNodRequest, org.opendedup.grpc.mkNodResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "mknod"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.mkNodRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.mkNodResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("mknod"))
              .build();
        }
      }
    }
    return getMknodMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.fileOpenRequest,
      org.opendedup.grpc.fileOpenResponse> getOpenMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "open",
      requestType = org.opendedup.grpc.fileOpenRequest.class,
      responseType = org.opendedup.grpc.fileOpenResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.fileOpenRequest,
      org.opendedup.grpc.fileOpenResponse> getOpenMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.fileOpenRequest, org.opendedup.grpc.fileOpenResponse> getOpenMethod;
    if ((getOpenMethod = FileIOServiceGrpc.getOpenMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getOpenMethod = FileIOServiceGrpc.getOpenMethod) == null) {
          FileIOServiceGrpc.getOpenMethod = getOpenMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.fileOpenRequest, org.opendedup.grpc.fileOpenResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "open"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.fileOpenRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.fileOpenResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("open"))
              .build();
        }
      }
    }
    return getOpenMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FileIOServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FileIOServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FileIOServiceStub>() {
        @java.lang.Override
        public FileIOServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FileIOServiceStub(channel, callOptions);
        }
      };
    return FileIOServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FileIOServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FileIOServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FileIOServiceBlockingStub>() {
        @java.lang.Override
        public FileIOServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FileIOServiceBlockingStub(channel, callOptions);
        }
      };
    return FileIOServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FileIOServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FileIOServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FileIOServiceFutureStub>() {
        @java.lang.Override
        public FileIOServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FileIOServiceFutureStub(channel, callOptions);
        }
      };
    return FileIOServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class FileIOServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Define a RPC operation
     * </pre>
     */
    public void mkDir(org.opendedup.grpc.mkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.mkDirResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMkDirMethod(), responseObserver);
    }

    /**
     */
    public void rmDir(org.opendedup.grpc.rmDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.rmDirResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRmDirMethod(), responseObserver);
    }

    /**
     */
    public void unlink(org.opendedup.grpc.unlinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.unlinkResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getUnlinkMethod(), responseObserver);
    }

    /**
     */
    public void write(org.opendedup.grpc.dataWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.dataWriteResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getWriteMethod(), responseObserver);
    }

    /**
     */
    public void read(org.opendedup.grpc.dataReadRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.dataReadResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReadMethod(), responseObserver);
    }

    /**
     */
    public void release(org.opendedup.grpc.fileCloseRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.fileCloseResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReleaseMethod(), responseObserver);
    }

    /**
     */
    public void mknod(org.opendedup.grpc.mkNodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.mkNodResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMknodMethod(), responseObserver);
    }

    /**
     */
    public void open(org.opendedup.grpc.fileOpenRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.fileOpenResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getOpenMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getMkDirMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.mkDirRequest,
                org.opendedup.grpc.mkDirResponse>(
                  this, METHODID_MK_DIR)))
          .addMethod(
            getRmDirMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.rmDirRequest,
                org.opendedup.grpc.rmDirResponse>(
                  this, METHODID_RM_DIR)))
          .addMethod(
            getUnlinkMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.unlinkRequest,
                org.opendedup.grpc.unlinkResponse>(
                  this, METHODID_UNLINK)))
          .addMethod(
            getWriteMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.dataWriteRequest,
                org.opendedup.grpc.dataWriteResponse>(
                  this, METHODID_WRITE)))
          .addMethod(
            getReadMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.dataReadRequest,
                org.opendedup.grpc.dataReadResponse>(
                  this, METHODID_READ)))
          .addMethod(
            getReleaseMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.fileCloseRequest,
                org.opendedup.grpc.fileCloseResponse>(
                  this, METHODID_RELEASE)))
          .addMethod(
            getMknodMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.mkNodRequest,
                org.opendedup.grpc.mkNodResponse>(
                  this, METHODID_MKNOD)))
          .addMethod(
            getOpenMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.fileOpenRequest,
                org.opendedup.grpc.fileOpenResponse>(
                  this, METHODID_OPEN)))
          .build();
    }
  }

  /**
   */
  public static final class FileIOServiceStub extends io.grpc.stub.AbstractAsyncStub<FileIOServiceStub> {
    private FileIOServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileIOServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FileIOServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Define a RPC operation
     * </pre>
     */
    public void mkDir(org.opendedup.grpc.mkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.mkDirResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMkDirMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rmDir(org.opendedup.grpc.rmDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.rmDirResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRmDirMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void unlink(org.opendedup.grpc.unlinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.unlinkResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getUnlinkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void write(org.opendedup.grpc.dataWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.dataWriteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void read(org.opendedup.grpc.dataReadRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.dataReadResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void release(org.opendedup.grpc.fileCloseRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.fileCloseResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void mknod(org.opendedup.grpc.mkNodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.mkNodResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMknodMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void open(org.opendedup.grpc.fileOpenRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.fileOpenResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class FileIOServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<FileIOServiceBlockingStub> {
    private FileIOServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileIOServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FileIOServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Define a RPC operation
     * </pre>
     */
    public org.opendedup.grpc.mkDirResponse mkDir(org.opendedup.grpc.mkDirRequest request) {
      return blockingUnaryCall(
          getChannel(), getMkDirMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.rmDirResponse rmDir(org.opendedup.grpc.rmDirRequest request) {
      return blockingUnaryCall(
          getChannel(), getRmDirMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.unlinkResponse unlink(org.opendedup.grpc.unlinkRequest request) {
      return blockingUnaryCall(
          getChannel(), getUnlinkMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.dataWriteResponse write(org.opendedup.grpc.dataWriteRequest request) {
      return blockingUnaryCall(
          getChannel(), getWriteMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.dataReadResponse read(org.opendedup.grpc.dataReadRequest request) {
      return blockingUnaryCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.fileCloseResponse release(org.opendedup.grpc.fileCloseRequest request) {
      return blockingUnaryCall(
          getChannel(), getReleaseMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.mkNodResponse mknod(org.opendedup.grpc.mkNodRequest request) {
      return blockingUnaryCall(
          getChannel(), getMknodMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.fileOpenResponse open(org.opendedup.grpc.fileOpenRequest request) {
      return blockingUnaryCall(
          getChannel(), getOpenMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class FileIOServiceFutureStub extends io.grpc.stub.AbstractFutureStub<FileIOServiceFutureStub> {
    private FileIOServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileIOServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FileIOServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Define a RPC operation
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.mkDirResponse> mkDir(
        org.opendedup.grpc.mkDirRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMkDirMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.rmDirResponse> rmDir(
        org.opendedup.grpc.rmDirRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRmDirMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.unlinkResponse> unlink(
        org.opendedup.grpc.unlinkRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getUnlinkMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.dataWriteResponse> write(
        org.opendedup.grpc.dataWriteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.dataReadResponse> read(
        org.opendedup.grpc.dataReadRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.fileCloseResponse> release(
        org.opendedup.grpc.fileCloseRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.mkNodResponse> mknod(
        org.opendedup.grpc.mkNodRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMknodMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.fileOpenResponse> open(
        org.opendedup.grpc.fileOpenRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_MK_DIR = 0;
  private static final int METHODID_RM_DIR = 1;
  private static final int METHODID_UNLINK = 2;
  private static final int METHODID_WRITE = 3;
  private static final int METHODID_READ = 4;
  private static final int METHODID_RELEASE = 5;
  private static final int METHODID_MKNOD = 6;
  private static final int METHODID_OPEN = 7;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final FileIOServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(FileIOServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_MK_DIR:
          serviceImpl.mkDir((org.opendedup.grpc.mkDirRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.mkDirResponse>) responseObserver);
          break;
        case METHODID_RM_DIR:
          serviceImpl.rmDir((org.opendedup.grpc.rmDirRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.rmDirResponse>) responseObserver);
          break;
        case METHODID_UNLINK:
          serviceImpl.unlink((org.opendedup.grpc.unlinkRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.unlinkResponse>) responseObserver);
          break;
        case METHODID_WRITE:
          serviceImpl.write((org.opendedup.grpc.dataWriteRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.dataWriteResponse>) responseObserver);
          break;
        case METHODID_READ:
          serviceImpl.read((org.opendedup.grpc.dataReadRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.dataReadResponse>) responseObserver);
          break;
        case METHODID_RELEASE:
          serviceImpl.release((org.opendedup.grpc.fileCloseRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.fileCloseResponse>) responseObserver);
          break;
        case METHODID_MKNOD:
          serviceImpl.mknod((org.opendedup.grpc.mkNodRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.mkNodResponse>) responseObserver);
          break;
        case METHODID_OPEN:
          serviceImpl.open((org.opendedup.grpc.fileOpenRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.fileOpenResponse>) responseObserver);
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

  private static abstract class FileIOServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FileIOServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.opendedup.grpc.IOService.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FileIOService");
    }
  }

  private static final class FileIOServiceFileDescriptorSupplier
      extends FileIOServiceBaseDescriptorSupplier {
    FileIOServiceFileDescriptorSupplier() {}
  }

  private static final class FileIOServiceMethodDescriptorSupplier
      extends FileIOServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    FileIOServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (FileIOServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FileIOServiceFileDescriptorSupplier())
              .addMethod(getMkDirMethod())
              .addMethod(getRmDirMethod())
              .addMethod(getUnlinkMethod())
              .addMethod(getWriteMethod())
              .addMethod(getReadMethod())
              .addMethod(getReleaseMethod())
              .addMethod(getMknodMethod())
              .addMethod(getOpenMethod())
              .build();
        }
      }
    }
    return result;
  }
}
