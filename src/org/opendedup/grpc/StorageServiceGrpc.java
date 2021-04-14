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
    comments = "Source: Storage.proto")
public final class StorageServiceGrpc {

  private StorageServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.StorageService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.HashingInfoRequest,
      org.opendedup.grpc.Storage.HashingInfoResponse> getHashingInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HashingInfo",
      requestType = org.opendedup.grpc.Storage.HashingInfoRequest.class,
      responseType = org.opendedup.grpc.Storage.HashingInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.HashingInfoRequest,
      org.opendedup.grpc.Storage.HashingInfoResponse> getHashingInfoMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.HashingInfoRequest, org.opendedup.grpc.Storage.HashingInfoResponse> getHashingInfoMethod;
    if ((getHashingInfoMethod = StorageServiceGrpc.getHashingInfoMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getHashingInfoMethod = StorageServiceGrpc.getHashingInfoMethod) == null) {
          StorageServiceGrpc.getHashingInfoMethod = getHashingInfoMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.Storage.HashingInfoRequest, org.opendedup.grpc.Storage.HashingInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HashingInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.HashingInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.HashingInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("HashingInfo"))
              .build();
        }
      }
    }
    return getHashingInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.CheckHashesRequest,
      org.opendedup.grpc.Storage.CheckHashesResponse> getCheckHashesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CheckHashes",
      requestType = org.opendedup.grpc.Storage.CheckHashesRequest.class,
      responseType = org.opendedup.grpc.Storage.CheckHashesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.CheckHashesRequest,
      org.opendedup.grpc.Storage.CheckHashesResponse> getCheckHashesMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.CheckHashesRequest, org.opendedup.grpc.Storage.CheckHashesResponse> getCheckHashesMethod;
    if ((getCheckHashesMethod = StorageServiceGrpc.getCheckHashesMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getCheckHashesMethod = StorageServiceGrpc.getCheckHashesMethod) == null) {
          StorageServiceGrpc.getCheckHashesMethod = getCheckHashesMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.Storage.CheckHashesRequest, org.opendedup.grpc.Storage.CheckHashesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CheckHashes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.CheckHashesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.CheckHashesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("CheckHashes"))
              .build();
        }
      }
    }
    return getCheckHashesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.WriteChunksRequest,
      org.opendedup.grpc.Storage.WriteChunksResponse> getWriteChunksMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WriteChunks",
      requestType = org.opendedup.grpc.Storage.WriteChunksRequest.class,
      responseType = org.opendedup.grpc.Storage.WriteChunksResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.WriteChunksRequest,
      org.opendedup.grpc.Storage.WriteChunksResponse> getWriteChunksMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.WriteChunksRequest, org.opendedup.grpc.Storage.WriteChunksResponse> getWriteChunksMethod;
    if ((getWriteChunksMethod = StorageServiceGrpc.getWriteChunksMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getWriteChunksMethod = StorageServiceGrpc.getWriteChunksMethod) == null) {
          StorageServiceGrpc.getWriteChunksMethod = getWriteChunksMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.Storage.WriteChunksRequest, org.opendedup.grpc.Storage.WriteChunksResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WriteChunks"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.WriteChunksRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.WriteChunksResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("WriteChunks"))
              .build();
        }
      }
    }
    return getWriteChunksMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.ReadChunksRequest,
      org.opendedup.grpc.Storage.ReadChunksResponse> getReadChunksMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadChunks",
      requestType = org.opendedup.grpc.Storage.ReadChunksRequest.class,
      responseType = org.opendedup.grpc.Storage.ReadChunksResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.ReadChunksRequest,
      org.opendedup.grpc.Storage.ReadChunksResponse> getReadChunksMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.ReadChunksRequest, org.opendedup.grpc.Storage.ReadChunksResponse> getReadChunksMethod;
    if ((getReadChunksMethod = StorageServiceGrpc.getReadChunksMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getReadChunksMethod = StorageServiceGrpc.getReadChunksMethod) == null) {
          StorageServiceGrpc.getReadChunksMethod = getReadChunksMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.Storage.ReadChunksRequest, org.opendedup.grpc.Storage.ReadChunksResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadChunks"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.ReadChunksRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.ReadChunksResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("ReadChunks"))
              .build();
        }
      }
    }
    return getReadChunksMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest,
      org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse> getWriteSparseDataChunkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WriteSparseDataChunk",
      requestType = org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest.class,
      responseType = org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest,
      org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse> getWriteSparseDataChunkMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest, org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse> getWriteSparseDataChunkMethod;
    if ((getWriteSparseDataChunkMethod = StorageServiceGrpc.getWriteSparseDataChunkMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getWriteSparseDataChunkMethod = StorageServiceGrpc.getWriteSparseDataChunkMethod) == null) {
          StorageServiceGrpc.getWriteSparseDataChunkMethod = getWriteSparseDataChunkMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest, org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WriteSparseDataChunk"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("WriteSparseDataChunk"))
              .build();
        }
      }
    }
    return getWriteSparseDataChunkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest,
      org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse> getReadSparseDataChunkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadSparseDataChunk",
      requestType = org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest.class,
      responseType = org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest,
      org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse> getReadSparseDataChunkMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest, org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse> getReadSparseDataChunkMethod;
    if ((getReadSparseDataChunkMethod = StorageServiceGrpc.getReadSparseDataChunkMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getReadSparseDataChunkMethod = StorageServiceGrpc.getReadSparseDataChunkMethod) == null) {
          StorageServiceGrpc.getReadSparseDataChunkMethod = getReadSparseDataChunkMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest, org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadSparseDataChunk"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("ReadSparseDataChunk"))
              .build();
        }
      }
    }
    return getReadSparseDataChunkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.MetaDataDedupeFileRequest,
      org.opendedup.grpc.Storage.ChunkResponse> getGetMetaDataDedupeFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMetaDataDedupeFile",
      requestType = org.opendedup.grpc.Storage.MetaDataDedupeFileRequest.class,
      responseType = org.opendedup.grpc.Storage.ChunkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.MetaDataDedupeFileRequest,
      org.opendedup.grpc.Storage.ChunkResponse> getGetMetaDataDedupeFileMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.MetaDataDedupeFileRequest, org.opendedup.grpc.Storage.ChunkResponse> getGetMetaDataDedupeFileMethod;
    if ((getGetMetaDataDedupeFileMethod = StorageServiceGrpc.getGetMetaDataDedupeFileMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getGetMetaDataDedupeFileMethod = StorageServiceGrpc.getGetMetaDataDedupeFileMethod) == null) {
          StorageServiceGrpc.getGetMetaDataDedupeFileMethod = getGetMetaDataDedupeFileMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.Storage.MetaDataDedupeFileRequest, org.opendedup.grpc.Storage.ChunkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMetaDataDedupeFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.MetaDataDedupeFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.ChunkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("GetMetaDataDedupeFile"))
              .build();
        }
      }
    }
    return getGetMetaDataDedupeFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.SparseDedupeFileRequest,
      org.opendedup.grpc.Storage.ChunkResponse> getGetSparseDedupeFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetSparseDedupeFile",
      requestType = org.opendedup.grpc.Storage.SparseDedupeFileRequest.class,
      responseType = org.opendedup.grpc.Storage.ChunkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.SparseDedupeFileRequest,
      org.opendedup.grpc.Storage.ChunkResponse> getGetSparseDedupeFileMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.Storage.SparseDedupeFileRequest, org.opendedup.grpc.Storage.ChunkResponse> getGetSparseDedupeFileMethod;
    if ((getGetSparseDedupeFileMethod = StorageServiceGrpc.getGetSparseDedupeFileMethod) == null) {
      synchronized (StorageServiceGrpc.class) {
        if ((getGetSparseDedupeFileMethod = StorageServiceGrpc.getGetSparseDedupeFileMethod) == null) {
          StorageServiceGrpc.getGetSparseDedupeFileMethod = getGetSparseDedupeFileMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.Storage.SparseDedupeFileRequest, org.opendedup.grpc.Storage.ChunkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSparseDedupeFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.SparseDedupeFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Storage.ChunkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorageServiceMethodDescriptorSupplier("GetSparseDedupeFile"))
              .build();
        }
      }
    }
    return getGetSparseDedupeFileMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static StorageServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorageServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorageServiceStub>() {
        @java.lang.Override
        public StorageServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorageServiceStub(channel, callOptions);
        }
      };
    return StorageServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static StorageServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorageServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorageServiceBlockingStub>() {
        @java.lang.Override
        public StorageServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorageServiceBlockingStub(channel, callOptions);
        }
      };
    return StorageServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static StorageServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorageServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorageServiceFutureStub>() {
        @java.lang.Override
        public StorageServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorageServiceFutureStub(channel, callOptions);
        }
      };
    return StorageServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static abstract class StorageServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void hashingInfo(org.opendedup.grpc.Storage.HashingInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.HashingInfoResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getHashingInfoMethod(), responseObserver);
    }

    /**
     */
    public void checkHashes(org.opendedup.grpc.Storage.CheckHashesRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.CheckHashesResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCheckHashesMethod(), responseObserver);
    }

    /**
     */
    public void writeChunks(org.opendedup.grpc.Storage.WriteChunksRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.WriteChunksResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getWriteChunksMethod(), responseObserver);
    }

    /**
     */
    public void readChunks(org.opendedup.grpc.Storage.ReadChunksRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.ReadChunksResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReadChunksMethod(), responseObserver);
    }

    /**
     */
    public void writeSparseDataChunk(org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getWriteSparseDataChunkMethod(), responseObserver);
    }

    /**
     */
    public void readSparseDataChunk(org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReadSparseDataChunkMethod(), responseObserver);
    }

    /**
     */
    public void getMetaDataDedupeFile(org.opendedup.grpc.Storage.MetaDataDedupeFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.ChunkResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetMetaDataDedupeFileMethod(), responseObserver);
    }

    /**
     */
    public void getSparseDedupeFile(org.opendedup.grpc.Storage.SparseDedupeFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.ChunkResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetSparseDedupeFileMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getHashingInfoMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.Storage.HashingInfoRequest,
                org.opendedup.grpc.Storage.HashingInfoResponse>(
                  this, METHODID_HASHING_INFO)))
          .addMethod(
            getCheckHashesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.Storage.CheckHashesRequest,
                org.opendedup.grpc.Storage.CheckHashesResponse>(
                  this, METHODID_CHECK_HASHES)))
          .addMethod(
            getWriteChunksMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.Storage.WriteChunksRequest,
                org.opendedup.grpc.Storage.WriteChunksResponse>(
                  this, METHODID_WRITE_CHUNKS)))
          .addMethod(
            getReadChunksMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.Storage.ReadChunksRequest,
                org.opendedup.grpc.Storage.ReadChunksResponse>(
                  this, METHODID_READ_CHUNKS)))
          .addMethod(
            getWriteSparseDataChunkMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest,
                org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse>(
                  this, METHODID_WRITE_SPARSE_DATA_CHUNK)))
          .addMethod(
            getReadSparseDataChunkMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest,
                org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse>(
                  this, METHODID_READ_SPARSE_DATA_CHUNK)))
          .addMethod(
            getGetMetaDataDedupeFileMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                org.opendedup.grpc.Storage.MetaDataDedupeFileRequest,
                org.opendedup.grpc.Storage.ChunkResponse>(
                  this, METHODID_GET_META_DATA_DEDUPE_FILE)))
          .addMethod(
            getGetSparseDedupeFileMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                org.opendedup.grpc.Storage.SparseDedupeFileRequest,
                org.opendedup.grpc.Storage.ChunkResponse>(
                  this, METHODID_GET_SPARSE_DEDUPE_FILE)))
          .build();
    }
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static final class StorageServiceStub extends io.grpc.stub.AbstractAsyncStub<StorageServiceStub> {
    private StorageServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorageServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorageServiceStub(channel, callOptions);
    }

    /**
     */
    public void hashingInfo(org.opendedup.grpc.Storage.HashingInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.HashingInfoResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getHashingInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void checkHashes(org.opendedup.grpc.Storage.CheckHashesRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.CheckHashesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCheckHashesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void writeChunks(org.opendedup.grpc.Storage.WriteChunksRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.WriteChunksResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getWriteChunksMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void readChunks(org.opendedup.grpc.Storage.ReadChunksRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.ReadChunksResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getReadChunksMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void writeSparseDataChunk(org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getWriteSparseDataChunkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void readSparseDataChunk(org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getReadSparseDataChunkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getMetaDataDedupeFile(org.opendedup.grpc.Storage.MetaDataDedupeFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.ChunkResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getGetMetaDataDedupeFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getSparseDedupeFile(org.opendedup.grpc.Storage.SparseDedupeFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.ChunkResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getGetSparseDedupeFileMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static final class StorageServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<StorageServiceBlockingStub> {
    private StorageServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorageServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorageServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.opendedup.grpc.Storage.HashingInfoResponse hashingInfo(org.opendedup.grpc.Storage.HashingInfoRequest request) {
      return blockingUnaryCall(
          getChannel(), getHashingInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.Storage.CheckHashesResponse checkHashes(org.opendedup.grpc.Storage.CheckHashesRequest request) {
      return blockingUnaryCall(
          getChannel(), getCheckHashesMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.Storage.WriteChunksResponse writeChunks(org.opendedup.grpc.Storage.WriteChunksRequest request) {
      return blockingUnaryCall(
          getChannel(), getWriteChunksMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.Storage.ReadChunksResponse readChunks(org.opendedup.grpc.Storage.ReadChunksRequest request) {
      return blockingUnaryCall(
          getChannel(), getReadChunksMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse writeSparseDataChunk(org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest request) {
      return blockingUnaryCall(
          getChannel(), getWriteSparseDataChunkMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse readSparseDataChunk(org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest request) {
      return blockingUnaryCall(
          getChannel(), getReadSparseDataChunkMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<org.opendedup.grpc.Storage.ChunkResponse> getMetaDataDedupeFile(
        org.opendedup.grpc.Storage.MetaDataDedupeFileRequest request) {
      return blockingServerStreamingCall(
          getChannel(), getGetMetaDataDedupeFileMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<org.opendedup.grpc.Storage.ChunkResponse> getSparseDedupeFile(
        org.opendedup.grpc.Storage.SparseDedupeFileRequest request) {
      return blockingServerStreamingCall(
          getChannel(), getGetSparseDedupeFileMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Defining a Service, a Service can have multiple RPC operations
   * </pre>
   */
  public static final class StorageServiceFutureStub extends io.grpc.stub.AbstractFutureStub<StorageServiceFutureStub> {
    private StorageServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorageServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorageServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.Storage.HashingInfoResponse> hashingInfo(
        org.opendedup.grpc.Storage.HashingInfoRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getHashingInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.Storage.CheckHashesResponse> checkHashes(
        org.opendedup.grpc.Storage.CheckHashesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCheckHashesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.Storage.WriteChunksResponse> writeChunks(
        org.opendedup.grpc.Storage.WriteChunksRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getWriteChunksMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.Storage.ReadChunksResponse> readChunks(
        org.opendedup.grpc.Storage.ReadChunksRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getReadChunksMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse> writeSparseDataChunk(
        org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getWriteSparseDataChunkMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse> readSparseDataChunk(
        org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getReadSparseDataChunkMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_HASHING_INFO = 0;
  private static final int METHODID_CHECK_HASHES = 1;
  private static final int METHODID_WRITE_CHUNKS = 2;
  private static final int METHODID_READ_CHUNKS = 3;
  private static final int METHODID_WRITE_SPARSE_DATA_CHUNK = 4;
  private static final int METHODID_READ_SPARSE_DATA_CHUNK = 5;
  private static final int METHODID_GET_META_DATA_DEDUPE_FILE = 6;
  private static final int METHODID_GET_SPARSE_DEDUPE_FILE = 7;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final StorageServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(StorageServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_HASHING_INFO:
          serviceImpl.hashingInfo((org.opendedup.grpc.Storage.HashingInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.HashingInfoResponse>) responseObserver);
          break;
        case METHODID_CHECK_HASHES:
          serviceImpl.checkHashes((org.opendedup.grpc.Storage.CheckHashesRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.CheckHashesResponse>) responseObserver);
          break;
        case METHODID_WRITE_CHUNKS:
          serviceImpl.writeChunks((org.opendedup.grpc.Storage.WriteChunksRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.WriteChunksResponse>) responseObserver);
          break;
        case METHODID_READ_CHUNKS:
          serviceImpl.readChunks((org.opendedup.grpc.Storage.ReadChunksRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.ReadChunksResponse>) responseObserver);
          break;
        case METHODID_WRITE_SPARSE_DATA_CHUNK:
          serviceImpl.writeSparseDataChunk((org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse>) responseObserver);
          break;
        case METHODID_READ_SPARSE_DATA_CHUNK:
          serviceImpl.readSparseDataChunk((org.opendedup.grpc.Storage.SparseDedupeChunkReadRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.SparseDedupeChunkReadResponse>) responseObserver);
          break;
        case METHODID_GET_META_DATA_DEDUPE_FILE:
          serviceImpl.getMetaDataDedupeFile((org.opendedup.grpc.Storage.MetaDataDedupeFileRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.ChunkResponse>) responseObserver);
          break;
        case METHODID_GET_SPARSE_DEDUPE_FILE:
          serviceImpl.getSparseDedupeFile((org.opendedup.grpc.Storage.SparseDedupeFileRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.Storage.ChunkResponse>) responseObserver);
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

  private static abstract class StorageServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    StorageServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.opendedup.grpc.Storage.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("StorageService");
    }
  }

  private static final class StorageServiceFileDescriptorSupplier
      extends StorageServiceBaseDescriptorSupplier {
    StorageServiceFileDescriptorSupplier() {}
  }

  private static final class StorageServiceMethodDescriptorSupplier
      extends StorageServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    StorageServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (StorageServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new StorageServiceFileDescriptorSupplier())
              .addMethod(getHashingInfoMethod())
              .addMethod(getCheckHashesMethod())
              .addMethod(getWriteChunksMethod())
              .addMethod(getReadChunksMethod())
              .addMethod(getWriteSparseDataChunkMethod())
              .addMethod(getReadSparseDataChunkMethod())
              .addMethod(getGetMetaDataDedupeFileMethod())
              .addMethod(getGetSparseDedupeFileMethod())
              .build();
        }
      }
    }
    return result;
  }
}
