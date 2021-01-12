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
    value = "by gRPC proto compiler (version 1.31.1)",
    comments = "Source: IOService.proto")
public final class FileIOServiceGrpc {

  private FileIOServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.FileIOService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.GetXAttrSizeRequest,
      org.opendedup.grpc.GetXAttrSizeResponse> getGetXAttrSizeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetXAttrSize",
      requestType = org.opendedup.grpc.GetXAttrSizeRequest.class,
      responseType = org.opendedup.grpc.GetXAttrSizeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.GetXAttrSizeRequest,
      org.opendedup.grpc.GetXAttrSizeResponse> getGetXAttrSizeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.GetXAttrSizeRequest, org.opendedup.grpc.GetXAttrSizeResponse> getGetXAttrSizeMethod;
    if ((getGetXAttrSizeMethod = FileIOServiceGrpc.getGetXAttrSizeMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetXAttrSizeMethod = FileIOServiceGrpc.getGetXAttrSizeMethod) == null) {
          FileIOServiceGrpc.getGetXAttrSizeMethod = getGetXAttrSizeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.GetXAttrSizeRequest, org.opendedup.grpc.GetXAttrSizeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetXAttrSize"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GetXAttrSizeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GetXAttrSizeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetXAttrSize"))
              .build();
        }
      }
    }
    return getGetXAttrSizeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FsyncRequest,
      org.opendedup.grpc.FsyncResponse> getFsyncMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Fsync",
      requestType = org.opendedup.grpc.FsyncRequest.class,
      responseType = org.opendedup.grpc.FsyncResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FsyncRequest,
      org.opendedup.grpc.FsyncResponse> getFsyncMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FsyncRequest, org.opendedup.grpc.FsyncResponse> getFsyncMethod;
    if ((getFsyncMethod = FileIOServiceGrpc.getFsyncMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getFsyncMethod = FileIOServiceGrpc.getFsyncMethod) == null) {
          FileIOServiceGrpc.getFsyncMethod = getFsyncMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FsyncRequest, org.opendedup.grpc.FsyncResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Fsync"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FsyncRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FsyncResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Fsync"))
              .build();
        }
      }
    }
    return getFsyncMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SetXAttrRequest,
      org.opendedup.grpc.SetXAttrResponse> getSetXAttrMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetXAttr",
      requestType = org.opendedup.grpc.SetXAttrRequest.class,
      responseType = org.opendedup.grpc.SetXAttrResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SetXAttrRequest,
      org.opendedup.grpc.SetXAttrResponse> getSetXAttrMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SetXAttrRequest, org.opendedup.grpc.SetXAttrResponse> getSetXAttrMethod;
    if ((getSetXAttrMethod = FileIOServiceGrpc.getSetXAttrMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getSetXAttrMethod = FileIOServiceGrpc.getSetXAttrMethod) == null) {
          FileIOServiceGrpc.getSetXAttrMethod = getSetXAttrMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SetXAttrRequest, org.opendedup.grpc.SetXAttrResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetXAttr"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetXAttrRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetXAttrResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("SetXAttr"))
              .build();
        }
      }
    }
    return getSetXAttrMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.RemoveXAttrRequest,
      org.opendedup.grpc.RemoveXAttrResponse> getRemoveXAttrMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RemoveXAttr",
      requestType = org.opendedup.grpc.RemoveXAttrRequest.class,
      responseType = org.opendedup.grpc.RemoveXAttrResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.RemoveXAttrRequest,
      org.opendedup.grpc.RemoveXAttrResponse> getRemoveXAttrMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.RemoveXAttrRequest, org.opendedup.grpc.RemoveXAttrResponse> getRemoveXAttrMethod;
    if ((getRemoveXAttrMethod = FileIOServiceGrpc.getRemoveXAttrMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getRemoveXAttrMethod = FileIOServiceGrpc.getRemoveXAttrMethod) == null) {
          FileIOServiceGrpc.getRemoveXAttrMethod = getRemoveXAttrMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.RemoveXAttrRequest, org.opendedup.grpc.RemoveXAttrResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RemoveXAttr"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.RemoveXAttrRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.RemoveXAttrResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("RemoveXAttr"))
              .build();
        }
      }
    }
    return getRemoveXAttrMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.GetXAttrRequest,
      org.opendedup.grpc.GetXAttrResponse> getGetXAttrMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetXAttr",
      requestType = org.opendedup.grpc.GetXAttrRequest.class,
      responseType = org.opendedup.grpc.GetXAttrResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.GetXAttrRequest,
      org.opendedup.grpc.GetXAttrResponse> getGetXAttrMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.GetXAttrRequest, org.opendedup.grpc.GetXAttrResponse> getGetXAttrMethod;
    if ((getGetXAttrMethod = FileIOServiceGrpc.getGetXAttrMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetXAttrMethod = FileIOServiceGrpc.getGetXAttrMethod) == null) {
          FileIOServiceGrpc.getGetXAttrMethod = getGetXAttrMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.GetXAttrRequest, org.opendedup.grpc.GetXAttrResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetXAttr"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GetXAttrRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GetXAttrResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetXAttr"))
              .build();
        }
      }
    }
    return getGetXAttrMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.UtimeRequest,
      org.opendedup.grpc.UtimeResponse> getUtimeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Utime",
      requestType = org.opendedup.grpc.UtimeRequest.class,
      responseType = org.opendedup.grpc.UtimeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.UtimeRequest,
      org.opendedup.grpc.UtimeResponse> getUtimeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.UtimeRequest, org.opendedup.grpc.UtimeResponse> getUtimeMethod;
    if ((getUtimeMethod = FileIOServiceGrpc.getUtimeMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getUtimeMethod = FileIOServiceGrpc.getUtimeMethod) == null) {
          FileIOServiceGrpc.getUtimeMethod = getUtimeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.UtimeRequest, org.opendedup.grpc.UtimeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Utime"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.UtimeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.UtimeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Utime"))
              .build();
        }
      }
    }
    return getUtimeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.TruncateRequest,
      org.opendedup.grpc.TruncateResponse> getTruncateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Truncate",
      requestType = org.opendedup.grpc.TruncateRequest.class,
      responseType = org.opendedup.grpc.TruncateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.TruncateRequest,
      org.opendedup.grpc.TruncateResponse> getTruncateMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.TruncateRequest, org.opendedup.grpc.TruncateResponse> getTruncateMethod;
    if ((getTruncateMethod = FileIOServiceGrpc.getTruncateMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getTruncateMethod = FileIOServiceGrpc.getTruncateMethod) == null) {
          FileIOServiceGrpc.getTruncateMethod = getTruncateMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.TruncateRequest, org.opendedup.grpc.TruncateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Truncate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.TruncateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.TruncateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Truncate"))
              .build();
        }
      }
    }
    return getTruncateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SymLinkRequest,
      org.opendedup.grpc.SymLinkResponse> getSymLinkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SymLink",
      requestType = org.opendedup.grpc.SymLinkRequest.class,
      responseType = org.opendedup.grpc.SymLinkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SymLinkRequest,
      org.opendedup.grpc.SymLinkResponse> getSymLinkMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SymLinkRequest, org.opendedup.grpc.SymLinkResponse> getSymLinkMethod;
    if ((getSymLinkMethod = FileIOServiceGrpc.getSymLinkMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getSymLinkMethod = FileIOServiceGrpc.getSymLinkMethod) == null) {
          FileIOServiceGrpc.getSymLinkMethod = getSymLinkMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SymLinkRequest, org.opendedup.grpc.SymLinkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SymLink"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SymLinkRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SymLinkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("SymLink"))
              .build();
        }
      }
    }
    return getSymLinkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.LinkRequest,
      org.opendedup.grpc.LinkResponse> getReadLinkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadLink",
      requestType = org.opendedup.grpc.LinkRequest.class,
      responseType = org.opendedup.grpc.LinkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.LinkRequest,
      org.opendedup.grpc.LinkResponse> getReadLinkMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.LinkRequest, org.opendedup.grpc.LinkResponse> getReadLinkMethod;
    if ((getReadLinkMethod = FileIOServiceGrpc.getReadLinkMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getReadLinkMethod = FileIOServiceGrpc.getReadLinkMethod) == null) {
          FileIOServiceGrpc.getReadLinkMethod = getReadLinkMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.LinkRequest, org.opendedup.grpc.LinkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadLink"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.LinkRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.LinkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("ReadLink"))
              .build();
        }
      }
    }
    return getReadLinkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.StatRequest,
      org.opendedup.grpc.StatResponse> getGetAttrMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAttr",
      requestType = org.opendedup.grpc.StatRequest.class,
      responseType = org.opendedup.grpc.StatResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.StatRequest,
      org.opendedup.grpc.StatResponse> getGetAttrMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.StatRequest, org.opendedup.grpc.StatResponse> getGetAttrMethod;
    if ((getGetAttrMethod = FileIOServiceGrpc.getGetAttrMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetAttrMethod = FileIOServiceGrpc.getGetAttrMethod) == null) {
          FileIOServiceGrpc.getGetAttrMethod = getGetAttrMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.StatRequest, org.opendedup.grpc.StatResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetAttr"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.StatRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.StatResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetAttr"))
              .build();
        }
      }
    }
    return getGetAttrMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FlushRequest,
      org.opendedup.grpc.FlushResponse> getFlushMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Flush",
      requestType = org.opendedup.grpc.FlushRequest.class,
      responseType = org.opendedup.grpc.FlushResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FlushRequest,
      org.opendedup.grpc.FlushResponse> getFlushMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FlushRequest, org.opendedup.grpc.FlushResponse> getFlushMethod;
    if ((getFlushMethod = FileIOServiceGrpc.getFlushMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getFlushMethod = FileIOServiceGrpc.getFlushMethod) == null) {
          FileIOServiceGrpc.getFlushMethod = getFlushMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FlushRequest, org.opendedup.grpc.FlushResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Flush"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FlushRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FlushResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Flush"))
              .build();
        }
      }
    }
    return getFlushMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.ChownRequest,
      org.opendedup.grpc.ChownResponse> getChownMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Chown",
      requestType = org.opendedup.grpc.ChownRequest.class,
      responseType = org.opendedup.grpc.ChownResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.ChownRequest,
      org.opendedup.grpc.ChownResponse> getChownMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.ChownRequest, org.opendedup.grpc.ChownResponse> getChownMethod;
    if ((getChownMethod = FileIOServiceGrpc.getChownMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getChownMethod = FileIOServiceGrpc.getChownMethod) == null) {
          FileIOServiceGrpc.getChownMethod = getChownMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.ChownRequest, org.opendedup.grpc.ChownResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Chown"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.ChownRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.ChownResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Chown"))
              .build();
        }
      }
    }
    return getChownMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.ChmodRequest,
      org.opendedup.grpc.ChmodResponse> getChmodMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Chmod",
      requestType = org.opendedup.grpc.ChmodRequest.class,
      responseType = org.opendedup.grpc.ChmodResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.ChmodRequest,
      org.opendedup.grpc.ChmodResponse> getChmodMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.ChmodRequest, org.opendedup.grpc.ChmodResponse> getChmodMethod;
    if ((getChmodMethod = FileIOServiceGrpc.getChmodMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getChmodMethod = FileIOServiceGrpc.getChmodMethod) == null) {
          FileIOServiceGrpc.getChmodMethod = getChmodMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.ChmodRequest, org.opendedup.grpc.ChmodResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Chmod"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.ChmodRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.ChmodResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Chmod"))
              .build();
        }
      }
    }
    return getChmodMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.MkDirRequest,
      org.opendedup.grpc.MkDirResponse> getMkDirMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MkDir",
      requestType = org.opendedup.grpc.MkDirRequest.class,
      responseType = org.opendedup.grpc.MkDirResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.MkDirRequest,
      org.opendedup.grpc.MkDirResponse> getMkDirMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.MkDirRequest, org.opendedup.grpc.MkDirResponse> getMkDirMethod;
    if ((getMkDirMethod = FileIOServiceGrpc.getMkDirMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getMkDirMethod = FileIOServiceGrpc.getMkDirMethod) == null) {
          FileIOServiceGrpc.getMkDirMethod = getMkDirMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.MkDirRequest, org.opendedup.grpc.MkDirResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MkDir"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.MkDirRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.MkDirResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("MkDir"))
              .build();
        }
      }
    }
    return getMkDirMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.RmDirRequest,
      org.opendedup.grpc.RmDirResponse> getRmDirMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RmDir",
      requestType = org.opendedup.grpc.RmDirRequest.class,
      responseType = org.opendedup.grpc.RmDirResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.RmDirRequest,
      org.opendedup.grpc.RmDirResponse> getRmDirMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.RmDirRequest, org.opendedup.grpc.RmDirResponse> getRmDirMethod;
    if ((getRmDirMethod = FileIOServiceGrpc.getRmDirMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getRmDirMethod = FileIOServiceGrpc.getRmDirMethod) == null) {
          FileIOServiceGrpc.getRmDirMethod = getRmDirMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.RmDirRequest, org.opendedup.grpc.RmDirResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RmDir"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.RmDirRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.RmDirResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("RmDir"))
              .build();
        }
      }
    }
    return getRmDirMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.UnlinkRequest,
      org.opendedup.grpc.UnlinkResponse> getUnlinkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Unlink",
      requestType = org.opendedup.grpc.UnlinkRequest.class,
      responseType = org.opendedup.grpc.UnlinkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.UnlinkRequest,
      org.opendedup.grpc.UnlinkResponse> getUnlinkMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.UnlinkRequest, org.opendedup.grpc.UnlinkResponse> getUnlinkMethod;
    if ((getUnlinkMethod = FileIOServiceGrpc.getUnlinkMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getUnlinkMethod = FileIOServiceGrpc.getUnlinkMethod) == null) {
          FileIOServiceGrpc.getUnlinkMethod = getUnlinkMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.UnlinkRequest, org.opendedup.grpc.UnlinkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Unlink"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.UnlinkRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.UnlinkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Unlink"))
              .build();
        }
      }
    }
    return getUnlinkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.DataWriteRequest,
      org.opendedup.grpc.DataWriteResponse> getWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Write",
      requestType = org.opendedup.grpc.DataWriteRequest.class,
      responseType = org.opendedup.grpc.DataWriteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.DataWriteRequest,
      org.opendedup.grpc.DataWriteResponse> getWriteMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.DataWriteRequest, org.opendedup.grpc.DataWriteResponse> getWriteMethod;
    if ((getWriteMethod = FileIOServiceGrpc.getWriteMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getWriteMethod = FileIOServiceGrpc.getWriteMethod) == null) {
          FileIOServiceGrpc.getWriteMethod = getWriteMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.DataWriteRequest, org.opendedup.grpc.DataWriteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Write"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.DataWriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.DataWriteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Write"))
              .build();
        }
      }
    }
    return getWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.DataReadRequest,
      org.opendedup.grpc.DataReadResponse> getReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Read",
      requestType = org.opendedup.grpc.DataReadRequest.class,
      responseType = org.opendedup.grpc.DataReadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.DataReadRequest,
      org.opendedup.grpc.DataReadResponse> getReadMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.DataReadRequest, org.opendedup.grpc.DataReadResponse> getReadMethod;
    if ((getReadMethod = FileIOServiceGrpc.getReadMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getReadMethod = FileIOServiceGrpc.getReadMethod) == null) {
          FileIOServiceGrpc.getReadMethod = getReadMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.DataReadRequest, org.opendedup.grpc.DataReadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Read"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.DataReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.DataReadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Read"))
              .build();
        }
      }
    }
    return getReadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FileCloseRequest,
      org.opendedup.grpc.FileCloseResponse> getReleaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Release",
      requestType = org.opendedup.grpc.FileCloseRequest.class,
      responseType = org.opendedup.grpc.FileCloseResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FileCloseRequest,
      org.opendedup.grpc.FileCloseResponse> getReleaseMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FileCloseRequest, org.opendedup.grpc.FileCloseResponse> getReleaseMethod;
    if ((getReleaseMethod = FileIOServiceGrpc.getReleaseMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getReleaseMethod = FileIOServiceGrpc.getReleaseMethod) == null) {
          FileIOServiceGrpc.getReleaseMethod = getReleaseMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FileCloseRequest, org.opendedup.grpc.FileCloseResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Release"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileCloseRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileCloseResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Release"))
              .build();
        }
      }
    }
    return getReleaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.MkNodRequest,
      org.opendedup.grpc.MkNodResponse> getMknodMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Mknod",
      requestType = org.opendedup.grpc.MkNodRequest.class,
      responseType = org.opendedup.grpc.MkNodResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.MkNodRequest,
      org.opendedup.grpc.MkNodResponse> getMknodMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.MkNodRequest, org.opendedup.grpc.MkNodResponse> getMknodMethod;
    if ((getMknodMethod = FileIOServiceGrpc.getMknodMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getMknodMethod = FileIOServiceGrpc.getMknodMethod) == null) {
          FileIOServiceGrpc.getMknodMethod = getMknodMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.MkNodRequest, org.opendedup.grpc.MkNodResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Mknod"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.MkNodRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.MkNodResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Mknod"))
              .build();
        }
      }
    }
    return getMknodMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FileOpenRequest,
      org.opendedup.grpc.FileOpenResponse> getOpenMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Open",
      requestType = org.opendedup.grpc.FileOpenRequest.class,
      responseType = org.opendedup.grpc.FileOpenResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FileOpenRequest,
      org.opendedup.grpc.FileOpenResponse> getOpenMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FileOpenRequest, org.opendedup.grpc.FileOpenResponse> getOpenMethod;
    if ((getOpenMethod = FileIOServiceGrpc.getOpenMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getOpenMethod = FileIOServiceGrpc.getOpenMethod) == null) {
          FileIOServiceGrpc.getOpenMethod = getOpenMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FileOpenRequest, org.opendedup.grpc.FileOpenResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Open"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileOpenRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileOpenResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Open"))
              .build();
        }
      }
    }
    return getOpenMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfoRequest,
      org.opendedup.grpc.FileMessageResponse> getGetFileInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetFileInfo",
      requestType = org.opendedup.grpc.FileInfoRequest.class,
      responseType = org.opendedup.grpc.FileMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfoRequest,
      org.opendedup.grpc.FileMessageResponse> getGetFileInfoMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfoRequest, org.opendedup.grpc.FileMessageResponse> getGetFileInfoMethod;
    if ((getGetFileInfoMethod = FileIOServiceGrpc.getGetFileInfoMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetFileInfoMethod = FileIOServiceGrpc.getGetFileInfoMethod) == null) {
          FileIOServiceGrpc.getGetFileInfoMethod = getGetFileInfoMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FileInfoRequest, org.opendedup.grpc.FileMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetFileInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetFileInfo"))
              .build();
        }
      }
    }
    return getGetFileInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FileSnapshotRequest,
      org.opendedup.grpc.FileSnapshotResponse> getCreateCopyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateCopy",
      requestType = org.opendedup.grpc.FileSnapshotRequest.class,
      responseType = org.opendedup.grpc.FileSnapshotResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FileSnapshotRequest,
      org.opendedup.grpc.FileSnapshotResponse> getCreateCopyMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FileSnapshotRequest, org.opendedup.grpc.FileSnapshotResponse> getCreateCopyMethod;
    if ((getCreateCopyMethod = FileIOServiceGrpc.getCreateCopyMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getCreateCopyMethod = FileIOServiceGrpc.getCreateCopyMethod) == null) {
          FileIOServiceGrpc.getCreateCopyMethod = getCreateCopyMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FileSnapshotRequest, org.opendedup.grpc.FileSnapshotResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateCopy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileSnapshotRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileSnapshotResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("CreateCopy"))
              .build();
        }
      }
    }
    return getCreateCopyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FileExistsRequest,
      org.opendedup.grpc.FileExistsResponse> getFileExistsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "FileExists",
      requestType = org.opendedup.grpc.FileExistsRequest.class,
      responseType = org.opendedup.grpc.FileExistsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FileExistsRequest,
      org.opendedup.grpc.FileExistsResponse> getFileExistsMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FileExistsRequest, org.opendedup.grpc.FileExistsResponse> getFileExistsMethod;
    if ((getFileExistsMethod = FileIOServiceGrpc.getFileExistsMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getFileExistsMethod = FileIOServiceGrpc.getFileExistsMethod) == null) {
          FileIOServiceGrpc.getFileExistsMethod = getFileExistsMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FileExistsRequest, org.opendedup.grpc.FileExistsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "FileExists"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileExistsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileExistsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("FileExists"))
              .build();
        }
      }
    }
    return getFileExistsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.MkDirRequest,
      org.opendedup.grpc.MkDirResponse> getMkDirAllMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MkDirAll",
      requestType = org.opendedup.grpc.MkDirRequest.class,
      responseType = org.opendedup.grpc.MkDirResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.MkDirRequest,
      org.opendedup.grpc.MkDirResponse> getMkDirAllMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.MkDirRequest, org.opendedup.grpc.MkDirResponse> getMkDirAllMethod;
    if ((getMkDirAllMethod = FileIOServiceGrpc.getMkDirAllMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getMkDirAllMethod = FileIOServiceGrpc.getMkDirAllMethod) == null) {
          FileIOServiceGrpc.getMkDirAllMethod = getMkDirAllMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.MkDirRequest, org.opendedup.grpc.MkDirResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MkDirAll"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.MkDirRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.MkDirResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("MkDirAll"))
              .build();
        }
      }
    }
    return getMkDirAllMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfoRequest,
      org.opendedup.grpc.FileMessageResponse> getStatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Stat",
      requestType = org.opendedup.grpc.FileInfoRequest.class,
      responseType = org.opendedup.grpc.FileMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfoRequest,
      org.opendedup.grpc.FileMessageResponse> getStatMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfoRequest, org.opendedup.grpc.FileMessageResponse> getStatMethod;
    if ((getStatMethod = FileIOServiceGrpc.getStatMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getStatMethod = FileIOServiceGrpc.getStatMethod) == null) {
          FileIOServiceGrpc.getStatMethod = getStatMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FileInfoRequest, org.opendedup.grpc.FileMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Stat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Stat"))
              .build();
        }
      }
    }
    return getStatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FileRenameRequest,
      org.opendedup.grpc.FileRenameResponse> getRenameMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Rename",
      requestType = org.opendedup.grpc.FileRenameRequest.class,
      responseType = org.opendedup.grpc.FileRenameResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FileRenameRequest,
      org.opendedup.grpc.FileRenameResponse> getRenameMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FileRenameRequest, org.opendedup.grpc.FileRenameResponse> getRenameMethod;
    if ((getRenameMethod = FileIOServiceGrpc.getRenameMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getRenameMethod = FileIOServiceGrpc.getRenameMethod) == null) {
          FileIOServiceGrpc.getRenameMethod = getRenameMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FileRenameRequest, org.opendedup.grpc.FileRenameResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Rename"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileRenameRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileRenameResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Rename"))
              .build();
        }
      }
    }
    return getRenameMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.CopyExtentRequest,
      org.opendedup.grpc.CopyExtentResponse> getCopyExtentMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CopyExtent",
      requestType = org.opendedup.grpc.CopyExtentRequest.class,
      responseType = org.opendedup.grpc.CopyExtentResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.CopyExtentRequest,
      org.opendedup.grpc.CopyExtentResponse> getCopyExtentMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.CopyExtentRequest, org.opendedup.grpc.CopyExtentResponse> getCopyExtentMethod;
    if ((getCopyExtentMethod = FileIOServiceGrpc.getCopyExtentMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getCopyExtentMethod = FileIOServiceGrpc.getCopyExtentMethod) == null) {
          FileIOServiceGrpc.getCopyExtentMethod = getCopyExtentMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.CopyExtentRequest, org.opendedup.grpc.CopyExtentResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CopyExtent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.CopyExtentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.CopyExtentResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("CopyExtent"))
              .build();
        }
      }
    }
    return getCopyExtentMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SetUserMetaDataRequest,
      org.opendedup.grpc.SetUserMetaDataResponse> getSetUserMetaDataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetUserMetaData",
      requestType = org.opendedup.grpc.SetUserMetaDataRequest.class,
      responseType = org.opendedup.grpc.SetUserMetaDataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SetUserMetaDataRequest,
      org.opendedup.grpc.SetUserMetaDataResponse> getSetUserMetaDataMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SetUserMetaDataRequest, org.opendedup.grpc.SetUserMetaDataResponse> getSetUserMetaDataMethod;
    if ((getSetUserMetaDataMethod = FileIOServiceGrpc.getSetUserMetaDataMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getSetUserMetaDataMethod = FileIOServiceGrpc.getSetUserMetaDataMethod) == null) {
          FileIOServiceGrpc.getSetUserMetaDataMethod = getSetUserMetaDataMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SetUserMetaDataRequest, org.opendedup.grpc.SetUserMetaDataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetUserMetaData"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetUserMetaDataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetUserMetaDataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("SetUserMetaData"))
              .build();
        }
      }
    }
    return getSetUserMetaDataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.GetCloudFileRequest,
      org.opendedup.grpc.GetCloudFileResponse> getGetCloudFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCloudFile",
      requestType = org.opendedup.grpc.GetCloudFileRequest.class,
      responseType = org.opendedup.grpc.GetCloudFileResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.GetCloudFileRequest,
      org.opendedup.grpc.GetCloudFileResponse> getGetCloudFileMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.GetCloudFileRequest, org.opendedup.grpc.GetCloudFileResponse> getGetCloudFileMethod;
    if ((getGetCloudFileMethod = FileIOServiceGrpc.getGetCloudFileMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetCloudFileMethod = FileIOServiceGrpc.getGetCloudFileMethod) == null) {
          FileIOServiceGrpc.getGetCloudFileMethod = getGetCloudFileMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.GetCloudFileRequest, org.opendedup.grpc.GetCloudFileResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCloudFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GetCloudFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GetCloudFileResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetCloudFile"))
              .build();
        }
      }
    }
    return getGetCloudFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.GetCloudFileRequest,
      org.opendedup.grpc.GetCloudFileResponse> getGetCloudMetaFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCloudMetaFile",
      requestType = org.opendedup.grpc.GetCloudFileRequest.class,
      responseType = org.opendedup.grpc.GetCloudFileResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.GetCloudFileRequest,
      org.opendedup.grpc.GetCloudFileResponse> getGetCloudMetaFileMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.GetCloudFileRequest, org.opendedup.grpc.GetCloudFileResponse> getGetCloudMetaFileMethod;
    if ((getGetCloudMetaFileMethod = FileIOServiceGrpc.getGetCloudMetaFileMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetCloudMetaFileMethod = FileIOServiceGrpc.getGetCloudMetaFileMethod) == null) {
          FileIOServiceGrpc.getGetCloudMetaFileMethod = getGetCloudMetaFileMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.GetCloudFileRequest, org.opendedup.grpc.GetCloudFileResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCloudMetaFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GetCloudFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GetCloudFileResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetCloudMetaFile"))
              .build();
        }
      }
    }
    return getGetCloudMetaFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.StatFSRequest,
      org.opendedup.grpc.StatFSResponse> getStatFSMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StatFS",
      requestType = org.opendedup.grpc.StatFSRequest.class,
      responseType = org.opendedup.grpc.StatFSResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.StatFSRequest,
      org.opendedup.grpc.StatFSResponse> getStatFSMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.StatFSRequest, org.opendedup.grpc.StatFSResponse> getStatFSMethod;
    if ((getStatFSMethod = FileIOServiceGrpc.getStatFSMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getStatFSMethod = FileIOServiceGrpc.getStatFSMethod) == null) {
          FileIOServiceGrpc.getStatFSMethod = getStatFSMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.StatFSRequest, org.opendedup.grpc.StatFSResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StatFS"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.StatFSRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.StatFSResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("StatFS"))
              .build();
        }
      }
    }
    return getStatFSMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SyncNotificationSubscription,
      org.opendedup.grpc.FileMessageResponse> getFileNotificationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "fileNotification",
      requestType = org.opendedup.grpc.SyncNotificationSubscription.class,
      responseType = org.opendedup.grpc.FileMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SyncNotificationSubscription,
      org.opendedup.grpc.FileMessageResponse> getFileNotificationMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SyncNotificationSubscription, org.opendedup.grpc.FileMessageResponse> getFileNotificationMethod;
    if ((getFileNotificationMethod = FileIOServiceGrpc.getFileNotificationMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getFileNotificationMethod = FileIOServiceGrpc.getFileNotificationMethod) == null) {
          FileIOServiceGrpc.getFileNotificationMethod = getFileNotificationMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SyncNotificationSubscription, org.opendedup.grpc.FileMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "fileNotification"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SyncNotificationSubscription.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("fileNotification"))
              .build();
        }
      }
    }
    return getFileNotificationMethod;
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
    public void getXAttrSize(org.opendedup.grpc.GetXAttrSizeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GetXAttrSizeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetXAttrSizeMethod(), responseObserver);
    }

    /**
     */
    public void fsync(org.opendedup.grpc.FsyncRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FsyncResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getFsyncMethod(), responseObserver);
    }

    /**
     */
    public void setXAttr(org.opendedup.grpc.SetXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetXAttrResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetXAttrMethod(), responseObserver);
    }

    /**
     */
    public void removeXAttr(org.opendedup.grpc.RemoveXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.RemoveXAttrResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRemoveXAttrMethod(), responseObserver);
    }

    /**
     */
    public void getXAttr(org.opendedup.grpc.GetXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GetXAttrResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetXAttrMethod(), responseObserver);
    }

    /**
     */
    public void utime(org.opendedup.grpc.UtimeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.UtimeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getUtimeMethod(), responseObserver);
    }

    /**
     */
    public void truncate(org.opendedup.grpc.TruncateRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.TruncateResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getTruncateMethod(), responseObserver);
    }

    /**
     */
    public void symLink(org.opendedup.grpc.SymLinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SymLinkResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSymLinkMethod(), responseObserver);
    }

    /**
     */
    public void readLink(org.opendedup.grpc.LinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.LinkResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReadLinkMethod(), responseObserver);
    }

    /**
     */
    public void getAttr(org.opendedup.grpc.StatRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.StatResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetAttrMethod(), responseObserver);
    }

    /**
     */
    public void flush(org.opendedup.grpc.FlushRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FlushResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getFlushMethod(), responseObserver);
    }

    /**
     */
    public void chown(org.opendedup.grpc.ChownRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.ChownResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getChownMethod(), responseObserver);
    }

    /**
     */
    public void chmod(org.opendedup.grpc.ChmodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.ChmodResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getChmodMethod(), responseObserver);
    }

    /**
     */
    public void mkDir(org.opendedup.grpc.MkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.MkDirResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMkDirMethod(), responseObserver);
    }

    /**
     */
    public void rmDir(org.opendedup.grpc.RmDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.RmDirResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRmDirMethod(), responseObserver);
    }

    /**
     */
    public void unlink(org.opendedup.grpc.UnlinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.UnlinkResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getUnlinkMethod(), responseObserver);
    }

    /**
     */
    public void write(org.opendedup.grpc.DataWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.DataWriteResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getWriteMethod(), responseObserver);
    }

    /**
     */
    public void read(org.opendedup.grpc.DataReadRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.DataReadResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReadMethod(), responseObserver);
    }

    /**
     */
    public void release(org.opendedup.grpc.FileCloseRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileCloseResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReleaseMethod(), responseObserver);
    }

    /**
     */
    public void mknod(org.opendedup.grpc.MkNodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.MkNodResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMknodMethod(), responseObserver);
    }

    /**
     */
    public void open(org.opendedup.grpc.FileOpenRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileOpenResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getOpenMethod(), responseObserver);
    }

    /**
     */
    public void getFileInfo(org.opendedup.grpc.FileInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileMessageResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetFileInfoMethod(), responseObserver);
    }

    /**
     */
    public void createCopy(org.opendedup.grpc.FileSnapshotRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileSnapshotResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateCopyMethod(), responseObserver);
    }

    /**
     */
    public void fileExists(org.opendedup.grpc.FileExistsRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileExistsResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getFileExistsMethod(), responseObserver);
    }

    /**
     */
    public void mkDirAll(org.opendedup.grpc.MkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.MkDirResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getMkDirAllMethod(), responseObserver);
    }

    /**
     */
    public void stat(org.opendedup.grpc.FileInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileMessageResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getStatMethod(), responseObserver);
    }

    /**
     */
    public void rename(org.opendedup.grpc.FileRenameRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileRenameResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRenameMethod(), responseObserver);
    }

    /**
     */
    public void copyExtent(org.opendedup.grpc.CopyExtentRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.CopyExtentResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCopyExtentMethod(), responseObserver);
    }

    /**
     */
    public void setUserMetaData(org.opendedup.grpc.SetUserMetaDataRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetUserMetaDataResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetUserMetaDataMethod(), responseObserver);
    }

    /**
     */
    public void getCloudFile(org.opendedup.grpc.GetCloudFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GetCloudFileResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetCloudFileMethod(), responseObserver);
    }

    /**
     */
    public void getCloudMetaFile(org.opendedup.grpc.GetCloudFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GetCloudFileResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetCloudMetaFileMethod(), responseObserver);
    }

    /**
     */
    public void statFS(org.opendedup.grpc.StatFSRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.StatFSResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getStatFSMethod(), responseObserver);
    }

    /**
     */
    public void fileNotification(org.opendedup.grpc.SyncNotificationSubscription request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileMessageResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getFileNotificationMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetXAttrSizeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.GetXAttrSizeRequest,
                org.opendedup.grpc.GetXAttrSizeResponse>(
                  this, METHODID_GET_XATTR_SIZE)))
          .addMethod(
            getFsyncMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FsyncRequest,
                org.opendedup.grpc.FsyncResponse>(
                  this, METHODID_FSYNC)))
          .addMethod(
            getSetXAttrMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SetXAttrRequest,
                org.opendedup.grpc.SetXAttrResponse>(
                  this, METHODID_SET_XATTR)))
          .addMethod(
            getRemoveXAttrMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.RemoveXAttrRequest,
                org.opendedup.grpc.RemoveXAttrResponse>(
                  this, METHODID_REMOVE_XATTR)))
          .addMethod(
            getGetXAttrMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.GetXAttrRequest,
                org.opendedup.grpc.GetXAttrResponse>(
                  this, METHODID_GET_XATTR)))
          .addMethod(
            getUtimeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.UtimeRequest,
                org.opendedup.grpc.UtimeResponse>(
                  this, METHODID_UTIME)))
          .addMethod(
            getTruncateMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.TruncateRequest,
                org.opendedup.grpc.TruncateResponse>(
                  this, METHODID_TRUNCATE)))
          .addMethod(
            getSymLinkMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SymLinkRequest,
                org.opendedup.grpc.SymLinkResponse>(
                  this, METHODID_SYM_LINK)))
          .addMethod(
            getReadLinkMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.LinkRequest,
                org.opendedup.grpc.LinkResponse>(
                  this, METHODID_READ_LINK)))
          .addMethod(
            getGetAttrMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.StatRequest,
                org.opendedup.grpc.StatResponse>(
                  this, METHODID_GET_ATTR)))
          .addMethod(
            getFlushMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FlushRequest,
                org.opendedup.grpc.FlushResponse>(
                  this, METHODID_FLUSH)))
          .addMethod(
            getChownMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.ChownRequest,
                org.opendedup.grpc.ChownResponse>(
                  this, METHODID_CHOWN)))
          .addMethod(
            getChmodMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.ChmodRequest,
                org.opendedup.grpc.ChmodResponse>(
                  this, METHODID_CHMOD)))
          .addMethod(
            getMkDirMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.MkDirRequest,
                org.opendedup.grpc.MkDirResponse>(
                  this, METHODID_MK_DIR)))
          .addMethod(
            getRmDirMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.RmDirRequest,
                org.opendedup.grpc.RmDirResponse>(
                  this, METHODID_RM_DIR)))
          .addMethod(
            getUnlinkMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.UnlinkRequest,
                org.opendedup.grpc.UnlinkResponse>(
                  this, METHODID_UNLINK)))
          .addMethod(
            getWriteMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.DataWriteRequest,
                org.opendedup.grpc.DataWriteResponse>(
                  this, METHODID_WRITE)))
          .addMethod(
            getReadMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.DataReadRequest,
                org.opendedup.grpc.DataReadResponse>(
                  this, METHODID_READ)))
          .addMethod(
            getReleaseMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FileCloseRequest,
                org.opendedup.grpc.FileCloseResponse>(
                  this, METHODID_RELEASE)))
          .addMethod(
            getMknodMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.MkNodRequest,
                org.opendedup.grpc.MkNodResponse>(
                  this, METHODID_MKNOD)))
          .addMethod(
            getOpenMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FileOpenRequest,
                org.opendedup.grpc.FileOpenResponse>(
                  this, METHODID_OPEN)))
          .addMethod(
            getGetFileInfoMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FileInfoRequest,
                org.opendedup.grpc.FileMessageResponse>(
                  this, METHODID_GET_FILE_INFO)))
          .addMethod(
            getCreateCopyMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FileSnapshotRequest,
                org.opendedup.grpc.FileSnapshotResponse>(
                  this, METHODID_CREATE_COPY)))
          .addMethod(
            getFileExistsMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FileExistsRequest,
                org.opendedup.grpc.FileExistsResponse>(
                  this, METHODID_FILE_EXISTS)))
          .addMethod(
            getMkDirAllMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.MkDirRequest,
                org.opendedup.grpc.MkDirResponse>(
                  this, METHODID_MK_DIR_ALL)))
          .addMethod(
            getStatMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FileInfoRequest,
                org.opendedup.grpc.FileMessageResponse>(
                  this, METHODID_STAT)))
          .addMethod(
            getRenameMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FileRenameRequest,
                org.opendedup.grpc.FileRenameResponse>(
                  this, METHODID_RENAME)))
          .addMethod(
            getCopyExtentMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.CopyExtentRequest,
                org.opendedup.grpc.CopyExtentResponse>(
                  this, METHODID_COPY_EXTENT)))
          .addMethod(
            getSetUserMetaDataMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SetUserMetaDataRequest,
                org.opendedup.grpc.SetUserMetaDataResponse>(
                  this, METHODID_SET_USER_META_DATA)))
          .addMethod(
            getGetCloudFileMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.GetCloudFileRequest,
                org.opendedup.grpc.GetCloudFileResponse>(
                  this, METHODID_GET_CLOUD_FILE)))
          .addMethod(
            getGetCloudMetaFileMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.GetCloudFileRequest,
                org.opendedup.grpc.GetCloudFileResponse>(
                  this, METHODID_GET_CLOUD_META_FILE)))
          .addMethod(
            getStatFSMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.StatFSRequest,
                org.opendedup.grpc.StatFSResponse>(
                  this, METHODID_STAT_FS)))
          .addMethod(
            getFileNotificationMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                org.opendedup.grpc.SyncNotificationSubscription,
                org.opendedup.grpc.FileMessageResponse>(
                  this, METHODID_FILE_NOTIFICATION)))
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
    public void getXAttrSize(org.opendedup.grpc.GetXAttrSizeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GetXAttrSizeResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetXAttrSizeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void fsync(org.opendedup.grpc.FsyncRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FsyncResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getFsyncMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setXAttr(org.opendedup.grpc.SetXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetXAttrResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetXAttrMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void removeXAttr(org.opendedup.grpc.RemoveXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.RemoveXAttrResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRemoveXAttrMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getXAttr(org.opendedup.grpc.GetXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GetXAttrResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetXAttrMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void utime(org.opendedup.grpc.UtimeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.UtimeResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getUtimeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void truncate(org.opendedup.grpc.TruncateRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.TruncateResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getTruncateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void symLink(org.opendedup.grpc.SymLinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SymLinkResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSymLinkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void readLink(org.opendedup.grpc.LinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.LinkResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getReadLinkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getAttr(org.opendedup.grpc.StatRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.StatResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetAttrMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void flush(org.opendedup.grpc.FlushRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FlushResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getFlushMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void chown(org.opendedup.grpc.ChownRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.ChownResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getChownMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void chmod(org.opendedup.grpc.ChmodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.ChmodResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getChmodMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void mkDir(org.opendedup.grpc.MkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.MkDirResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMkDirMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rmDir(org.opendedup.grpc.RmDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.RmDirResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRmDirMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void unlink(org.opendedup.grpc.UnlinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.UnlinkResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getUnlinkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void write(org.opendedup.grpc.DataWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.DataWriteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void read(org.opendedup.grpc.DataReadRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.DataReadResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void release(org.opendedup.grpc.FileCloseRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileCloseResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void mknod(org.opendedup.grpc.MkNodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.MkNodResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMknodMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void open(org.opendedup.grpc.FileOpenRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileOpenResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getFileInfo(org.opendedup.grpc.FileInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileMessageResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetFileInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createCopy(org.opendedup.grpc.FileSnapshotRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileSnapshotResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateCopyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void fileExists(org.opendedup.grpc.FileExistsRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileExistsResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getFileExistsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void mkDirAll(org.opendedup.grpc.MkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.MkDirResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getMkDirAllMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void stat(org.opendedup.grpc.FileInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileMessageResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getStatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rename(org.opendedup.grpc.FileRenameRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileRenameResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRenameMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void copyExtent(org.opendedup.grpc.CopyExtentRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.CopyExtentResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCopyExtentMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setUserMetaData(org.opendedup.grpc.SetUserMetaDataRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetUserMetaDataResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetUserMetaDataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getCloudFile(org.opendedup.grpc.GetCloudFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GetCloudFileResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetCloudFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getCloudMetaFile(org.opendedup.grpc.GetCloudFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GetCloudFileResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetCloudMetaFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void statFS(org.opendedup.grpc.StatFSRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.StatFSResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getStatFSMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void fileNotification(org.opendedup.grpc.SyncNotificationSubscription request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileMessageResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getFileNotificationMethod(), getCallOptions()), request, responseObserver);
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
    public org.opendedup.grpc.GetXAttrSizeResponse getXAttrSize(org.opendedup.grpc.GetXAttrSizeRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetXAttrSizeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FsyncResponse fsync(org.opendedup.grpc.FsyncRequest request) {
      return blockingUnaryCall(
          getChannel(), getFsyncMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SetXAttrResponse setXAttr(org.opendedup.grpc.SetXAttrRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetXAttrMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.RemoveXAttrResponse removeXAttr(org.opendedup.grpc.RemoveXAttrRequest request) {
      return blockingUnaryCall(
          getChannel(), getRemoveXAttrMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.GetXAttrResponse getXAttr(org.opendedup.grpc.GetXAttrRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetXAttrMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.UtimeResponse utime(org.opendedup.grpc.UtimeRequest request) {
      return blockingUnaryCall(
          getChannel(), getUtimeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.TruncateResponse truncate(org.opendedup.grpc.TruncateRequest request) {
      return blockingUnaryCall(
          getChannel(), getTruncateMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SymLinkResponse symLink(org.opendedup.grpc.SymLinkRequest request) {
      return blockingUnaryCall(
          getChannel(), getSymLinkMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.LinkResponse readLink(org.opendedup.grpc.LinkRequest request) {
      return blockingUnaryCall(
          getChannel(), getReadLinkMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.StatResponse getAttr(org.opendedup.grpc.StatRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetAttrMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FlushResponse flush(org.opendedup.grpc.FlushRequest request) {
      return blockingUnaryCall(
          getChannel(), getFlushMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.ChownResponse chown(org.opendedup.grpc.ChownRequest request) {
      return blockingUnaryCall(
          getChannel(), getChownMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.ChmodResponse chmod(org.opendedup.grpc.ChmodRequest request) {
      return blockingUnaryCall(
          getChannel(), getChmodMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.MkDirResponse mkDir(org.opendedup.grpc.MkDirRequest request) {
      return blockingUnaryCall(
          getChannel(), getMkDirMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.RmDirResponse rmDir(org.opendedup.grpc.RmDirRequest request) {
      return blockingUnaryCall(
          getChannel(), getRmDirMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.UnlinkResponse unlink(org.opendedup.grpc.UnlinkRequest request) {
      return blockingUnaryCall(
          getChannel(), getUnlinkMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.DataWriteResponse write(org.opendedup.grpc.DataWriteRequest request) {
      return blockingUnaryCall(
          getChannel(), getWriteMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.DataReadResponse read(org.opendedup.grpc.DataReadRequest request) {
      return blockingUnaryCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FileCloseResponse release(org.opendedup.grpc.FileCloseRequest request) {
      return blockingUnaryCall(
          getChannel(), getReleaseMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.MkNodResponse mknod(org.opendedup.grpc.MkNodRequest request) {
      return blockingUnaryCall(
          getChannel(), getMknodMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FileOpenResponse open(org.opendedup.grpc.FileOpenRequest request) {
      return blockingUnaryCall(
          getChannel(), getOpenMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FileMessageResponse getFileInfo(org.opendedup.grpc.FileInfoRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetFileInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FileSnapshotResponse createCopy(org.opendedup.grpc.FileSnapshotRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateCopyMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FileExistsResponse fileExists(org.opendedup.grpc.FileExistsRequest request) {
      return blockingUnaryCall(
          getChannel(), getFileExistsMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.MkDirResponse mkDirAll(org.opendedup.grpc.MkDirRequest request) {
      return blockingUnaryCall(
          getChannel(), getMkDirAllMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FileMessageResponse stat(org.opendedup.grpc.FileInfoRequest request) {
      return blockingUnaryCall(
          getChannel(), getStatMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FileRenameResponse rename(org.opendedup.grpc.FileRenameRequest request) {
      return blockingUnaryCall(
          getChannel(), getRenameMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.CopyExtentResponse copyExtent(org.opendedup.grpc.CopyExtentRequest request) {
      return blockingUnaryCall(
          getChannel(), getCopyExtentMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SetUserMetaDataResponse setUserMetaData(org.opendedup.grpc.SetUserMetaDataRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetUserMetaDataMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.GetCloudFileResponse getCloudFile(org.opendedup.grpc.GetCloudFileRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetCloudFileMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.GetCloudFileResponse getCloudMetaFile(org.opendedup.grpc.GetCloudFileRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetCloudMetaFileMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.StatFSResponse statFS(org.opendedup.grpc.StatFSRequest request) {
      return blockingUnaryCall(
          getChannel(), getStatFSMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<org.opendedup.grpc.FileMessageResponse> fileNotification(
        org.opendedup.grpc.SyncNotificationSubscription request) {
      return blockingServerStreamingCall(
          getChannel(), getFileNotificationMethod(), getCallOptions(), request);
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
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.GetXAttrSizeResponse> getXAttrSize(
        org.opendedup.grpc.GetXAttrSizeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetXAttrSizeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FsyncResponse> fsync(
        org.opendedup.grpc.FsyncRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getFsyncMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SetXAttrResponse> setXAttr(
        org.opendedup.grpc.SetXAttrRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSetXAttrMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.RemoveXAttrResponse> removeXAttr(
        org.opendedup.grpc.RemoveXAttrRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRemoveXAttrMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.GetXAttrResponse> getXAttr(
        org.opendedup.grpc.GetXAttrRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetXAttrMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.UtimeResponse> utime(
        org.opendedup.grpc.UtimeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getUtimeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.TruncateResponse> truncate(
        org.opendedup.grpc.TruncateRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getTruncateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SymLinkResponse> symLink(
        org.opendedup.grpc.SymLinkRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSymLinkMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.LinkResponse> readLink(
        org.opendedup.grpc.LinkRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getReadLinkMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.StatResponse> getAttr(
        org.opendedup.grpc.StatRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetAttrMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FlushResponse> flush(
        org.opendedup.grpc.FlushRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getFlushMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.ChownResponse> chown(
        org.opendedup.grpc.ChownRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getChownMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.ChmodResponse> chmod(
        org.opendedup.grpc.ChmodRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getChmodMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.MkDirResponse> mkDir(
        org.opendedup.grpc.MkDirRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMkDirMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.RmDirResponse> rmDir(
        org.opendedup.grpc.RmDirRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRmDirMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.UnlinkResponse> unlink(
        org.opendedup.grpc.UnlinkRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getUnlinkMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.DataWriteResponse> write(
        org.opendedup.grpc.DataWriteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.DataReadResponse> read(
        org.opendedup.grpc.DataReadRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FileCloseResponse> release(
        org.opendedup.grpc.FileCloseRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.MkNodResponse> mknod(
        org.opendedup.grpc.MkNodRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMknodMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FileOpenResponse> open(
        org.opendedup.grpc.FileOpenRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FileMessageResponse> getFileInfo(
        org.opendedup.grpc.FileInfoRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetFileInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FileSnapshotResponse> createCopy(
        org.opendedup.grpc.FileSnapshotRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateCopyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FileExistsResponse> fileExists(
        org.opendedup.grpc.FileExistsRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getFileExistsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.MkDirResponse> mkDirAll(
        org.opendedup.grpc.MkDirRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getMkDirAllMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FileMessageResponse> stat(
        org.opendedup.grpc.FileInfoRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getStatMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FileRenameResponse> rename(
        org.opendedup.grpc.FileRenameRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getRenameMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.CopyExtentResponse> copyExtent(
        org.opendedup.grpc.CopyExtentRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCopyExtentMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SetUserMetaDataResponse> setUserMetaData(
        org.opendedup.grpc.SetUserMetaDataRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSetUserMetaDataMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.GetCloudFileResponse> getCloudFile(
        org.opendedup.grpc.GetCloudFileRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetCloudFileMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.GetCloudFileResponse> getCloudMetaFile(
        org.opendedup.grpc.GetCloudFileRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetCloudMetaFileMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.StatFSResponse> statFS(
        org.opendedup.grpc.StatFSRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getStatFSMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_XATTR_SIZE = 0;
  private static final int METHODID_FSYNC = 1;
  private static final int METHODID_SET_XATTR = 2;
  private static final int METHODID_REMOVE_XATTR = 3;
  private static final int METHODID_GET_XATTR = 4;
  private static final int METHODID_UTIME = 5;
  private static final int METHODID_TRUNCATE = 6;
  private static final int METHODID_SYM_LINK = 7;
  private static final int METHODID_READ_LINK = 8;
  private static final int METHODID_GET_ATTR = 9;
  private static final int METHODID_FLUSH = 10;
  private static final int METHODID_CHOWN = 11;
  private static final int METHODID_CHMOD = 12;
  private static final int METHODID_MK_DIR = 13;
  private static final int METHODID_RM_DIR = 14;
  private static final int METHODID_UNLINK = 15;
  private static final int METHODID_WRITE = 16;
  private static final int METHODID_READ = 17;
  private static final int METHODID_RELEASE = 18;
  private static final int METHODID_MKNOD = 19;
  private static final int METHODID_OPEN = 20;
  private static final int METHODID_GET_FILE_INFO = 21;
  private static final int METHODID_CREATE_COPY = 22;
  private static final int METHODID_FILE_EXISTS = 23;
  private static final int METHODID_MK_DIR_ALL = 24;
  private static final int METHODID_STAT = 25;
  private static final int METHODID_RENAME = 26;
  private static final int METHODID_COPY_EXTENT = 27;
  private static final int METHODID_SET_USER_META_DATA = 28;
  private static final int METHODID_GET_CLOUD_FILE = 29;
  private static final int METHODID_GET_CLOUD_META_FILE = 30;
  private static final int METHODID_STAT_FS = 31;
  private static final int METHODID_FILE_NOTIFICATION = 32;

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
        case METHODID_GET_XATTR_SIZE:
          serviceImpl.getXAttrSize((org.opendedup.grpc.GetXAttrSizeRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.GetXAttrSizeResponse>) responseObserver);
          break;
        case METHODID_FSYNC:
          serviceImpl.fsync((org.opendedup.grpc.FsyncRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FsyncResponse>) responseObserver);
          break;
        case METHODID_SET_XATTR:
          serviceImpl.setXAttr((org.opendedup.grpc.SetXAttrRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SetXAttrResponse>) responseObserver);
          break;
        case METHODID_REMOVE_XATTR:
          serviceImpl.removeXAttr((org.opendedup.grpc.RemoveXAttrRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.RemoveXAttrResponse>) responseObserver);
          break;
        case METHODID_GET_XATTR:
          serviceImpl.getXAttr((org.opendedup.grpc.GetXAttrRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.GetXAttrResponse>) responseObserver);
          break;
        case METHODID_UTIME:
          serviceImpl.utime((org.opendedup.grpc.UtimeRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.UtimeResponse>) responseObserver);
          break;
        case METHODID_TRUNCATE:
          serviceImpl.truncate((org.opendedup.grpc.TruncateRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.TruncateResponse>) responseObserver);
          break;
        case METHODID_SYM_LINK:
          serviceImpl.symLink((org.opendedup.grpc.SymLinkRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SymLinkResponse>) responseObserver);
          break;
        case METHODID_READ_LINK:
          serviceImpl.readLink((org.opendedup.grpc.LinkRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.LinkResponse>) responseObserver);
          break;
        case METHODID_GET_ATTR:
          serviceImpl.getAttr((org.opendedup.grpc.StatRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.StatResponse>) responseObserver);
          break;
        case METHODID_FLUSH:
          serviceImpl.flush((org.opendedup.grpc.FlushRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FlushResponse>) responseObserver);
          break;
        case METHODID_CHOWN:
          serviceImpl.chown((org.opendedup.grpc.ChownRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.ChownResponse>) responseObserver);
          break;
        case METHODID_CHMOD:
          serviceImpl.chmod((org.opendedup.grpc.ChmodRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.ChmodResponse>) responseObserver);
          break;
        case METHODID_MK_DIR:
          serviceImpl.mkDir((org.opendedup.grpc.MkDirRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.MkDirResponse>) responseObserver);
          break;
        case METHODID_RM_DIR:
          serviceImpl.rmDir((org.opendedup.grpc.RmDirRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.RmDirResponse>) responseObserver);
          break;
        case METHODID_UNLINK:
          serviceImpl.unlink((org.opendedup.grpc.UnlinkRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.UnlinkResponse>) responseObserver);
          break;
        case METHODID_WRITE:
          serviceImpl.write((org.opendedup.grpc.DataWriteRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.DataWriteResponse>) responseObserver);
          break;
        case METHODID_READ:
          serviceImpl.read((org.opendedup.grpc.DataReadRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.DataReadResponse>) responseObserver);
          break;
        case METHODID_RELEASE:
          serviceImpl.release((org.opendedup.grpc.FileCloseRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileCloseResponse>) responseObserver);
          break;
        case METHODID_MKNOD:
          serviceImpl.mknod((org.opendedup.grpc.MkNodRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.MkNodResponse>) responseObserver);
          break;
        case METHODID_OPEN:
          serviceImpl.open((org.opendedup.grpc.FileOpenRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileOpenResponse>) responseObserver);
          break;
        case METHODID_GET_FILE_INFO:
          serviceImpl.getFileInfo((org.opendedup.grpc.FileInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileMessageResponse>) responseObserver);
          break;
        case METHODID_CREATE_COPY:
          serviceImpl.createCopy((org.opendedup.grpc.FileSnapshotRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileSnapshotResponse>) responseObserver);
          break;
        case METHODID_FILE_EXISTS:
          serviceImpl.fileExists((org.opendedup.grpc.FileExistsRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileExistsResponse>) responseObserver);
          break;
        case METHODID_MK_DIR_ALL:
          serviceImpl.mkDirAll((org.opendedup.grpc.MkDirRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.MkDirResponse>) responseObserver);
          break;
        case METHODID_STAT:
          serviceImpl.stat((org.opendedup.grpc.FileInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileMessageResponse>) responseObserver);
          break;
        case METHODID_RENAME:
          serviceImpl.rename((org.opendedup.grpc.FileRenameRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileRenameResponse>) responseObserver);
          break;
        case METHODID_COPY_EXTENT:
          serviceImpl.copyExtent((org.opendedup.grpc.CopyExtentRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.CopyExtentResponse>) responseObserver);
          break;
        case METHODID_SET_USER_META_DATA:
          serviceImpl.setUserMetaData((org.opendedup.grpc.SetUserMetaDataRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SetUserMetaDataResponse>) responseObserver);
          break;
        case METHODID_GET_CLOUD_FILE:
          serviceImpl.getCloudFile((org.opendedup.grpc.GetCloudFileRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.GetCloudFileResponse>) responseObserver);
          break;
        case METHODID_GET_CLOUD_META_FILE:
          serviceImpl.getCloudMetaFile((org.opendedup.grpc.GetCloudFileRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.GetCloudFileResponse>) responseObserver);
          break;
        case METHODID_STAT_FS:
          serviceImpl.statFS((org.opendedup.grpc.StatFSRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.StatFSResponse>) responseObserver);
          break;
        case METHODID_FILE_NOTIFICATION:
          serviceImpl.fileNotification((org.opendedup.grpc.SyncNotificationSubscription) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileMessageResponse>) responseObserver);
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
              .addMethod(getGetXAttrSizeMethod())
              .addMethod(getFsyncMethod())
              .addMethod(getSetXAttrMethod())
              .addMethod(getRemoveXAttrMethod())
              .addMethod(getGetXAttrMethod())
              .addMethod(getUtimeMethod())
              .addMethod(getTruncateMethod())
              .addMethod(getSymLinkMethod())
              .addMethod(getReadLinkMethod())
              .addMethod(getGetAttrMethod())
              .addMethod(getFlushMethod())
              .addMethod(getChownMethod())
              .addMethod(getChmodMethod())
              .addMethod(getMkDirMethod())
              .addMethod(getRmDirMethod())
              .addMethod(getUnlinkMethod())
              .addMethod(getWriteMethod())
              .addMethod(getReadMethod())
              .addMethod(getReleaseMethod())
              .addMethod(getMknodMethod())
              .addMethod(getOpenMethod())
              .addMethod(getGetFileInfoMethod())
              .addMethod(getCreateCopyMethod())
              .addMethod(getFileExistsMethod())
              .addMethod(getMkDirAllMethod())
              .addMethod(getStatMethod())
              .addMethod(getRenameMethod())
              .addMethod(getCopyExtentMethod())
              .addMethod(getSetUserMetaDataMethod())
              .addMethod(getGetCloudFileMethod())
              .addMethod(getGetCloudMetaFileMethod())
              .addMethod(getStatFSMethod())
              .addMethod(getFileNotificationMethod())
              .build();
        }
      }
    }
    return result;
  }
}
