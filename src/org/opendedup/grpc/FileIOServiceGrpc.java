package org.opendedup.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.46.0)",
    comments = "Source: IOService.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class FileIOServiceGrpc {

  private FileIOServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.FileIOService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetXAttrSizeRequest,
      org.opendedup.grpc.IOService.GetXAttrSizeResponse> getGetXAttrSizeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetXAttrSize",
      requestType = org.opendedup.grpc.IOService.GetXAttrSizeRequest.class,
      responseType = org.opendedup.grpc.IOService.GetXAttrSizeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetXAttrSizeRequest,
      org.opendedup.grpc.IOService.GetXAttrSizeResponse> getGetXAttrSizeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetXAttrSizeRequest, org.opendedup.grpc.IOService.GetXAttrSizeResponse> getGetXAttrSizeMethod;
    if ((getGetXAttrSizeMethod = FileIOServiceGrpc.getGetXAttrSizeMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetXAttrSizeMethod = FileIOServiceGrpc.getGetXAttrSizeMethod) == null) {
          FileIOServiceGrpc.getGetXAttrSizeMethod = getGetXAttrSizeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.GetXAttrSizeRequest, org.opendedup.grpc.IOService.GetXAttrSizeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetXAttrSize"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetXAttrSizeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetXAttrSizeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetXAttrSize"))
              .build();
        }
      }
    }
    return getGetXAttrSizeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FsyncRequest,
      org.opendedup.grpc.IOService.FsyncResponse> getFsyncMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Fsync",
      requestType = org.opendedup.grpc.IOService.FsyncRequest.class,
      responseType = org.opendedup.grpc.IOService.FsyncResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FsyncRequest,
      org.opendedup.grpc.IOService.FsyncResponse> getFsyncMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FsyncRequest, org.opendedup.grpc.IOService.FsyncResponse> getFsyncMethod;
    if ((getFsyncMethod = FileIOServiceGrpc.getFsyncMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getFsyncMethod = FileIOServiceGrpc.getFsyncMethod) == null) {
          FileIOServiceGrpc.getFsyncMethod = getFsyncMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.FsyncRequest, org.opendedup.grpc.IOService.FsyncResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Fsync"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FsyncRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FsyncResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Fsync"))
              .build();
        }
      }
    }
    return getFsyncMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SetXAttrRequest,
      org.opendedup.grpc.IOService.SetXAttrResponse> getSetXAttrMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetXAttr",
      requestType = org.opendedup.grpc.IOService.SetXAttrRequest.class,
      responseType = org.opendedup.grpc.IOService.SetXAttrResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SetXAttrRequest,
      org.opendedup.grpc.IOService.SetXAttrResponse> getSetXAttrMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SetXAttrRequest, org.opendedup.grpc.IOService.SetXAttrResponse> getSetXAttrMethod;
    if ((getSetXAttrMethod = FileIOServiceGrpc.getSetXAttrMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getSetXAttrMethod = FileIOServiceGrpc.getSetXAttrMethod) == null) {
          FileIOServiceGrpc.getSetXAttrMethod = getSetXAttrMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.SetXAttrRequest, org.opendedup.grpc.IOService.SetXAttrResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetXAttr"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.SetXAttrRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.SetXAttrResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("SetXAttr"))
              .build();
        }
      }
    }
    return getSetXAttrMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.RemoveXAttrRequest,
      org.opendedup.grpc.IOService.RemoveXAttrResponse> getRemoveXAttrMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RemoveXAttr",
      requestType = org.opendedup.grpc.IOService.RemoveXAttrRequest.class,
      responseType = org.opendedup.grpc.IOService.RemoveXAttrResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.RemoveXAttrRequest,
      org.opendedup.grpc.IOService.RemoveXAttrResponse> getRemoveXAttrMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.RemoveXAttrRequest, org.opendedup.grpc.IOService.RemoveXAttrResponse> getRemoveXAttrMethod;
    if ((getRemoveXAttrMethod = FileIOServiceGrpc.getRemoveXAttrMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getRemoveXAttrMethod = FileIOServiceGrpc.getRemoveXAttrMethod) == null) {
          FileIOServiceGrpc.getRemoveXAttrMethod = getRemoveXAttrMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.RemoveXAttrRequest, org.opendedup.grpc.IOService.RemoveXAttrResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RemoveXAttr"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.RemoveXAttrRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.RemoveXAttrResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("RemoveXAttr"))
              .build();
        }
      }
    }
    return getRemoveXAttrMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetXAttrRequest,
      org.opendedup.grpc.IOService.GetXAttrResponse> getGetXAttrMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetXAttr",
      requestType = org.opendedup.grpc.IOService.GetXAttrRequest.class,
      responseType = org.opendedup.grpc.IOService.GetXAttrResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetXAttrRequest,
      org.opendedup.grpc.IOService.GetXAttrResponse> getGetXAttrMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetXAttrRequest, org.opendedup.grpc.IOService.GetXAttrResponse> getGetXAttrMethod;
    if ((getGetXAttrMethod = FileIOServiceGrpc.getGetXAttrMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetXAttrMethod = FileIOServiceGrpc.getGetXAttrMethod) == null) {
          FileIOServiceGrpc.getGetXAttrMethod = getGetXAttrMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.GetXAttrRequest, org.opendedup.grpc.IOService.GetXAttrResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetXAttr"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetXAttrRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetXAttrResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetXAttr"))
              .build();
        }
      }
    }
    return getGetXAttrMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.UtimeRequest,
      org.opendedup.grpc.IOService.UtimeResponse> getUtimeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Utime",
      requestType = org.opendedup.grpc.IOService.UtimeRequest.class,
      responseType = org.opendedup.grpc.IOService.UtimeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.UtimeRequest,
      org.opendedup.grpc.IOService.UtimeResponse> getUtimeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.UtimeRequest, org.opendedup.grpc.IOService.UtimeResponse> getUtimeMethod;
    if ((getUtimeMethod = FileIOServiceGrpc.getUtimeMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getUtimeMethod = FileIOServiceGrpc.getUtimeMethod) == null) {
          FileIOServiceGrpc.getUtimeMethod = getUtimeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.UtimeRequest, org.opendedup.grpc.IOService.UtimeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Utime"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.UtimeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.UtimeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Utime"))
              .build();
        }
      }
    }
    return getUtimeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.TruncateRequest,
      org.opendedup.grpc.IOService.TruncateResponse> getTruncateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Truncate",
      requestType = org.opendedup.grpc.IOService.TruncateRequest.class,
      responseType = org.opendedup.grpc.IOService.TruncateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.TruncateRequest,
      org.opendedup.grpc.IOService.TruncateResponse> getTruncateMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.TruncateRequest, org.opendedup.grpc.IOService.TruncateResponse> getTruncateMethod;
    if ((getTruncateMethod = FileIOServiceGrpc.getTruncateMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getTruncateMethod = FileIOServiceGrpc.getTruncateMethod) == null) {
          FileIOServiceGrpc.getTruncateMethod = getTruncateMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.TruncateRequest, org.opendedup.grpc.IOService.TruncateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Truncate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.TruncateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.TruncateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Truncate"))
              .build();
        }
      }
    }
    return getTruncateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SymLinkRequest,
      org.opendedup.grpc.IOService.SymLinkResponse> getSymLinkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SymLink",
      requestType = org.opendedup.grpc.IOService.SymLinkRequest.class,
      responseType = org.opendedup.grpc.IOService.SymLinkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SymLinkRequest,
      org.opendedup.grpc.IOService.SymLinkResponse> getSymLinkMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SymLinkRequest, org.opendedup.grpc.IOService.SymLinkResponse> getSymLinkMethod;
    if ((getSymLinkMethod = FileIOServiceGrpc.getSymLinkMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getSymLinkMethod = FileIOServiceGrpc.getSymLinkMethod) == null) {
          FileIOServiceGrpc.getSymLinkMethod = getSymLinkMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.SymLinkRequest, org.opendedup.grpc.IOService.SymLinkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SymLink"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.SymLinkRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.SymLinkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("SymLink"))
              .build();
        }
      }
    }
    return getSymLinkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.LinkRequest,
      org.opendedup.grpc.IOService.LinkResponse> getReadLinkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadLink",
      requestType = org.opendedup.grpc.IOService.LinkRequest.class,
      responseType = org.opendedup.grpc.IOService.LinkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.LinkRequest,
      org.opendedup.grpc.IOService.LinkResponse> getReadLinkMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.LinkRequest, org.opendedup.grpc.IOService.LinkResponse> getReadLinkMethod;
    if ((getReadLinkMethod = FileIOServiceGrpc.getReadLinkMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getReadLinkMethod = FileIOServiceGrpc.getReadLinkMethod) == null) {
          FileIOServiceGrpc.getReadLinkMethod = getReadLinkMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.LinkRequest, org.opendedup.grpc.IOService.LinkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadLink"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.LinkRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.LinkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("ReadLink"))
              .build();
        }
      }
    }
    return getReadLinkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.StatRequest,
      org.opendedup.grpc.IOService.StatResponse> getGetAttrMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAttr",
      requestType = org.opendedup.grpc.IOService.StatRequest.class,
      responseType = org.opendedup.grpc.IOService.StatResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.StatRequest,
      org.opendedup.grpc.IOService.StatResponse> getGetAttrMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.StatRequest, org.opendedup.grpc.IOService.StatResponse> getGetAttrMethod;
    if ((getGetAttrMethod = FileIOServiceGrpc.getGetAttrMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetAttrMethod = FileIOServiceGrpc.getGetAttrMethod) == null) {
          FileIOServiceGrpc.getGetAttrMethod = getGetAttrMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.StatRequest, org.opendedup.grpc.IOService.StatResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetAttr"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.StatRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.StatResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetAttr"))
              .build();
        }
      }
    }
    return getGetAttrMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FlushRequest,
      org.opendedup.grpc.IOService.FlushResponse> getFlushMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Flush",
      requestType = org.opendedup.grpc.IOService.FlushRequest.class,
      responseType = org.opendedup.grpc.IOService.FlushResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FlushRequest,
      org.opendedup.grpc.IOService.FlushResponse> getFlushMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FlushRequest, org.opendedup.grpc.IOService.FlushResponse> getFlushMethod;
    if ((getFlushMethod = FileIOServiceGrpc.getFlushMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getFlushMethod = FileIOServiceGrpc.getFlushMethod) == null) {
          FileIOServiceGrpc.getFlushMethod = getFlushMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.FlushRequest, org.opendedup.grpc.IOService.FlushResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Flush"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FlushRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FlushResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Flush"))
              .build();
        }
      }
    }
    return getFlushMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.ChownRequest,
      org.opendedup.grpc.IOService.ChownResponse> getChownMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Chown",
      requestType = org.opendedup.grpc.IOService.ChownRequest.class,
      responseType = org.opendedup.grpc.IOService.ChownResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.ChownRequest,
      org.opendedup.grpc.IOService.ChownResponse> getChownMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.ChownRequest, org.opendedup.grpc.IOService.ChownResponse> getChownMethod;
    if ((getChownMethod = FileIOServiceGrpc.getChownMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getChownMethod = FileIOServiceGrpc.getChownMethod) == null) {
          FileIOServiceGrpc.getChownMethod = getChownMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.ChownRequest, org.opendedup.grpc.IOService.ChownResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Chown"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.ChownRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.ChownResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Chown"))
              .build();
        }
      }
    }
    return getChownMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.ChmodRequest,
      org.opendedup.grpc.IOService.ChmodResponse> getChmodMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Chmod",
      requestType = org.opendedup.grpc.IOService.ChmodRequest.class,
      responseType = org.opendedup.grpc.IOService.ChmodResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.ChmodRequest,
      org.opendedup.grpc.IOService.ChmodResponse> getChmodMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.ChmodRequest, org.opendedup.grpc.IOService.ChmodResponse> getChmodMethod;
    if ((getChmodMethod = FileIOServiceGrpc.getChmodMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getChmodMethod = FileIOServiceGrpc.getChmodMethod) == null) {
          FileIOServiceGrpc.getChmodMethod = getChmodMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.ChmodRequest, org.opendedup.grpc.IOService.ChmodResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Chmod"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.ChmodRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.ChmodResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Chmod"))
              .build();
        }
      }
    }
    return getChmodMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.MkDirRequest,
      org.opendedup.grpc.IOService.MkDirResponse> getMkDirMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MkDir",
      requestType = org.opendedup.grpc.IOService.MkDirRequest.class,
      responseType = org.opendedup.grpc.IOService.MkDirResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.MkDirRequest,
      org.opendedup.grpc.IOService.MkDirResponse> getMkDirMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.MkDirRequest, org.opendedup.grpc.IOService.MkDirResponse> getMkDirMethod;
    if ((getMkDirMethod = FileIOServiceGrpc.getMkDirMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getMkDirMethod = FileIOServiceGrpc.getMkDirMethod) == null) {
          FileIOServiceGrpc.getMkDirMethod = getMkDirMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.MkDirRequest, org.opendedup.grpc.IOService.MkDirResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MkDir"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.MkDirRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.MkDirResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("MkDir"))
              .build();
        }
      }
    }
    return getMkDirMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.RmDirRequest,
      org.opendedup.grpc.IOService.RmDirResponse> getRmDirMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RmDir",
      requestType = org.opendedup.grpc.IOService.RmDirRequest.class,
      responseType = org.opendedup.grpc.IOService.RmDirResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.RmDirRequest,
      org.opendedup.grpc.IOService.RmDirResponse> getRmDirMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.RmDirRequest, org.opendedup.grpc.IOService.RmDirResponse> getRmDirMethod;
    if ((getRmDirMethod = FileIOServiceGrpc.getRmDirMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getRmDirMethod = FileIOServiceGrpc.getRmDirMethod) == null) {
          FileIOServiceGrpc.getRmDirMethod = getRmDirMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.RmDirRequest, org.opendedup.grpc.IOService.RmDirResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RmDir"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.RmDirRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.RmDirResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("RmDir"))
              .build();
        }
      }
    }
    return getRmDirMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.UnlinkRequest,
      org.opendedup.grpc.IOService.UnlinkResponse> getUnlinkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Unlink",
      requestType = org.opendedup.grpc.IOService.UnlinkRequest.class,
      responseType = org.opendedup.grpc.IOService.UnlinkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.UnlinkRequest,
      org.opendedup.grpc.IOService.UnlinkResponse> getUnlinkMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.UnlinkRequest, org.opendedup.grpc.IOService.UnlinkResponse> getUnlinkMethod;
    if ((getUnlinkMethod = FileIOServiceGrpc.getUnlinkMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getUnlinkMethod = FileIOServiceGrpc.getUnlinkMethod) == null) {
          FileIOServiceGrpc.getUnlinkMethod = getUnlinkMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.UnlinkRequest, org.opendedup.grpc.IOService.UnlinkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Unlink"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.UnlinkRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.UnlinkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Unlink"))
              .build();
        }
      }
    }
    return getUnlinkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.DataWriteRequest,
      org.opendedup.grpc.IOService.DataWriteResponse> getWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Write",
      requestType = org.opendedup.grpc.IOService.DataWriteRequest.class,
      responseType = org.opendedup.grpc.IOService.DataWriteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.DataWriteRequest,
      org.opendedup.grpc.IOService.DataWriteResponse> getWriteMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.DataWriteRequest, org.opendedup.grpc.IOService.DataWriteResponse> getWriteMethod;
    if ((getWriteMethod = FileIOServiceGrpc.getWriteMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getWriteMethod = FileIOServiceGrpc.getWriteMethod) == null) {
          FileIOServiceGrpc.getWriteMethod = getWriteMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.DataWriteRequest, org.opendedup.grpc.IOService.DataWriteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Write"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.DataWriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.DataWriteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Write"))
              .build();
        }
      }
    }
    return getWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.RawDataWriteRequest,
      org.opendedup.grpc.IOService.DataWriteResponse> getRawWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RawWrite",
      requestType = org.opendedup.grpc.IOService.RawDataWriteRequest.class,
      responseType = org.opendedup.grpc.IOService.DataWriteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.RawDataWriteRequest,
      org.opendedup.grpc.IOService.DataWriteResponse> getRawWriteMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.RawDataWriteRequest, org.opendedup.grpc.IOService.DataWriteResponse> getRawWriteMethod;
    if ((getRawWriteMethod = FileIOServiceGrpc.getRawWriteMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getRawWriteMethod = FileIOServiceGrpc.getRawWriteMethod) == null) {
          FileIOServiceGrpc.getRawWriteMethod = getRawWriteMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.RawDataWriteRequest, org.opendedup.grpc.IOService.DataWriteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RawWrite"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.RawDataWriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.DataWriteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("RawWrite"))
              .build();
        }
      }
    }
    return getRawWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.DataWriteRequest,
      org.opendedup.grpc.IOService.DataWriteResponse> getStreamWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StreamWrite",
      requestType = org.opendedup.grpc.IOService.DataWriteRequest.class,
      responseType = org.opendedup.grpc.IOService.DataWriteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.DataWriteRequest,
      org.opendedup.grpc.IOService.DataWriteResponse> getStreamWriteMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.DataWriteRequest, org.opendedup.grpc.IOService.DataWriteResponse> getStreamWriteMethod;
    if ((getStreamWriteMethod = FileIOServiceGrpc.getStreamWriteMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getStreamWriteMethod = FileIOServiceGrpc.getStreamWriteMethod) == null) {
          FileIOServiceGrpc.getStreamWriteMethod = getStreamWriteMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.DataWriteRequest, org.opendedup.grpc.IOService.DataWriteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StreamWrite"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.DataWriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.DataWriteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("StreamWrite"))
              .build();
        }
      }
    }
    return getStreamWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.DataReadRequest,
      org.opendedup.grpc.IOService.DataReadResponse> getReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Read",
      requestType = org.opendedup.grpc.IOService.DataReadRequest.class,
      responseType = org.opendedup.grpc.IOService.DataReadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.DataReadRequest,
      org.opendedup.grpc.IOService.DataReadResponse> getReadMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.DataReadRequest, org.opendedup.grpc.IOService.DataReadResponse> getReadMethod;
    if ((getReadMethod = FileIOServiceGrpc.getReadMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getReadMethod = FileIOServiceGrpc.getReadMethod) == null) {
          FileIOServiceGrpc.getReadMethod = getReadMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.DataReadRequest, org.opendedup.grpc.IOService.DataReadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Read"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.DataReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.DataReadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Read"))
              .build();
        }
      }
    }
    return getReadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileCloseRequest,
      org.opendedup.grpc.IOService.FileCloseResponse> getReleaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Release",
      requestType = org.opendedup.grpc.IOService.FileCloseRequest.class,
      responseType = org.opendedup.grpc.IOService.FileCloseResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileCloseRequest,
      org.opendedup.grpc.IOService.FileCloseResponse> getReleaseMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileCloseRequest, org.opendedup.grpc.IOService.FileCloseResponse> getReleaseMethod;
    if ((getReleaseMethod = FileIOServiceGrpc.getReleaseMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getReleaseMethod = FileIOServiceGrpc.getReleaseMethod) == null) {
          FileIOServiceGrpc.getReleaseMethod = getReleaseMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.FileCloseRequest, org.opendedup.grpc.IOService.FileCloseResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Release"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileCloseRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileCloseResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Release"))
              .build();
        }
      }
    }
    return getReleaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.MkNodRequest,
      org.opendedup.grpc.IOService.MkNodResponse> getMknodMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Mknod",
      requestType = org.opendedup.grpc.IOService.MkNodRequest.class,
      responseType = org.opendedup.grpc.IOService.MkNodResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.MkNodRequest,
      org.opendedup.grpc.IOService.MkNodResponse> getMknodMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.MkNodRequest, org.opendedup.grpc.IOService.MkNodResponse> getMknodMethod;
    if ((getMknodMethod = FileIOServiceGrpc.getMknodMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getMknodMethod = FileIOServiceGrpc.getMknodMethod) == null) {
          FileIOServiceGrpc.getMknodMethod = getMknodMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.MkNodRequest, org.opendedup.grpc.IOService.MkNodResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Mknod"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.MkNodRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.MkNodResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Mknod"))
              .build();
        }
      }
    }
    return getMknodMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileOpenRequest,
      org.opendedup.grpc.IOService.FileOpenResponse> getOpenMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Open",
      requestType = org.opendedup.grpc.IOService.FileOpenRequest.class,
      responseType = org.opendedup.grpc.IOService.FileOpenResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileOpenRequest,
      org.opendedup.grpc.IOService.FileOpenResponse> getOpenMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileOpenRequest, org.opendedup.grpc.IOService.FileOpenResponse> getOpenMethod;
    if ((getOpenMethod = FileIOServiceGrpc.getOpenMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getOpenMethod = FileIOServiceGrpc.getOpenMethod) == null) {
          FileIOServiceGrpc.getOpenMethod = getOpenMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.FileOpenRequest, org.opendedup.grpc.IOService.FileOpenResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Open"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileOpenRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileOpenResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Open"))
              .build();
        }
      }
    }
    return getOpenMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfo.FileInfoRequest,
      org.opendedup.grpc.FileInfo.FileMessageResponse> getGetFileInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetFileInfo",
      requestType = org.opendedup.grpc.FileInfo.FileInfoRequest.class,
      responseType = org.opendedup.grpc.FileInfo.FileMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfo.FileInfoRequest,
      org.opendedup.grpc.FileInfo.FileMessageResponse> getGetFileInfoMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfo.FileInfoRequest, org.opendedup.grpc.FileInfo.FileMessageResponse> getGetFileInfoMethod;
    if ((getGetFileInfoMethod = FileIOServiceGrpc.getGetFileInfoMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetFileInfoMethod = FileIOServiceGrpc.getGetFileInfoMethod) == null) {
          FileIOServiceGrpc.getGetFileInfoMethod = getGetFileInfoMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FileInfo.FileInfoRequest, org.opendedup.grpc.FileInfo.FileMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetFileInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileInfo.FileInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileInfo.FileMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetFileInfo"))
              .build();
        }
      }
    }
    return getGetFileInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileSnapshotRequest,
      org.opendedup.grpc.IOService.FileSnapshotResponse> getCreateCopyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateCopy",
      requestType = org.opendedup.grpc.IOService.FileSnapshotRequest.class,
      responseType = org.opendedup.grpc.IOService.FileSnapshotResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileSnapshotRequest,
      org.opendedup.grpc.IOService.FileSnapshotResponse> getCreateCopyMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileSnapshotRequest, org.opendedup.grpc.IOService.FileSnapshotResponse> getCreateCopyMethod;
    if ((getCreateCopyMethod = FileIOServiceGrpc.getCreateCopyMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getCreateCopyMethod = FileIOServiceGrpc.getCreateCopyMethod) == null) {
          FileIOServiceGrpc.getCreateCopyMethod = getCreateCopyMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.FileSnapshotRequest, org.opendedup.grpc.IOService.FileSnapshotResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateCopy"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileSnapshotRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileSnapshotResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("CreateCopy"))
              .build();
        }
      }
    }
    return getCreateCopyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileExistsRequest,
      org.opendedup.grpc.IOService.FileExistsResponse> getFileExistsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "FileExists",
      requestType = org.opendedup.grpc.IOService.FileExistsRequest.class,
      responseType = org.opendedup.grpc.IOService.FileExistsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileExistsRequest,
      org.opendedup.grpc.IOService.FileExistsResponse> getFileExistsMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileExistsRequest, org.opendedup.grpc.IOService.FileExistsResponse> getFileExistsMethod;
    if ((getFileExistsMethod = FileIOServiceGrpc.getFileExistsMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getFileExistsMethod = FileIOServiceGrpc.getFileExistsMethod) == null) {
          FileIOServiceGrpc.getFileExistsMethod = getFileExistsMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.FileExistsRequest, org.opendedup.grpc.IOService.FileExistsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "FileExists"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileExistsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileExistsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("FileExists"))
              .build();
        }
      }
    }
    return getFileExistsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.MkDirRequest,
      org.opendedup.grpc.IOService.MkDirResponse> getMkDirAllMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MkDirAll",
      requestType = org.opendedup.grpc.IOService.MkDirRequest.class,
      responseType = org.opendedup.grpc.IOService.MkDirResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.MkDirRequest,
      org.opendedup.grpc.IOService.MkDirResponse> getMkDirAllMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.MkDirRequest, org.opendedup.grpc.IOService.MkDirResponse> getMkDirAllMethod;
    if ((getMkDirAllMethod = FileIOServiceGrpc.getMkDirAllMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getMkDirAllMethod = FileIOServiceGrpc.getMkDirAllMethod) == null) {
          FileIOServiceGrpc.getMkDirAllMethod = getMkDirAllMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.MkDirRequest, org.opendedup.grpc.IOService.MkDirResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MkDirAll"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.MkDirRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.MkDirResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("MkDirAll"))
              .build();
        }
      }
    }
    return getMkDirAllMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfo.FileInfoRequest,
      org.opendedup.grpc.FileInfo.FileMessageResponse> getStatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Stat",
      requestType = org.opendedup.grpc.FileInfo.FileInfoRequest.class,
      responseType = org.opendedup.grpc.FileInfo.FileMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfo.FileInfoRequest,
      org.opendedup.grpc.FileInfo.FileMessageResponse> getStatMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.FileInfo.FileInfoRequest, org.opendedup.grpc.FileInfo.FileMessageResponse> getStatMethod;
    if ((getStatMethod = FileIOServiceGrpc.getStatMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getStatMethod = FileIOServiceGrpc.getStatMethod) == null) {
          FileIOServiceGrpc.getStatMethod = getStatMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.FileInfo.FileInfoRequest, org.opendedup.grpc.FileInfo.FileMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Stat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileInfo.FileInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileInfo.FileMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Stat"))
              .build();
        }
      }
    }
    return getStatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileRenameRequest,
      org.opendedup.grpc.IOService.FileRenameResponse> getRenameMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Rename",
      requestType = org.opendedup.grpc.IOService.FileRenameRequest.class,
      responseType = org.opendedup.grpc.IOService.FileRenameResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileRenameRequest,
      org.opendedup.grpc.IOService.FileRenameResponse> getRenameMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.FileRenameRequest, org.opendedup.grpc.IOService.FileRenameResponse> getRenameMethod;
    if ((getRenameMethod = FileIOServiceGrpc.getRenameMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getRenameMethod = FileIOServiceGrpc.getRenameMethod) == null) {
          FileIOServiceGrpc.getRenameMethod = getRenameMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.FileRenameRequest, org.opendedup.grpc.IOService.FileRenameResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Rename"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileRenameRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.FileRenameResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("Rename"))
              .build();
        }
      }
    }
    return getRenameMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.CopyExtentRequest,
      org.opendedup.grpc.IOService.CopyExtentResponse> getCopyExtentMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CopyExtent",
      requestType = org.opendedup.grpc.IOService.CopyExtentRequest.class,
      responseType = org.opendedup.grpc.IOService.CopyExtentResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.CopyExtentRequest,
      org.opendedup.grpc.IOService.CopyExtentResponse> getCopyExtentMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.CopyExtentRequest, org.opendedup.grpc.IOService.CopyExtentResponse> getCopyExtentMethod;
    if ((getCopyExtentMethod = FileIOServiceGrpc.getCopyExtentMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getCopyExtentMethod = FileIOServiceGrpc.getCopyExtentMethod) == null) {
          FileIOServiceGrpc.getCopyExtentMethod = getCopyExtentMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.CopyExtentRequest, org.opendedup.grpc.IOService.CopyExtentResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CopyExtent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.CopyExtentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.CopyExtentResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("CopyExtent"))
              .build();
        }
      }
    }
    return getCopyExtentMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SetUserMetaDataRequest,
      org.opendedup.grpc.IOService.SetUserMetaDataResponse> getSetUserMetaDataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetUserMetaData",
      requestType = org.opendedup.grpc.IOService.SetUserMetaDataRequest.class,
      responseType = org.opendedup.grpc.IOService.SetUserMetaDataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SetUserMetaDataRequest,
      org.opendedup.grpc.IOService.SetUserMetaDataResponse> getSetUserMetaDataMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SetUserMetaDataRequest, org.opendedup.grpc.IOService.SetUserMetaDataResponse> getSetUserMetaDataMethod;
    if ((getSetUserMetaDataMethod = FileIOServiceGrpc.getSetUserMetaDataMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getSetUserMetaDataMethod = FileIOServiceGrpc.getSetUserMetaDataMethod) == null) {
          FileIOServiceGrpc.getSetUserMetaDataMethod = getSetUserMetaDataMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.SetUserMetaDataRequest, org.opendedup.grpc.IOService.SetUserMetaDataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetUserMetaData"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.SetUserMetaDataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.SetUserMetaDataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("SetUserMetaData"))
              .build();
        }
      }
    }
    return getSetUserMetaDataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetCloudFileRequest,
      org.opendedup.grpc.IOService.GetCloudFileResponse> getGetCloudFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCloudFile",
      requestType = org.opendedup.grpc.IOService.GetCloudFileRequest.class,
      responseType = org.opendedup.grpc.IOService.GetCloudFileResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetCloudFileRequest,
      org.opendedup.grpc.IOService.GetCloudFileResponse> getGetCloudFileMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetCloudFileRequest, org.opendedup.grpc.IOService.GetCloudFileResponse> getGetCloudFileMethod;
    if ((getGetCloudFileMethod = FileIOServiceGrpc.getGetCloudFileMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetCloudFileMethod = FileIOServiceGrpc.getGetCloudFileMethod) == null) {
          FileIOServiceGrpc.getGetCloudFileMethod = getGetCloudFileMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.GetCloudFileRequest, org.opendedup.grpc.IOService.GetCloudFileResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCloudFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetCloudFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetCloudFileResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetCloudFile"))
              .build();
        }
      }
    }
    return getGetCloudFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetCloudFileRequest,
      org.opendedup.grpc.IOService.GetCloudFileResponse> getGetCloudMetaFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCloudMetaFile",
      requestType = org.opendedup.grpc.IOService.GetCloudFileRequest.class,
      responseType = org.opendedup.grpc.IOService.GetCloudFileResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetCloudFileRequest,
      org.opendedup.grpc.IOService.GetCloudFileResponse> getGetCloudMetaFileMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetCloudFileRequest, org.opendedup.grpc.IOService.GetCloudFileResponse> getGetCloudMetaFileMethod;
    if ((getGetCloudMetaFileMethod = FileIOServiceGrpc.getGetCloudMetaFileMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetCloudMetaFileMethod = FileIOServiceGrpc.getGetCloudMetaFileMethod) == null) {
          FileIOServiceGrpc.getGetCloudMetaFileMethod = getGetCloudMetaFileMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.GetCloudFileRequest, org.opendedup.grpc.IOService.GetCloudFileResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCloudMetaFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetCloudFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetCloudFileResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetCloudMetaFile"))
              .build();
        }
      }
    }
    return getGetCloudMetaFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.StatFSRequest,
      org.opendedup.grpc.IOService.StatFSResponse> getStatFSMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StatFS",
      requestType = org.opendedup.grpc.IOService.StatFSRequest.class,
      responseType = org.opendedup.grpc.IOService.StatFSResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.StatFSRequest,
      org.opendedup.grpc.IOService.StatFSResponse> getStatFSMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.StatFSRequest, org.opendedup.grpc.IOService.StatFSResponse> getStatFSMethod;
    if ((getStatFSMethod = FileIOServiceGrpc.getStatFSMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getStatFSMethod = FileIOServiceGrpc.getStatFSMethod) == null) {
          FileIOServiceGrpc.getStatFSMethod = getStatFSMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.StatFSRequest, org.opendedup.grpc.IOService.StatFSResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StatFS"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.StatFSRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.StatFSResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("StatFS"))
              .build();
        }
      }
    }
    return getStatFSMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SyncNotificationSubscription,
      org.opendedup.grpc.FileInfo.FileMessageResponse> getFileNotificationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "fileNotification",
      requestType = org.opendedup.grpc.IOService.SyncNotificationSubscription.class,
      responseType = org.opendedup.grpc.FileInfo.FileMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SyncNotificationSubscription,
      org.opendedup.grpc.FileInfo.FileMessageResponse> getFileNotificationMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SyncNotificationSubscription, org.opendedup.grpc.FileInfo.FileMessageResponse> getFileNotificationMethod;
    if ((getFileNotificationMethod = FileIOServiceGrpc.getFileNotificationMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getFileNotificationMethod = FileIOServiceGrpc.getFileNotificationMethod) == null) {
          FileIOServiceGrpc.getFileNotificationMethod = getFileNotificationMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.SyncNotificationSubscription, org.opendedup.grpc.FileInfo.FileMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "fileNotification"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.SyncNotificationSubscription.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.FileInfo.FileMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("fileNotification"))
              .build();
        }
      }
    }
    return getFileNotificationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SetRetrievalTierRequest,
      org.opendedup.grpc.IOService.SetRetrievalTierResponse> getSetRetrievalTierMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetRetrievalTier",
      requestType = org.opendedup.grpc.IOService.SetRetrievalTierRequest.class,
      responseType = org.opendedup.grpc.IOService.SetRetrievalTierResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SetRetrievalTierRequest,
      org.opendedup.grpc.IOService.SetRetrievalTierResponse> getSetRetrievalTierMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.SetRetrievalTierRequest, org.opendedup.grpc.IOService.SetRetrievalTierResponse> getSetRetrievalTierMethod;
    if ((getSetRetrievalTierMethod = FileIOServiceGrpc.getSetRetrievalTierMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getSetRetrievalTierMethod = FileIOServiceGrpc.getSetRetrievalTierMethod) == null) {
          FileIOServiceGrpc.getSetRetrievalTierMethod = getSetRetrievalTierMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.SetRetrievalTierRequest, org.opendedup.grpc.IOService.SetRetrievalTierResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetRetrievalTier"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.SetRetrievalTierRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.SetRetrievalTierResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("SetRetrievalTier"))
              .build();
        }
      }
    }
    return getSetRetrievalTierMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetRetrievalTierRequest,
      org.opendedup.grpc.IOService.GetRetrievalTierResponse> getGetRetrievalTierMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetRetrievalTier",
      requestType = org.opendedup.grpc.IOService.GetRetrievalTierRequest.class,
      responseType = org.opendedup.grpc.IOService.GetRetrievalTierResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetRetrievalTierRequest,
      org.opendedup.grpc.IOService.GetRetrievalTierResponse> getGetRetrievalTierMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.IOService.GetRetrievalTierRequest, org.opendedup.grpc.IOService.GetRetrievalTierResponse> getGetRetrievalTierMethod;
    if ((getGetRetrievalTierMethod = FileIOServiceGrpc.getGetRetrievalTierMethod) == null) {
      synchronized (FileIOServiceGrpc.class) {
        if ((getGetRetrievalTierMethod = FileIOServiceGrpc.getGetRetrievalTierMethod) == null) {
          FileIOServiceGrpc.getGetRetrievalTierMethod = getGetRetrievalTierMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.IOService.GetRetrievalTierRequest, org.opendedup.grpc.IOService.GetRetrievalTierResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetRetrievalTier"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetRetrievalTierRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.IOService.GetRetrievalTierResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileIOServiceMethodDescriptorSupplier("GetRetrievalTier"))
              .build();
        }
      }
    }
    return getGetRetrievalTierMethod;
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
    public void getXAttrSize(org.opendedup.grpc.IOService.GetXAttrSizeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetXAttrSizeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetXAttrSizeMethod(), responseObserver);
    }

    /**
     */
    public void fsync(org.opendedup.grpc.IOService.FsyncRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FsyncResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFsyncMethod(), responseObserver);
    }

    /**
     */
    public void setXAttr(org.opendedup.grpc.IOService.SetXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SetXAttrResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetXAttrMethod(), responseObserver);
    }

    /**
     */
    public void removeXAttr(org.opendedup.grpc.IOService.RemoveXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.RemoveXAttrResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRemoveXAttrMethod(), responseObserver);
    }

    /**
     */
    public void getXAttr(org.opendedup.grpc.IOService.GetXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetXAttrResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetXAttrMethod(), responseObserver);
    }

    /**
     */
    public void utime(org.opendedup.grpc.IOService.UtimeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.UtimeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUtimeMethod(), responseObserver);
    }

    /**
     */
    public void truncate(org.opendedup.grpc.IOService.TruncateRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.TruncateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTruncateMethod(), responseObserver);
    }

    /**
     */
    public void symLink(org.opendedup.grpc.IOService.SymLinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SymLinkResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSymLinkMethod(), responseObserver);
    }

    /**
     */
    public void readLink(org.opendedup.grpc.IOService.LinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.LinkResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadLinkMethod(), responseObserver);
    }

    /**
     */
    public void getAttr(org.opendedup.grpc.IOService.StatRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.StatResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetAttrMethod(), responseObserver);
    }

    /**
     */
    public void flush(org.opendedup.grpc.IOService.FlushRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FlushResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFlushMethod(), responseObserver);
    }

    /**
     */
    public void chown(org.opendedup.grpc.IOService.ChownRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.ChownResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getChownMethod(), responseObserver);
    }

    /**
     */
    public void chmod(org.opendedup.grpc.IOService.ChmodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.ChmodResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getChmodMethod(), responseObserver);
    }

    /**
     */
    public void mkDir(org.opendedup.grpc.IOService.MkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.MkDirResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMkDirMethod(), responseObserver);
    }

    /**
     */
    public void rmDir(org.opendedup.grpc.IOService.RmDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.RmDirResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRmDirMethod(), responseObserver);
    }

    /**
     */
    public void unlink(org.opendedup.grpc.IOService.UnlinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.UnlinkResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUnlinkMethod(), responseObserver);
    }

    /**
     */
    public void write(org.opendedup.grpc.IOService.DataWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWriteMethod(), responseObserver);
    }

    /**
     */
    public void rawWrite(org.opendedup.grpc.IOService.RawDataWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRawWriteMethod(), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteRequest> streamWrite(
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getStreamWriteMethod(), responseObserver);
    }

    /**
     */
    public void read(org.opendedup.grpc.IOService.DataReadRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataReadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadMethod(), responseObserver);
    }

    /**
     */
    public void release(org.opendedup.grpc.IOService.FileCloseRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileCloseResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReleaseMethod(), responseObserver);
    }

    /**
     */
    public void mknod(org.opendedup.grpc.IOService.MkNodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.MkNodResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMknodMethod(), responseObserver);
    }

    /**
     */
    public void open(org.opendedup.grpc.IOService.FileOpenRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileOpenResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getOpenMethod(), responseObserver);
    }

    /**
     */
    public void getFileInfo(org.opendedup.grpc.FileInfo.FileInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileInfo.FileMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetFileInfoMethod(), responseObserver);
    }

    /**
     */
    public void createCopy(org.opendedup.grpc.IOService.FileSnapshotRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileSnapshotResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateCopyMethod(), responseObserver);
    }

    /**
     */
    public void fileExists(org.opendedup.grpc.IOService.FileExistsRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileExistsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFileExistsMethod(), responseObserver);
    }

    /**
     */
    public void mkDirAll(org.opendedup.grpc.IOService.MkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.MkDirResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMkDirAllMethod(), responseObserver);
    }

    /**
     */
    public void stat(org.opendedup.grpc.FileInfo.FileInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileInfo.FileMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStatMethod(), responseObserver);
    }

    /**
     */
    public void rename(org.opendedup.grpc.IOService.FileRenameRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileRenameResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRenameMethod(), responseObserver);
    }

    /**
     */
    public void copyExtent(org.opendedup.grpc.IOService.CopyExtentRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.CopyExtentResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCopyExtentMethod(), responseObserver);
    }

    /**
     */
    public void setUserMetaData(org.opendedup.grpc.IOService.SetUserMetaDataRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SetUserMetaDataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetUserMetaDataMethod(), responseObserver);
    }

    /**
     */
    public void getCloudFile(org.opendedup.grpc.IOService.GetCloudFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetCloudFileResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCloudFileMethod(), responseObserver);
    }

    /**
     */
    public void getCloudMetaFile(org.opendedup.grpc.IOService.GetCloudFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetCloudFileResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCloudMetaFileMethod(), responseObserver);
    }

    /**
     */
    public void statFS(org.opendedup.grpc.IOService.StatFSRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.StatFSResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStatFSMethod(), responseObserver);
    }

    /**
     */
    public void fileNotification(org.opendedup.grpc.IOService.SyncNotificationSubscription request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileInfo.FileMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFileNotificationMethod(), responseObserver);
    }

    /**
     */
    public void setRetrievalTier(org.opendedup.grpc.IOService.SetRetrievalTierRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SetRetrievalTierResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetRetrievalTierMethod(), responseObserver);
    }

    /**
     */
    public void getRetrievalTier(org.opendedup.grpc.IOService.GetRetrievalTierRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetRetrievalTierResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetRetrievalTierMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetXAttrSizeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.GetXAttrSizeRequest,
                org.opendedup.grpc.IOService.GetXAttrSizeResponse>(
                  this, METHODID_GET_XATTR_SIZE)))
          .addMethod(
            getFsyncMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.FsyncRequest,
                org.opendedup.grpc.IOService.FsyncResponse>(
                  this, METHODID_FSYNC)))
          .addMethod(
            getSetXAttrMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.SetXAttrRequest,
                org.opendedup.grpc.IOService.SetXAttrResponse>(
                  this, METHODID_SET_XATTR)))
          .addMethod(
            getRemoveXAttrMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.RemoveXAttrRequest,
                org.opendedup.grpc.IOService.RemoveXAttrResponse>(
                  this, METHODID_REMOVE_XATTR)))
          .addMethod(
            getGetXAttrMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.GetXAttrRequest,
                org.opendedup.grpc.IOService.GetXAttrResponse>(
                  this, METHODID_GET_XATTR)))
          .addMethod(
            getUtimeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.UtimeRequest,
                org.opendedup.grpc.IOService.UtimeResponse>(
                  this, METHODID_UTIME)))
          .addMethod(
            getTruncateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.TruncateRequest,
                org.opendedup.grpc.IOService.TruncateResponse>(
                  this, METHODID_TRUNCATE)))
          .addMethod(
            getSymLinkMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.SymLinkRequest,
                org.opendedup.grpc.IOService.SymLinkResponse>(
                  this, METHODID_SYM_LINK)))
          .addMethod(
            getReadLinkMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.LinkRequest,
                org.opendedup.grpc.IOService.LinkResponse>(
                  this, METHODID_READ_LINK)))
          .addMethod(
            getGetAttrMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.StatRequest,
                org.opendedup.grpc.IOService.StatResponse>(
                  this, METHODID_GET_ATTR)))
          .addMethod(
            getFlushMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.FlushRequest,
                org.opendedup.grpc.IOService.FlushResponse>(
                  this, METHODID_FLUSH)))
          .addMethod(
            getChownMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.ChownRequest,
                org.opendedup.grpc.IOService.ChownResponse>(
                  this, METHODID_CHOWN)))
          .addMethod(
            getChmodMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.ChmodRequest,
                org.opendedup.grpc.IOService.ChmodResponse>(
                  this, METHODID_CHMOD)))
          .addMethod(
            getMkDirMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.MkDirRequest,
                org.opendedup.grpc.IOService.MkDirResponse>(
                  this, METHODID_MK_DIR)))
          .addMethod(
            getRmDirMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.RmDirRequest,
                org.opendedup.grpc.IOService.RmDirResponse>(
                  this, METHODID_RM_DIR)))
          .addMethod(
            getUnlinkMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.UnlinkRequest,
                org.opendedup.grpc.IOService.UnlinkResponse>(
                  this, METHODID_UNLINK)))
          .addMethod(
            getWriteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.DataWriteRequest,
                org.opendedup.grpc.IOService.DataWriteResponse>(
                  this, METHODID_WRITE)))
          .addMethod(
            getRawWriteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.RawDataWriteRequest,
                org.opendedup.grpc.IOService.DataWriteResponse>(
                  this, METHODID_RAW_WRITE)))
          .addMethod(
            getStreamWriteMethod(),
            io.grpc.stub.ServerCalls.asyncClientStreamingCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.DataWriteRequest,
                org.opendedup.grpc.IOService.DataWriteResponse>(
                  this, METHODID_STREAM_WRITE)))
          .addMethod(
            getReadMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.DataReadRequest,
                org.opendedup.grpc.IOService.DataReadResponse>(
                  this, METHODID_READ)))
          .addMethod(
            getReleaseMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.FileCloseRequest,
                org.opendedup.grpc.IOService.FileCloseResponse>(
                  this, METHODID_RELEASE)))
          .addMethod(
            getMknodMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.MkNodRequest,
                org.opendedup.grpc.IOService.MkNodResponse>(
                  this, METHODID_MKNOD)))
          .addMethod(
            getOpenMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.FileOpenRequest,
                org.opendedup.grpc.IOService.FileOpenResponse>(
                  this, METHODID_OPEN)))
          .addMethod(
            getGetFileInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FileInfo.FileInfoRequest,
                org.opendedup.grpc.FileInfo.FileMessageResponse>(
                  this, METHODID_GET_FILE_INFO)))
          .addMethod(
            getCreateCopyMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.FileSnapshotRequest,
                org.opendedup.grpc.IOService.FileSnapshotResponse>(
                  this, METHODID_CREATE_COPY)))
          .addMethod(
            getFileExistsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.FileExistsRequest,
                org.opendedup.grpc.IOService.FileExistsResponse>(
                  this, METHODID_FILE_EXISTS)))
          .addMethod(
            getMkDirAllMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.MkDirRequest,
                org.opendedup.grpc.IOService.MkDirResponse>(
                  this, METHODID_MK_DIR_ALL)))
          .addMethod(
            getStatMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.FileInfo.FileInfoRequest,
                org.opendedup.grpc.FileInfo.FileMessageResponse>(
                  this, METHODID_STAT)))
          .addMethod(
            getRenameMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.FileRenameRequest,
                org.opendedup.grpc.IOService.FileRenameResponse>(
                  this, METHODID_RENAME)))
          .addMethod(
            getCopyExtentMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.CopyExtentRequest,
                org.opendedup.grpc.IOService.CopyExtentResponse>(
                  this, METHODID_COPY_EXTENT)))
          .addMethod(
            getSetUserMetaDataMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.SetUserMetaDataRequest,
                org.opendedup.grpc.IOService.SetUserMetaDataResponse>(
                  this, METHODID_SET_USER_META_DATA)))
          .addMethod(
            getGetCloudFileMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.GetCloudFileRequest,
                org.opendedup.grpc.IOService.GetCloudFileResponse>(
                  this, METHODID_GET_CLOUD_FILE)))
          .addMethod(
            getGetCloudMetaFileMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.GetCloudFileRequest,
                org.opendedup.grpc.IOService.GetCloudFileResponse>(
                  this, METHODID_GET_CLOUD_META_FILE)))
          .addMethod(
            getStatFSMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.StatFSRequest,
                org.opendedup.grpc.IOService.StatFSResponse>(
                  this, METHODID_STAT_FS)))
          .addMethod(
            getFileNotificationMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.SyncNotificationSubscription,
                org.opendedup.grpc.FileInfo.FileMessageResponse>(
                  this, METHODID_FILE_NOTIFICATION)))
          .addMethod(
            getSetRetrievalTierMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.SetRetrievalTierRequest,
                org.opendedup.grpc.IOService.SetRetrievalTierResponse>(
                  this, METHODID_SET_RETRIEVAL_TIER)))
          .addMethod(
            getGetRetrievalTierMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.IOService.GetRetrievalTierRequest,
                org.opendedup.grpc.IOService.GetRetrievalTierResponse>(
                  this, METHODID_GET_RETRIEVAL_TIER)))
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
    public void getXAttrSize(org.opendedup.grpc.IOService.GetXAttrSizeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetXAttrSizeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetXAttrSizeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void fsync(org.opendedup.grpc.IOService.FsyncRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FsyncResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFsyncMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setXAttr(org.opendedup.grpc.IOService.SetXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SetXAttrResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetXAttrMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void removeXAttr(org.opendedup.grpc.IOService.RemoveXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.RemoveXAttrResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRemoveXAttrMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getXAttr(org.opendedup.grpc.IOService.GetXAttrRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetXAttrResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetXAttrMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void utime(org.opendedup.grpc.IOService.UtimeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.UtimeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUtimeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void truncate(org.opendedup.grpc.IOService.TruncateRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.TruncateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTruncateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void symLink(org.opendedup.grpc.IOService.SymLinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SymLinkResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSymLinkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void readLink(org.opendedup.grpc.IOService.LinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.LinkResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadLinkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getAttr(org.opendedup.grpc.IOService.StatRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.StatResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetAttrMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void flush(org.opendedup.grpc.IOService.FlushRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FlushResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFlushMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void chown(org.opendedup.grpc.IOService.ChownRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.ChownResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getChownMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void chmod(org.opendedup.grpc.IOService.ChmodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.ChmodResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getChmodMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void mkDir(org.opendedup.grpc.IOService.MkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.MkDirResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMkDirMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rmDir(org.opendedup.grpc.IOService.RmDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.RmDirResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRmDirMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void unlink(org.opendedup.grpc.IOService.UnlinkRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.UnlinkResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUnlinkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void write(org.opendedup.grpc.IOService.DataWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rawWrite(org.opendedup.grpc.IOService.RawDataWriteRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRawWriteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteRequest> streamWrite(
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncClientStreamingCall(
          getChannel().newCall(getStreamWriteMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void read(org.opendedup.grpc.IOService.DataReadRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataReadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void release(org.opendedup.grpc.IOService.FileCloseRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileCloseResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void mknod(org.opendedup.grpc.IOService.MkNodRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.MkNodResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMknodMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void open(org.opendedup.grpc.IOService.FileOpenRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileOpenResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getFileInfo(org.opendedup.grpc.FileInfo.FileInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileInfo.FileMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetFileInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createCopy(org.opendedup.grpc.IOService.FileSnapshotRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileSnapshotResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateCopyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void fileExists(org.opendedup.grpc.IOService.FileExistsRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileExistsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFileExistsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void mkDirAll(org.opendedup.grpc.IOService.MkDirRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.MkDirResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMkDirAllMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void stat(org.opendedup.grpc.FileInfo.FileInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileInfo.FileMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void rename(org.opendedup.grpc.IOService.FileRenameRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileRenameResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRenameMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void copyExtent(org.opendedup.grpc.IOService.CopyExtentRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.CopyExtentResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCopyExtentMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setUserMetaData(org.opendedup.grpc.IOService.SetUserMetaDataRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SetUserMetaDataResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetUserMetaDataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getCloudFile(org.opendedup.grpc.IOService.GetCloudFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetCloudFileResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCloudFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getCloudMetaFile(org.opendedup.grpc.IOService.GetCloudFileRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetCloudFileResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCloudMetaFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void statFS(org.opendedup.grpc.IOService.StatFSRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.StatFSResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStatFSMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void fileNotification(org.opendedup.grpc.IOService.SyncNotificationSubscription request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.FileInfo.FileMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getFileNotificationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setRetrievalTier(org.opendedup.grpc.IOService.SetRetrievalTierRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SetRetrievalTierResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetRetrievalTierMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getRetrievalTier(org.opendedup.grpc.IOService.GetRetrievalTierRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetRetrievalTierResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetRetrievalTierMethod(), getCallOptions()), request, responseObserver);
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
    public org.opendedup.grpc.IOService.GetXAttrSizeResponse getXAttrSize(org.opendedup.grpc.IOService.GetXAttrSizeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetXAttrSizeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.FsyncResponse fsync(org.opendedup.grpc.IOService.FsyncRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFsyncMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.SetXAttrResponse setXAttr(org.opendedup.grpc.IOService.SetXAttrRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetXAttrMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.RemoveXAttrResponse removeXAttr(org.opendedup.grpc.IOService.RemoveXAttrRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRemoveXAttrMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.GetXAttrResponse getXAttr(org.opendedup.grpc.IOService.GetXAttrRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetXAttrMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.UtimeResponse utime(org.opendedup.grpc.IOService.UtimeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUtimeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.TruncateResponse truncate(org.opendedup.grpc.IOService.TruncateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTruncateMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.SymLinkResponse symLink(org.opendedup.grpc.IOService.SymLinkRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSymLinkMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.LinkResponse readLink(org.opendedup.grpc.IOService.LinkRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadLinkMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.StatResponse getAttr(org.opendedup.grpc.IOService.StatRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetAttrMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.FlushResponse flush(org.opendedup.grpc.IOService.FlushRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFlushMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.ChownResponse chown(org.opendedup.grpc.IOService.ChownRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getChownMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.ChmodResponse chmod(org.opendedup.grpc.IOService.ChmodRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getChmodMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.MkDirResponse mkDir(org.opendedup.grpc.IOService.MkDirRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMkDirMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.RmDirResponse rmDir(org.opendedup.grpc.IOService.RmDirRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRmDirMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.UnlinkResponse unlink(org.opendedup.grpc.IOService.UnlinkRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUnlinkMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.DataWriteResponse write(org.opendedup.grpc.IOService.DataWriteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWriteMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.DataWriteResponse rawWrite(org.opendedup.grpc.IOService.RawDataWriteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRawWriteMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.DataReadResponse read(org.opendedup.grpc.IOService.DataReadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.FileCloseResponse release(org.opendedup.grpc.IOService.FileCloseRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReleaseMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.MkNodResponse mknod(org.opendedup.grpc.IOService.MkNodRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMknodMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.FileOpenResponse open(org.opendedup.grpc.IOService.FileOpenRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getOpenMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FileInfo.FileMessageResponse getFileInfo(org.opendedup.grpc.FileInfo.FileInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetFileInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.FileSnapshotResponse createCopy(org.opendedup.grpc.IOService.FileSnapshotRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateCopyMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.FileExistsResponse fileExists(org.opendedup.grpc.IOService.FileExistsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFileExistsMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.MkDirResponse mkDirAll(org.opendedup.grpc.IOService.MkDirRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMkDirAllMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.FileInfo.FileMessageResponse stat(org.opendedup.grpc.FileInfo.FileInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStatMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.FileRenameResponse rename(org.opendedup.grpc.IOService.FileRenameRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRenameMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.CopyExtentResponse copyExtent(org.opendedup.grpc.IOService.CopyExtentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCopyExtentMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.SetUserMetaDataResponse setUserMetaData(org.opendedup.grpc.IOService.SetUserMetaDataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetUserMetaDataMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.GetCloudFileResponse getCloudFile(org.opendedup.grpc.IOService.GetCloudFileRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCloudFileMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.GetCloudFileResponse getCloudMetaFile(org.opendedup.grpc.IOService.GetCloudFileRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCloudMetaFileMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.StatFSResponse statFS(org.opendedup.grpc.IOService.StatFSRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStatFSMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<org.opendedup.grpc.FileInfo.FileMessageResponse> fileNotification(
        org.opendedup.grpc.IOService.SyncNotificationSubscription request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getFileNotificationMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.SetRetrievalTierResponse setRetrievalTier(org.opendedup.grpc.IOService.SetRetrievalTierRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetRetrievalTierMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.IOService.GetRetrievalTierResponse getRetrievalTier(org.opendedup.grpc.IOService.GetRetrievalTierRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetRetrievalTierMethod(), getCallOptions(), request);
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
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.GetXAttrSizeResponse> getXAttrSize(
        org.opendedup.grpc.IOService.GetXAttrSizeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetXAttrSizeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.FsyncResponse> fsync(
        org.opendedup.grpc.IOService.FsyncRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFsyncMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.SetXAttrResponse> setXAttr(
        org.opendedup.grpc.IOService.SetXAttrRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetXAttrMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.RemoveXAttrResponse> removeXAttr(
        org.opendedup.grpc.IOService.RemoveXAttrRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRemoveXAttrMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.GetXAttrResponse> getXAttr(
        org.opendedup.grpc.IOService.GetXAttrRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetXAttrMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.UtimeResponse> utime(
        org.opendedup.grpc.IOService.UtimeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUtimeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.TruncateResponse> truncate(
        org.opendedup.grpc.IOService.TruncateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTruncateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.SymLinkResponse> symLink(
        org.opendedup.grpc.IOService.SymLinkRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSymLinkMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.LinkResponse> readLink(
        org.opendedup.grpc.IOService.LinkRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadLinkMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.StatResponse> getAttr(
        org.opendedup.grpc.IOService.StatRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetAttrMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.FlushResponse> flush(
        org.opendedup.grpc.IOService.FlushRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFlushMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.ChownResponse> chown(
        org.opendedup.grpc.IOService.ChownRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getChownMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.ChmodResponse> chmod(
        org.opendedup.grpc.IOService.ChmodRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getChmodMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.MkDirResponse> mkDir(
        org.opendedup.grpc.IOService.MkDirRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMkDirMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.RmDirResponse> rmDir(
        org.opendedup.grpc.IOService.RmDirRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRmDirMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.UnlinkResponse> unlink(
        org.opendedup.grpc.IOService.UnlinkRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUnlinkMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.DataWriteResponse> write(
        org.opendedup.grpc.IOService.DataWriteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.DataWriteResponse> rawWrite(
        org.opendedup.grpc.IOService.RawDataWriteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRawWriteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.DataReadResponse> read(
        org.opendedup.grpc.IOService.DataReadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.FileCloseResponse> release(
        org.opendedup.grpc.IOService.FileCloseRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReleaseMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.MkNodResponse> mknod(
        org.opendedup.grpc.IOService.MkNodRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMknodMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.FileOpenResponse> open(
        org.opendedup.grpc.IOService.FileOpenRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getOpenMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FileInfo.FileMessageResponse> getFileInfo(
        org.opendedup.grpc.FileInfo.FileInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetFileInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.FileSnapshotResponse> createCopy(
        org.opendedup.grpc.IOService.FileSnapshotRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateCopyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.FileExistsResponse> fileExists(
        org.opendedup.grpc.IOService.FileExistsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFileExistsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.MkDirResponse> mkDirAll(
        org.opendedup.grpc.IOService.MkDirRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMkDirAllMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.FileInfo.FileMessageResponse> stat(
        org.opendedup.grpc.FileInfo.FileInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStatMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.FileRenameResponse> rename(
        org.opendedup.grpc.IOService.FileRenameRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRenameMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.CopyExtentResponse> copyExtent(
        org.opendedup.grpc.IOService.CopyExtentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCopyExtentMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.SetUserMetaDataResponse> setUserMetaData(
        org.opendedup.grpc.IOService.SetUserMetaDataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetUserMetaDataMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.GetCloudFileResponse> getCloudFile(
        org.opendedup.grpc.IOService.GetCloudFileRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCloudFileMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.GetCloudFileResponse> getCloudMetaFile(
        org.opendedup.grpc.IOService.GetCloudFileRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCloudMetaFileMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.StatFSResponse> statFS(
        org.opendedup.grpc.IOService.StatFSRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStatFSMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.SetRetrievalTierResponse> setRetrievalTier(
        org.opendedup.grpc.IOService.SetRetrievalTierRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetRetrievalTierMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.IOService.GetRetrievalTierResponse> getRetrievalTier(
        org.opendedup.grpc.IOService.GetRetrievalTierRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetRetrievalTierMethod(), getCallOptions()), request);
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
  private static final int METHODID_RAW_WRITE = 17;
  private static final int METHODID_READ = 18;
  private static final int METHODID_RELEASE = 19;
  private static final int METHODID_MKNOD = 20;
  private static final int METHODID_OPEN = 21;
  private static final int METHODID_GET_FILE_INFO = 22;
  private static final int METHODID_CREATE_COPY = 23;
  private static final int METHODID_FILE_EXISTS = 24;
  private static final int METHODID_MK_DIR_ALL = 25;
  private static final int METHODID_STAT = 26;
  private static final int METHODID_RENAME = 27;
  private static final int METHODID_COPY_EXTENT = 28;
  private static final int METHODID_SET_USER_META_DATA = 29;
  private static final int METHODID_GET_CLOUD_FILE = 30;
  private static final int METHODID_GET_CLOUD_META_FILE = 31;
  private static final int METHODID_STAT_FS = 32;
  private static final int METHODID_FILE_NOTIFICATION = 33;
  private static final int METHODID_SET_RETRIEVAL_TIER = 34;
  private static final int METHODID_GET_RETRIEVAL_TIER = 35;
  private static final int METHODID_STREAM_WRITE = 36;

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
          serviceImpl.getXAttrSize((org.opendedup.grpc.IOService.GetXAttrSizeRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetXAttrSizeResponse>) responseObserver);
          break;
        case METHODID_FSYNC:
          serviceImpl.fsync((org.opendedup.grpc.IOService.FsyncRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FsyncResponse>) responseObserver);
          break;
        case METHODID_SET_XATTR:
          serviceImpl.setXAttr((org.opendedup.grpc.IOService.SetXAttrRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SetXAttrResponse>) responseObserver);
          break;
        case METHODID_REMOVE_XATTR:
          serviceImpl.removeXAttr((org.opendedup.grpc.IOService.RemoveXAttrRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.RemoveXAttrResponse>) responseObserver);
          break;
        case METHODID_GET_XATTR:
          serviceImpl.getXAttr((org.opendedup.grpc.IOService.GetXAttrRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetXAttrResponse>) responseObserver);
          break;
        case METHODID_UTIME:
          serviceImpl.utime((org.opendedup.grpc.IOService.UtimeRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.UtimeResponse>) responseObserver);
          break;
        case METHODID_TRUNCATE:
          serviceImpl.truncate((org.opendedup.grpc.IOService.TruncateRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.TruncateResponse>) responseObserver);
          break;
        case METHODID_SYM_LINK:
          serviceImpl.symLink((org.opendedup.grpc.IOService.SymLinkRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SymLinkResponse>) responseObserver);
          break;
        case METHODID_READ_LINK:
          serviceImpl.readLink((org.opendedup.grpc.IOService.LinkRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.LinkResponse>) responseObserver);
          break;
        case METHODID_GET_ATTR:
          serviceImpl.getAttr((org.opendedup.grpc.IOService.StatRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.StatResponse>) responseObserver);
          break;
        case METHODID_FLUSH:
          serviceImpl.flush((org.opendedup.grpc.IOService.FlushRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FlushResponse>) responseObserver);
          break;
        case METHODID_CHOWN:
          serviceImpl.chown((org.opendedup.grpc.IOService.ChownRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.ChownResponse>) responseObserver);
          break;
        case METHODID_CHMOD:
          serviceImpl.chmod((org.opendedup.grpc.IOService.ChmodRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.ChmodResponse>) responseObserver);
          break;
        case METHODID_MK_DIR:
          serviceImpl.mkDir((org.opendedup.grpc.IOService.MkDirRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.MkDirResponse>) responseObserver);
          break;
        case METHODID_RM_DIR:
          serviceImpl.rmDir((org.opendedup.grpc.IOService.RmDirRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.RmDirResponse>) responseObserver);
          break;
        case METHODID_UNLINK:
          serviceImpl.unlink((org.opendedup.grpc.IOService.UnlinkRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.UnlinkResponse>) responseObserver);
          break;
        case METHODID_WRITE:
          serviceImpl.write((org.opendedup.grpc.IOService.DataWriteRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteResponse>) responseObserver);
          break;
        case METHODID_RAW_WRITE:
          serviceImpl.rawWrite((org.opendedup.grpc.IOService.RawDataWriteRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteResponse>) responseObserver);
          break;
        case METHODID_READ:
          serviceImpl.read((org.opendedup.grpc.IOService.DataReadRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataReadResponse>) responseObserver);
          break;
        case METHODID_RELEASE:
          serviceImpl.release((org.opendedup.grpc.IOService.FileCloseRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileCloseResponse>) responseObserver);
          break;
        case METHODID_MKNOD:
          serviceImpl.mknod((org.opendedup.grpc.IOService.MkNodRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.MkNodResponse>) responseObserver);
          break;
        case METHODID_OPEN:
          serviceImpl.open((org.opendedup.grpc.IOService.FileOpenRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileOpenResponse>) responseObserver);
          break;
        case METHODID_GET_FILE_INFO:
          serviceImpl.getFileInfo((org.opendedup.grpc.FileInfo.FileInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileInfo.FileMessageResponse>) responseObserver);
          break;
        case METHODID_CREATE_COPY:
          serviceImpl.createCopy((org.opendedup.grpc.IOService.FileSnapshotRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileSnapshotResponse>) responseObserver);
          break;
        case METHODID_FILE_EXISTS:
          serviceImpl.fileExists((org.opendedup.grpc.IOService.FileExistsRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileExistsResponse>) responseObserver);
          break;
        case METHODID_MK_DIR_ALL:
          serviceImpl.mkDirAll((org.opendedup.grpc.IOService.MkDirRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.MkDirResponse>) responseObserver);
          break;
        case METHODID_STAT:
          serviceImpl.stat((org.opendedup.grpc.FileInfo.FileInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileInfo.FileMessageResponse>) responseObserver);
          break;
        case METHODID_RENAME:
          serviceImpl.rename((org.opendedup.grpc.IOService.FileRenameRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.FileRenameResponse>) responseObserver);
          break;
        case METHODID_COPY_EXTENT:
          serviceImpl.copyExtent((org.opendedup.grpc.IOService.CopyExtentRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.CopyExtentResponse>) responseObserver);
          break;
        case METHODID_SET_USER_META_DATA:
          serviceImpl.setUserMetaData((org.opendedup.grpc.IOService.SetUserMetaDataRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SetUserMetaDataResponse>) responseObserver);
          break;
        case METHODID_GET_CLOUD_FILE:
          serviceImpl.getCloudFile((org.opendedup.grpc.IOService.GetCloudFileRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetCloudFileResponse>) responseObserver);
          break;
        case METHODID_GET_CLOUD_META_FILE:
          serviceImpl.getCloudMetaFile((org.opendedup.grpc.IOService.GetCloudFileRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetCloudFileResponse>) responseObserver);
          break;
        case METHODID_STAT_FS:
          serviceImpl.statFS((org.opendedup.grpc.IOService.StatFSRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.StatFSResponse>) responseObserver);
          break;
        case METHODID_FILE_NOTIFICATION:
          serviceImpl.fileNotification((org.opendedup.grpc.IOService.SyncNotificationSubscription) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.FileInfo.FileMessageResponse>) responseObserver);
          break;
        case METHODID_SET_RETRIEVAL_TIER:
          serviceImpl.setRetrievalTier((org.opendedup.grpc.IOService.SetRetrievalTierRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.SetRetrievalTierResponse>) responseObserver);
          break;
        case METHODID_GET_RETRIEVAL_TIER:
          serviceImpl.getRetrievalTier((org.opendedup.grpc.IOService.GetRetrievalTierRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.GetRetrievalTierResponse>) responseObserver);
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
        case METHODID_STREAM_WRITE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.streamWrite(
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.IOService.DataWriteResponse>) responseObserver);
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
              .addMethod(getRawWriteMethod())
              .addMethod(getStreamWriteMethod())
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
              .addMethod(getSetRetrievalTierMethod())
              .addMethod(getGetRetrievalTierMethod())
              .build();
        }
      }
    }
    return result;
  }
}
