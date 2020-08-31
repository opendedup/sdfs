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

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
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
  }

  private static final int METHODID_MK_DIR = 0;
  private static final int METHODID_RM_DIR = 1;
  private static final int METHODID_UNLINK = 2;
  private static final int METHODID_WRITE = 3;
  private static final int METHODID_READ = 4;
  private static final int METHODID_RELEASE = 5;
  private static final int METHODID_MKNOD = 6;
  private static final int METHODID_OPEN = 7;
  private static final int METHODID_GET_FILE_INFO = 8;
  private static final int METHODID_CREATE_COPY = 9;
  private static final int METHODID_FILE_EXISTS = 10;
  private static final int METHODID_MK_DIR_ALL = 11;
  private static final int METHODID_STAT = 12;
  private static final int METHODID_RENAME = 13;
  private static final int METHODID_COPY_EXTENT = 14;
  private static final int METHODID_SET_USER_META_DATA = 15;
  private static final int METHODID_GET_CLOUD_FILE = 16;
  private static final int METHODID_GET_CLOUD_META_FILE = 17;

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
              .build();
        }
      }
    }
    return result;
  }
}
