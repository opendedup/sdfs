package org.opendedup.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Defining a Service, a Service can have multiple RPC operations
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.46.0)",
    comments = "Source: VolumeService.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class VolumeServiceGrpc {

  private VolumeServiceGrpc() {}

  public static final String SERVICE_NAME = "org.opendedup.grpc.VolumeService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse> getAuthenticateUserMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AuthenticateUser",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse> getAuthenticateUserMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest, org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse> getAuthenticateUserMethod;
    if ((getAuthenticateUserMethod = VolumeServiceGrpc.getAuthenticateUserMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getAuthenticateUserMethod = VolumeServiceGrpc.getAuthenticateUserMethod) == null) {
          VolumeServiceGrpc.getAuthenticateUserMethod = getAuthenticateUserMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest, org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AuthenticateUser"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("AuthenticateUser"))
              .build();
        }
      }
    }
    return getAuthenticateUserMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse> getGetVolumeInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetVolumeInfo",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse> getGetVolumeInfoMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest, org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse> getGetVolumeInfoMethod;
    if ((getGetVolumeInfoMethod = VolumeServiceGrpc.getGetVolumeInfoMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getGetVolumeInfoMethod = VolumeServiceGrpc.getGetVolumeInfoMethod) == null) {
          VolumeServiceGrpc.getGetVolumeInfoMethod = getGetVolumeInfoMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest, org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetVolumeInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("GetVolumeInfo"))
              .build();
        }
      }
    }
    return getGetVolumeInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.Shutdown.ShutdownRequest,
      org.opendedup.grpc.Shutdown.ShutdownResponse> getShutdownVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ShutdownVolume",
      requestType = org.opendedup.grpc.Shutdown.ShutdownRequest.class,
      responseType = org.opendedup.grpc.Shutdown.ShutdownResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.Shutdown.ShutdownRequest,
      org.opendedup.grpc.Shutdown.ShutdownResponse> getShutdownVolumeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.Shutdown.ShutdownRequest, org.opendedup.grpc.Shutdown.ShutdownResponse> getShutdownVolumeMethod;
    if ((getShutdownVolumeMethod = VolumeServiceGrpc.getShutdownVolumeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getShutdownVolumeMethod = VolumeServiceGrpc.getShutdownVolumeMethod) == null) {
          VolumeServiceGrpc.getShutdownVolumeMethod = getShutdownVolumeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.Shutdown.ShutdownRequest, org.opendedup.grpc.Shutdown.ShutdownResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShutdownVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Shutdown.ShutdownRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.Shutdown.ShutdownResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("ShutdownVolume"))
              .build();
        }
      }
    }
    return getShutdownVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse> getCleanStoreMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CleanStore",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse> getCleanStoreMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest, org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse> getCleanStoreMethod;
    if ((getCleanStoreMethod = VolumeServiceGrpc.getCleanStoreMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getCleanStoreMethod = VolumeServiceGrpc.getCleanStoreMethod) == null) {
          VolumeServiceGrpc.getCleanStoreMethod = getCleanStoreMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest, org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CleanStore"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("CleanStore"))
              .build();
        }
      }
    }
    return getCleanStoreMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse> getDeleteCloudVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteCloudVolume",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse> getDeleteCloudVolumeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest, org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse> getDeleteCloudVolumeMethod;
    if ((getDeleteCloudVolumeMethod = VolumeServiceGrpc.getDeleteCloudVolumeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getDeleteCloudVolumeMethod = VolumeServiceGrpc.getDeleteCloudVolumeMethod) == null) {
          VolumeServiceGrpc.getDeleteCloudVolumeMethod = getDeleteCloudVolumeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest, org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteCloudVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("DeleteCloudVolume"))
              .build();
        }
      }
    }
    return getDeleteCloudVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.DSERequest,
      org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse> getDSEInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DSEInfo",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.DSERequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.DSERequest,
      org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse> getDSEInfoMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.DSERequest, org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse> getDSEInfoMethod;
    if ((getDSEInfoMethod = VolumeServiceGrpc.getDSEInfoMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getDSEInfoMethod = VolumeServiceGrpc.getDSEInfoMethod) == null) {
          VolumeServiceGrpc.getDSEInfoMethod = getDSEInfoMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.DSERequest, org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DSEInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.DSERequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("DSEInfo"))
              .build();
        }
      }
    }
    return getDSEInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse> getSystemInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SystemInfo",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse> getSystemInfoMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest, org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse> getSystemInfoMethod;
    if ((getSystemInfoMethod = VolumeServiceGrpc.getSystemInfoMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSystemInfoMethod = VolumeServiceGrpc.getSystemInfoMethod) == null) {
          VolumeServiceGrpc.getSystemInfoMethod = getSystemInfoMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest, org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SystemInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SystemInfo"))
              .build();
        }
      }
    }
    return getSystemInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse> getSetVolumeCapacityMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetVolumeCapacity",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse> getSetVolumeCapacityMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest, org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse> getSetVolumeCapacityMethod;
    if ((getSetVolumeCapacityMethod = VolumeServiceGrpc.getSetVolumeCapacityMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetVolumeCapacityMethod = VolumeServiceGrpc.getSetVolumeCapacityMethod) == null) {
          VolumeServiceGrpc.getSetVolumeCapacityMethod = getSetVolumeCapacityMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest, org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetVolumeCapacity"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetVolumeCapacity"))
              .build();
        }
      }
    }
    return getSetVolumeCapacityMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse> getGetConnectedVolumesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetConnectedVolumes",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse> getGetConnectedVolumesMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest, org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse> getGetConnectedVolumesMethod;
    if ((getGetConnectedVolumesMethod = VolumeServiceGrpc.getGetConnectedVolumesMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getGetConnectedVolumesMethod = VolumeServiceGrpc.getGetConnectedVolumesMethod) == null) {
          VolumeServiceGrpc.getGetConnectedVolumesMethod = getGetConnectedVolumesMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest, org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetConnectedVolumes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("GetConnectedVolumes"))
              .build();
        }
      }
    }
    return getGetConnectedVolumesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse> getGetGCScheduleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetGCSchedule",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse> getGetGCScheduleMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest, org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse> getGetGCScheduleMethod;
    if ((getGetGCScheduleMethod = VolumeServiceGrpc.getGetGCScheduleMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getGetGCScheduleMethod = VolumeServiceGrpc.getGetGCScheduleMethod) == null) {
          VolumeServiceGrpc.getGetGCScheduleMethod = getGetGCScheduleMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest, org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetGCSchedule"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("GetGCSchedule"))
              .build();
        }
      }
    }
    return getGetGCScheduleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse> getSetCacheSizeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetCacheSize",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse> getSetCacheSizeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest, org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse> getSetCacheSizeMethod;
    if ((getSetCacheSizeMethod = VolumeServiceGrpc.getSetCacheSizeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetCacheSizeMethod = VolumeServiceGrpc.getSetCacheSizeMethod) == null) {
          VolumeServiceGrpc.getSetCacheSizeMethod = getSetCacheSizeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest, org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetCacheSize"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetCacheSize"))
              .build();
        }
      }
    }
    return getSetCacheSizeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse> getSetPasswordMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetPassword",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse> getSetPasswordMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest, org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse> getSetPasswordMethod;
    if ((getSetPasswordMethod = VolumeServiceGrpc.getSetPasswordMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetPasswordMethod = VolumeServiceGrpc.getSetPasswordMethod) == null) {
          VolumeServiceGrpc.getSetPasswordMethod = getSetPasswordMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest, org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetPassword"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetPassword"))
              .build();
        }
      }
    }
    return getSetPasswordMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> getSetReadSpeedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetReadSpeed",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> getSetReadSpeedMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest, org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> getSetReadSpeedMethod;
    if ((getSetReadSpeedMethod = VolumeServiceGrpc.getSetReadSpeedMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetReadSpeedMethod = VolumeServiceGrpc.getSetReadSpeedMethod) == null) {
          VolumeServiceGrpc.getSetReadSpeedMethod = getSetReadSpeedMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest, org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetReadSpeed"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetReadSpeed"))
              .build();
        }
      }
    }
    return getSetReadSpeedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> getSetWriteSpeedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetWriteSpeed",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> getSetWriteSpeedMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest, org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> getSetWriteSpeedMethod;
    if ((getSetWriteSpeedMethod = VolumeServiceGrpc.getSetWriteSpeedMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetWriteSpeedMethod = VolumeServiceGrpc.getSetWriteSpeedMethod) == null) {
          VolumeServiceGrpc.getSetWriteSpeedMethod = getSetWriteSpeedMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest, org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetWriteSpeed"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetWriteSpeed"))
              .build();
        }
      }
    }
    return getSetWriteSpeedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse> getSyncFromCloudVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SyncFromCloudVolume",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse> getSyncFromCloudVolumeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest, org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse> getSyncFromCloudVolumeMethod;
    if ((getSyncFromCloudVolumeMethod = VolumeServiceGrpc.getSyncFromCloudVolumeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSyncFromCloudVolumeMethod = VolumeServiceGrpc.getSyncFromCloudVolumeMethod) == null) {
          VolumeServiceGrpc.getSyncFromCloudVolumeMethod = getSyncFromCloudVolumeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest, org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SyncFromCloudVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SyncFromCloudVolume"))
              .build();
        }
      }
    }
    return getSyncFromCloudVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse> getSyncCloudVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SyncCloudVolume",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse> getSyncCloudVolumeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest, org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse> getSyncCloudVolumeMethod;
    if ((getSyncCloudVolumeMethod = VolumeServiceGrpc.getSyncCloudVolumeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSyncCloudVolumeMethod = VolumeServiceGrpc.getSyncCloudVolumeMethod) == null) {
          VolumeServiceGrpc.getSyncCloudVolumeMethod = getSyncCloudVolumeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest, org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SyncCloudVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SyncCloudVolume"))
              .build();
        }
      }
    }
    return getSyncCloudVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse> getSetMaxAgeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetMaxAge",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse> getSetMaxAgeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest, org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse> getSetMaxAgeMethod;
    if ((getSetMaxAgeMethod = VolumeServiceGrpc.getSetMaxAgeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetMaxAgeMethod = VolumeServiceGrpc.getSetMaxAgeMethod) == null) {
          VolumeServiceGrpc.getSetMaxAgeMethod = getSetMaxAgeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest, org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetMaxAge"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetMaxAge"))
              .build();
        }
      }
    }
    return getSetMaxAgeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse> getReconcileCloudMetadataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReconcileCloudMetadata",
      requestType = org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest.class,
      responseType = org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest,
      org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse> getReconcileCloudMetadataMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest, org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse> getReconcileCloudMetadataMethod;
    if ((getReconcileCloudMetadataMethod = VolumeServiceGrpc.getReconcileCloudMetadataMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getReconcileCloudMetadataMethod = VolumeServiceGrpc.getReconcileCloudMetadataMethod) == null) {
          VolumeServiceGrpc.getReconcileCloudMetadataMethod = getReconcileCloudMetadataMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest, org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReconcileCloudMetadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("ReconcileCloudMetadata"))
              .build();
        }
      }
    }
    return getReconcileCloudMetadataMethod;
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
    public void authenticateUser(org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAuthenticateUserMethod(), responseObserver);
    }

    /**
     */
    public void getVolumeInfo(org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetVolumeInfoMethod(), responseObserver);
    }

    /**
     */
    public void shutdownVolume(org.opendedup.grpc.Shutdown.ShutdownRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Shutdown.ShutdownResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getShutdownVolumeMethod(), responseObserver);
    }

    /**
     */
    public void cleanStore(org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCleanStoreMethod(), responseObserver);
    }

    /**
     */
    public void deleteCloudVolume(org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteCloudVolumeMethod(), responseObserver);
    }

    /**
     */
    public void dSEInfo(org.opendedup.grpc.VolumeServiceOuterClass.DSERequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDSEInfoMethod(), responseObserver);
    }

    /**
     */
    public void systemInfo(org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSystemInfoMethod(), responseObserver);
    }

    /**
     */
    public void setVolumeCapacity(org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetVolumeCapacityMethod(), responseObserver);
    }

    /**
     */
    public void getConnectedVolumes(org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetConnectedVolumesMethod(), responseObserver);
    }

    /**
     */
    public void getGCSchedule(org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetGCScheduleMethod(), responseObserver);
    }

    /**
     */
    public void setCacheSize(org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetCacheSizeMethod(), responseObserver);
    }

    /**
     */
    public void setPassword(org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetPasswordMethod(), responseObserver);
    }

    /**
     */
    public void setReadSpeed(org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetReadSpeedMethod(), responseObserver);
    }

    /**
     */
    public void setWriteSpeed(org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetWriteSpeedMethod(), responseObserver);
    }

    /**
     */
    public void syncFromCloudVolume(org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSyncFromCloudVolumeMethod(), responseObserver);
    }

    /**
     */
    public void syncCloudVolume(org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSyncCloudVolumeMethod(), responseObserver);
    }

    /**
     */
    public void setMaxAge(org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetMaxAgeMethod(), responseObserver);
    }

    /**
     */
    public void reconcileCloudMetadata(org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReconcileCloudMetadataMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getAuthenticateUserMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse>(
                  this, METHODID_AUTHENTICATE_USER)))
          .addMethod(
            getGetVolumeInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse>(
                  this, METHODID_GET_VOLUME_INFO)))
          .addMethod(
            getShutdownVolumeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.Shutdown.ShutdownRequest,
                org.opendedup.grpc.Shutdown.ShutdownResponse>(
                  this, METHODID_SHUTDOWN_VOLUME)))
          .addMethod(
            getCleanStoreMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse>(
                  this, METHODID_CLEAN_STORE)))
          .addMethod(
            getDeleteCloudVolumeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse>(
                  this, METHODID_DELETE_CLOUD_VOLUME)))
          .addMethod(
            getDSEInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.DSERequest,
                org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse>(
                  this, METHODID_DSEINFO)))
          .addMethod(
            getSystemInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse>(
                  this, METHODID_SYSTEM_INFO)))
          .addMethod(
            getSetVolumeCapacityMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse>(
                  this, METHODID_SET_VOLUME_CAPACITY)))
          .addMethod(
            getGetConnectedVolumesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse>(
                  this, METHODID_GET_CONNECTED_VOLUMES)))
          .addMethod(
            getGetGCScheduleMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse>(
                  this, METHODID_GET_GCSCHEDULE)))
          .addMethod(
            getSetCacheSizeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse>(
                  this, METHODID_SET_CACHE_SIZE)))
          .addMethod(
            getSetPasswordMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse>(
                  this, METHODID_SET_PASSWORD)))
          .addMethod(
            getSetReadSpeedMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse>(
                  this, METHODID_SET_READ_SPEED)))
          .addMethod(
            getSetWriteSpeedMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse>(
                  this, METHODID_SET_WRITE_SPEED)))
          .addMethod(
            getSyncFromCloudVolumeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse>(
                  this, METHODID_SYNC_FROM_CLOUD_VOLUME)))
          .addMethod(
            getSyncCloudVolumeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse>(
                  this, METHODID_SYNC_CLOUD_VOLUME)))
          .addMethod(
            getSetMaxAgeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse>(
                  this, METHODID_SET_MAX_AGE)))
          .addMethod(
            getReconcileCloudMetadataMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest,
                org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse>(
                  this, METHODID_RECONCILE_CLOUD_METADATA)))
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
    public void authenticateUser(org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAuthenticateUserMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getVolumeInfo(org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetVolumeInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void shutdownVolume(org.opendedup.grpc.Shutdown.ShutdownRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.Shutdown.ShutdownResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getShutdownVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cleanStore(org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCleanStoreMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteCloudVolume(org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteCloudVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dSEInfo(org.opendedup.grpc.VolumeServiceOuterClass.DSERequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDSEInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void systemInfo(org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSystemInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setVolumeCapacity(org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetVolumeCapacityMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getConnectedVolumes(org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetConnectedVolumesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getGCSchedule(org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetGCScheduleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setCacheSize(org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetCacheSizeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setPassword(org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetPasswordMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setReadSpeed(org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetReadSpeedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setWriteSpeed(org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetWriteSpeedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void syncFromCloudVolume(org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSyncFromCloudVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void syncCloudVolume(org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSyncCloudVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setMaxAge(org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetMaxAgeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reconcileCloudMetadata(org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReconcileCloudMetadataMethod(), getCallOptions()), request, responseObserver);
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
    public org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse authenticateUser(org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAuthenticateUserMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse getVolumeInfo(org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetVolumeInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.Shutdown.ShutdownResponse shutdownVolume(org.opendedup.grpc.Shutdown.ShutdownRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getShutdownVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse cleanStore(org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCleanStoreMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse deleteCloudVolume(org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteCloudVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse dSEInfo(org.opendedup.grpc.VolumeServiceOuterClass.DSERequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDSEInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse systemInfo(org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSystemInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse setVolumeCapacity(org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetVolumeCapacityMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse getConnectedVolumes(org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetConnectedVolumesMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse getGCSchedule(org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetGCScheduleMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse setCacheSize(org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetCacheSizeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse setPassword(org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetPasswordMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse setReadSpeed(org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetReadSpeedMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse setWriteSpeed(org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetWriteSpeedMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse syncFromCloudVolume(org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSyncFromCloudVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse syncCloudVolume(org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSyncCloudVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse setMaxAge(org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetMaxAgeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse reconcileCloudMetadata(org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReconcileCloudMetadataMethod(), getCallOptions(), request);
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
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse> authenticateUser(
        org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAuthenticateUserMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse> getVolumeInfo(
        org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetVolumeInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.Shutdown.ShutdownResponse> shutdownVolume(
        org.opendedup.grpc.Shutdown.ShutdownRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getShutdownVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse> cleanStore(
        org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCleanStoreMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse> deleteCloudVolume(
        org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteCloudVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse> dSEInfo(
        org.opendedup.grpc.VolumeServiceOuterClass.DSERequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDSEInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse> systemInfo(
        org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSystemInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse> setVolumeCapacity(
        org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetVolumeCapacityMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse> getConnectedVolumes(
        org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetConnectedVolumesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse> getGCSchedule(
        org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetGCScheduleMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse> setCacheSize(
        org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetCacheSizeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse> setPassword(
        org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetPasswordMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> setReadSpeed(
        org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetReadSpeedMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse> setWriteSpeed(
        org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetWriteSpeedMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse> syncFromCloudVolume(
        org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSyncFromCloudVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse> syncCloudVolume(
        org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSyncCloudVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse> setMaxAge(
        org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetMaxAgeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse> reconcileCloudMetadata(
        org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReconcileCloudMetadataMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_AUTHENTICATE_USER = 0;
  private static final int METHODID_GET_VOLUME_INFO = 1;
  private static final int METHODID_SHUTDOWN_VOLUME = 2;
  private static final int METHODID_CLEAN_STORE = 3;
  private static final int METHODID_DELETE_CLOUD_VOLUME = 4;
  private static final int METHODID_DSEINFO = 5;
  private static final int METHODID_SYSTEM_INFO = 6;
  private static final int METHODID_SET_VOLUME_CAPACITY = 7;
  private static final int METHODID_GET_CONNECTED_VOLUMES = 8;
  private static final int METHODID_GET_GCSCHEDULE = 9;
  private static final int METHODID_SET_CACHE_SIZE = 10;
  private static final int METHODID_SET_PASSWORD = 11;
  private static final int METHODID_SET_READ_SPEED = 12;
  private static final int METHODID_SET_WRITE_SPEED = 13;
  private static final int METHODID_SYNC_FROM_CLOUD_VOLUME = 14;
  private static final int METHODID_SYNC_CLOUD_VOLUME = 15;
  private static final int METHODID_SET_MAX_AGE = 16;
  private static final int METHODID_RECONCILE_CLOUD_METADATA = 17;

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
          serviceImpl.authenticateUser((org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse>) responseObserver);
          break;
        case METHODID_GET_VOLUME_INFO:
          serviceImpl.getVolumeInfo((org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse>) responseObserver);
          break;
        case METHODID_SHUTDOWN_VOLUME:
          serviceImpl.shutdownVolume((org.opendedup.grpc.Shutdown.ShutdownRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.Shutdown.ShutdownResponse>) responseObserver);
          break;
        case METHODID_CLEAN_STORE:
          serviceImpl.cleanStore((org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse>) responseObserver);
          break;
        case METHODID_DELETE_CLOUD_VOLUME:
          serviceImpl.deleteCloudVolume((org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse>) responseObserver);
          break;
        case METHODID_DSEINFO:
          serviceImpl.dSEInfo((org.opendedup.grpc.VolumeServiceOuterClass.DSERequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse>) responseObserver);
          break;
        case METHODID_SYSTEM_INFO:
          serviceImpl.systemInfo((org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse>) responseObserver);
          break;
        case METHODID_SET_VOLUME_CAPACITY:
          serviceImpl.setVolumeCapacity((org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse>) responseObserver);
          break;
        case METHODID_GET_CONNECTED_VOLUMES:
          serviceImpl.getConnectedVolumes((org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse>) responseObserver);
          break;
        case METHODID_GET_GCSCHEDULE:
          serviceImpl.getGCSchedule((org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse>) responseObserver);
          break;
        case METHODID_SET_CACHE_SIZE:
          serviceImpl.setCacheSize((org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse>) responseObserver);
          break;
        case METHODID_SET_PASSWORD:
          serviceImpl.setPassword((org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse>) responseObserver);
          break;
        case METHODID_SET_READ_SPEED:
          serviceImpl.setReadSpeed((org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse>) responseObserver);
          break;
        case METHODID_SET_WRITE_SPEED:
          serviceImpl.setWriteSpeed((org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse>) responseObserver);
          break;
        case METHODID_SYNC_FROM_CLOUD_VOLUME:
          serviceImpl.syncFromCloudVolume((org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse>) responseObserver);
          break;
        case METHODID_SYNC_CLOUD_VOLUME:
          serviceImpl.syncCloudVolume((org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse>) responseObserver);
          break;
        case METHODID_SET_MAX_AGE:
          serviceImpl.setMaxAge((org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse>) responseObserver);
          break;
        case METHODID_RECONCILE_CLOUD_METADATA:
          serviceImpl.reconcileCloudMetadata((org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.VolumeServiceOuterClass.ReconcileCloudMetadataResponse>) responseObserver);
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
              .addMethod(getSetVolumeCapacityMethod())
              .addMethod(getGetConnectedVolumesMethod())
              .addMethod(getGetGCScheduleMethod())
              .addMethod(getSetCacheSizeMethod())
              .addMethod(getSetPasswordMethod())
              .addMethod(getSetReadSpeedMethod())
              .addMethod(getSetWriteSpeedMethod())
              .addMethod(getSyncFromCloudVolumeMethod())
              .addMethod(getSyncCloudVolumeMethod())
              .addMethod(getSetMaxAgeMethod())
              .addMethod(getReconcileCloudMetadataMethod())
              .build();
        }
      }
    }
    return result;
  }
}
