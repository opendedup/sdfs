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
    value = "by gRPC proto compiler (version 1.31.1)",
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

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SetVolumeCapacityRequest,
      org.opendedup.grpc.SetVolumeCapacityResponse> getSetVolumeCapacityMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetVolumeCapacity",
      requestType = org.opendedup.grpc.SetVolumeCapacityRequest.class,
      responseType = org.opendedup.grpc.SetVolumeCapacityResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SetVolumeCapacityRequest,
      org.opendedup.grpc.SetVolumeCapacityResponse> getSetVolumeCapacityMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SetVolumeCapacityRequest, org.opendedup.grpc.SetVolumeCapacityResponse> getSetVolumeCapacityMethod;
    if ((getSetVolumeCapacityMethod = VolumeServiceGrpc.getSetVolumeCapacityMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetVolumeCapacityMethod = VolumeServiceGrpc.getSetVolumeCapacityMethod) == null) {
          VolumeServiceGrpc.getSetVolumeCapacityMethod = getSetVolumeCapacityMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SetVolumeCapacityRequest, org.opendedup.grpc.SetVolumeCapacityResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetVolumeCapacity"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetVolumeCapacityRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetVolumeCapacityResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetVolumeCapacity"))
              .build();
        }
      }
    }
    return getSetVolumeCapacityMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.CloudVolumesRequest,
      org.opendedup.grpc.CloudVolumesResponse> getGetConnectedVolumesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetConnectedVolumes",
      requestType = org.opendedup.grpc.CloudVolumesRequest.class,
      responseType = org.opendedup.grpc.CloudVolumesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.CloudVolumesRequest,
      org.opendedup.grpc.CloudVolumesResponse> getGetConnectedVolumesMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.CloudVolumesRequest, org.opendedup.grpc.CloudVolumesResponse> getGetConnectedVolumesMethod;
    if ((getGetConnectedVolumesMethod = VolumeServiceGrpc.getGetConnectedVolumesMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getGetConnectedVolumesMethod = VolumeServiceGrpc.getGetConnectedVolumesMethod) == null) {
          VolumeServiceGrpc.getGetConnectedVolumesMethod = getGetConnectedVolumesMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.CloudVolumesRequest, org.opendedup.grpc.CloudVolumesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetConnectedVolumes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.CloudVolumesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.CloudVolumesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("GetConnectedVolumes"))
              .build();
        }
      }
    }
    return getGetConnectedVolumesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.GCScheduleRequest,
      org.opendedup.grpc.GCScheduleResponse> getGetGCScheduleMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetGCSchedule",
      requestType = org.opendedup.grpc.GCScheduleRequest.class,
      responseType = org.opendedup.grpc.GCScheduleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.GCScheduleRequest,
      org.opendedup.grpc.GCScheduleResponse> getGetGCScheduleMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.GCScheduleRequest, org.opendedup.grpc.GCScheduleResponse> getGetGCScheduleMethod;
    if ((getGetGCScheduleMethod = VolumeServiceGrpc.getGetGCScheduleMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getGetGCScheduleMethod = VolumeServiceGrpc.getGetGCScheduleMethod) == null) {
          VolumeServiceGrpc.getGetGCScheduleMethod = getGetGCScheduleMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.GCScheduleRequest, org.opendedup.grpc.GCScheduleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetGCSchedule"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GCScheduleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.GCScheduleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("GetGCSchedule"))
              .build();
        }
      }
    }
    return getGetGCScheduleMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SetCacheSizeRequest,
      org.opendedup.grpc.SetCacheSizeResponse> getSetCacheSizeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetCacheSize",
      requestType = org.opendedup.grpc.SetCacheSizeRequest.class,
      responseType = org.opendedup.grpc.SetCacheSizeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SetCacheSizeRequest,
      org.opendedup.grpc.SetCacheSizeResponse> getSetCacheSizeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SetCacheSizeRequest, org.opendedup.grpc.SetCacheSizeResponse> getSetCacheSizeMethod;
    if ((getSetCacheSizeMethod = VolumeServiceGrpc.getSetCacheSizeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetCacheSizeMethod = VolumeServiceGrpc.getSetCacheSizeMethod) == null) {
          VolumeServiceGrpc.getSetCacheSizeMethod = getSetCacheSizeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SetCacheSizeRequest, org.opendedup.grpc.SetCacheSizeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetCacheSize"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetCacheSizeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetCacheSizeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetCacheSize"))
              .build();
        }
      }
    }
    return getSetCacheSizeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SetPasswordRequest,
      org.opendedup.grpc.SetPasswordResponse> getSetPasswordMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetPassword",
      requestType = org.opendedup.grpc.SetPasswordRequest.class,
      responseType = org.opendedup.grpc.SetPasswordResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SetPasswordRequest,
      org.opendedup.grpc.SetPasswordResponse> getSetPasswordMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SetPasswordRequest, org.opendedup.grpc.SetPasswordResponse> getSetPasswordMethod;
    if ((getSetPasswordMethod = VolumeServiceGrpc.getSetPasswordMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetPasswordMethod = VolumeServiceGrpc.getSetPasswordMethod) == null) {
          VolumeServiceGrpc.getSetPasswordMethod = getSetPasswordMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SetPasswordRequest, org.opendedup.grpc.SetPasswordResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetPassword"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetPasswordRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SetPasswordResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetPassword"))
              .build();
        }
      }
    }
    return getSetPasswordMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SpeedRequest,
      org.opendedup.grpc.SpeedResponse> getSetReadSpeedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetReadSpeed",
      requestType = org.opendedup.grpc.SpeedRequest.class,
      responseType = org.opendedup.grpc.SpeedResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SpeedRequest,
      org.opendedup.grpc.SpeedResponse> getSetReadSpeedMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SpeedRequest, org.opendedup.grpc.SpeedResponse> getSetReadSpeedMethod;
    if ((getSetReadSpeedMethod = VolumeServiceGrpc.getSetReadSpeedMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetReadSpeedMethod = VolumeServiceGrpc.getSetReadSpeedMethod) == null) {
          VolumeServiceGrpc.getSetReadSpeedMethod = getSetReadSpeedMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SpeedRequest, org.opendedup.grpc.SpeedResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetReadSpeed"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SpeedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SpeedResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetReadSpeed"))
              .build();
        }
      }
    }
    return getSetReadSpeedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SpeedRequest,
      org.opendedup.grpc.SpeedResponse> getSetWriteSpeedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetWriteSpeed",
      requestType = org.opendedup.grpc.SpeedRequest.class,
      responseType = org.opendedup.grpc.SpeedResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SpeedRequest,
      org.opendedup.grpc.SpeedResponse> getSetWriteSpeedMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SpeedRequest, org.opendedup.grpc.SpeedResponse> getSetWriteSpeedMethod;
    if ((getSetWriteSpeedMethod = VolumeServiceGrpc.getSetWriteSpeedMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSetWriteSpeedMethod = VolumeServiceGrpc.getSetWriteSpeedMethod) == null) {
          VolumeServiceGrpc.getSetWriteSpeedMethod = getSetWriteSpeedMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SpeedRequest, org.opendedup.grpc.SpeedResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetWriteSpeed"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SpeedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SpeedResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SetWriteSpeed"))
              .build();
        }
      }
    }
    return getSetWriteSpeedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SyncFromVolRequest,
      org.opendedup.grpc.SyncFromVolResponse> getSyncFromCloudVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SyncFromCloudVolume",
      requestType = org.opendedup.grpc.SyncFromVolRequest.class,
      responseType = org.opendedup.grpc.SyncFromVolResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SyncFromVolRequest,
      org.opendedup.grpc.SyncFromVolResponse> getSyncFromCloudVolumeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SyncFromVolRequest, org.opendedup.grpc.SyncFromVolResponse> getSyncFromCloudVolumeMethod;
    if ((getSyncFromCloudVolumeMethod = VolumeServiceGrpc.getSyncFromCloudVolumeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSyncFromCloudVolumeMethod = VolumeServiceGrpc.getSyncFromCloudVolumeMethod) == null) {
          VolumeServiceGrpc.getSyncFromCloudVolumeMethod = getSyncFromCloudVolumeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SyncFromVolRequest, org.opendedup.grpc.SyncFromVolResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SyncFromCloudVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SyncFromVolRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SyncFromVolResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SyncFromCloudVolume"))
              .build();
        }
      }
    }
    return getSyncFromCloudVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.opendedup.grpc.SyncVolRequest,
      org.opendedup.grpc.SyncVolResponse> getSyncCloudVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SyncCloudVolume",
      requestType = org.opendedup.grpc.SyncVolRequest.class,
      responseType = org.opendedup.grpc.SyncVolResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.opendedup.grpc.SyncVolRequest,
      org.opendedup.grpc.SyncVolResponse> getSyncCloudVolumeMethod() {
    io.grpc.MethodDescriptor<org.opendedup.grpc.SyncVolRequest, org.opendedup.grpc.SyncVolResponse> getSyncCloudVolumeMethod;
    if ((getSyncCloudVolumeMethod = VolumeServiceGrpc.getSyncCloudVolumeMethod) == null) {
      synchronized (VolumeServiceGrpc.class) {
        if ((getSyncCloudVolumeMethod = VolumeServiceGrpc.getSyncCloudVolumeMethod) == null) {
          VolumeServiceGrpc.getSyncCloudVolumeMethod = getSyncCloudVolumeMethod =
              io.grpc.MethodDescriptor.<org.opendedup.grpc.SyncVolRequest, org.opendedup.grpc.SyncVolResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SyncCloudVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SyncVolRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.opendedup.grpc.SyncVolResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VolumeServiceMethodDescriptorSupplier("SyncCloudVolume"))
              .build();
        }
      }
    }
    return getSyncCloudVolumeMethod;
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
    public void setVolumeCapacity(org.opendedup.grpc.SetVolumeCapacityRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetVolumeCapacityResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetVolumeCapacityMethod(), responseObserver);
    }

    /**
     */
    public void getConnectedVolumes(org.opendedup.grpc.CloudVolumesRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.CloudVolumesResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetConnectedVolumesMethod(), responseObserver);
    }

    /**
     */
    public void getGCSchedule(org.opendedup.grpc.GCScheduleRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GCScheduleResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetGCScheduleMethod(), responseObserver);
    }

    /**
     */
    public void setCacheSize(org.opendedup.grpc.SetCacheSizeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetCacheSizeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetCacheSizeMethod(), responseObserver);
    }

    /**
     */
    public void setPassword(org.opendedup.grpc.SetPasswordRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetPasswordResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetPasswordMethod(), responseObserver);
    }

    /**
     */
    public void setReadSpeed(org.opendedup.grpc.SpeedRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SpeedResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetReadSpeedMethod(), responseObserver);
    }

    /**
     */
    public void setWriteSpeed(org.opendedup.grpc.SpeedRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SpeedResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetWriteSpeedMethod(), responseObserver);
    }

    /**
     */
    public void syncFromCloudVolume(org.opendedup.grpc.SyncFromVolRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SyncFromVolResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSyncFromCloudVolumeMethod(), responseObserver);
    }

    /**
     */
    public void syncCloudVolume(org.opendedup.grpc.SyncVolRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SyncVolResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSyncCloudVolumeMethod(), responseObserver);
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
            getSetVolumeCapacityMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SetVolumeCapacityRequest,
                org.opendedup.grpc.SetVolumeCapacityResponse>(
                  this, METHODID_SET_VOLUME_CAPACITY)))
          .addMethod(
            getGetConnectedVolumesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.CloudVolumesRequest,
                org.opendedup.grpc.CloudVolumesResponse>(
                  this, METHODID_GET_CONNECTED_VOLUMES)))
          .addMethod(
            getGetGCScheduleMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.GCScheduleRequest,
                org.opendedup.grpc.GCScheduleResponse>(
                  this, METHODID_GET_GCSCHEDULE)))
          .addMethod(
            getSetCacheSizeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SetCacheSizeRequest,
                org.opendedup.grpc.SetCacheSizeResponse>(
                  this, METHODID_SET_CACHE_SIZE)))
          .addMethod(
            getSetPasswordMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SetPasswordRequest,
                org.opendedup.grpc.SetPasswordResponse>(
                  this, METHODID_SET_PASSWORD)))
          .addMethod(
            getSetReadSpeedMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SpeedRequest,
                org.opendedup.grpc.SpeedResponse>(
                  this, METHODID_SET_READ_SPEED)))
          .addMethod(
            getSetWriteSpeedMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SpeedRequest,
                org.opendedup.grpc.SpeedResponse>(
                  this, METHODID_SET_WRITE_SPEED)))
          .addMethod(
            getSyncFromCloudVolumeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SyncFromVolRequest,
                org.opendedup.grpc.SyncFromVolResponse>(
                  this, METHODID_SYNC_FROM_CLOUD_VOLUME)))
          .addMethod(
            getSyncCloudVolumeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.opendedup.grpc.SyncVolRequest,
                org.opendedup.grpc.SyncVolResponse>(
                  this, METHODID_SYNC_CLOUD_VOLUME)))
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
    public void setVolumeCapacity(org.opendedup.grpc.SetVolumeCapacityRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetVolumeCapacityResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetVolumeCapacityMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getConnectedVolumes(org.opendedup.grpc.CloudVolumesRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.CloudVolumesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetConnectedVolumesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getGCSchedule(org.opendedup.grpc.GCScheduleRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.GCScheduleResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetGCScheduleMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setCacheSize(org.opendedup.grpc.SetCacheSizeRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetCacheSizeResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetCacheSizeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setPassword(org.opendedup.grpc.SetPasswordRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SetPasswordResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetPasswordMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setReadSpeed(org.opendedup.grpc.SpeedRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SpeedResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetReadSpeedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setWriteSpeed(org.opendedup.grpc.SpeedRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SpeedResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSetWriteSpeedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void syncFromCloudVolume(org.opendedup.grpc.SyncFromVolRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SyncFromVolResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSyncFromCloudVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void syncCloudVolume(org.opendedup.grpc.SyncVolRequest request,
        io.grpc.stub.StreamObserver<org.opendedup.grpc.SyncVolResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSyncCloudVolumeMethod(), getCallOptions()), request, responseObserver);
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
    public org.opendedup.grpc.SetVolumeCapacityResponse setVolumeCapacity(org.opendedup.grpc.SetVolumeCapacityRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetVolumeCapacityMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.CloudVolumesResponse getConnectedVolumes(org.opendedup.grpc.CloudVolumesRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetConnectedVolumesMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.GCScheduleResponse getGCSchedule(org.opendedup.grpc.GCScheduleRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetGCScheduleMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SetCacheSizeResponse setCacheSize(org.opendedup.grpc.SetCacheSizeRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetCacheSizeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SetPasswordResponse setPassword(org.opendedup.grpc.SetPasswordRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetPasswordMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SpeedResponse setReadSpeed(org.opendedup.grpc.SpeedRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetReadSpeedMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SpeedResponse setWriteSpeed(org.opendedup.grpc.SpeedRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetWriteSpeedMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SyncFromVolResponse syncFromCloudVolume(org.opendedup.grpc.SyncFromVolRequest request) {
      return blockingUnaryCall(
          getChannel(), getSyncFromCloudVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.opendedup.grpc.SyncVolResponse syncCloudVolume(org.opendedup.grpc.SyncVolRequest request) {
      return blockingUnaryCall(
          getChannel(), getSyncCloudVolumeMethod(), getCallOptions(), request);
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
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SetVolumeCapacityResponse> setVolumeCapacity(
        org.opendedup.grpc.SetVolumeCapacityRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSetVolumeCapacityMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.CloudVolumesResponse> getConnectedVolumes(
        org.opendedup.grpc.CloudVolumesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetConnectedVolumesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.GCScheduleResponse> getGCSchedule(
        org.opendedup.grpc.GCScheduleRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetGCScheduleMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SetCacheSizeResponse> setCacheSize(
        org.opendedup.grpc.SetCacheSizeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSetCacheSizeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SetPasswordResponse> setPassword(
        org.opendedup.grpc.SetPasswordRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSetPasswordMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SpeedResponse> setReadSpeed(
        org.opendedup.grpc.SpeedRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSetReadSpeedMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SpeedResponse> setWriteSpeed(
        org.opendedup.grpc.SpeedRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSetWriteSpeedMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SyncFromVolResponse> syncFromCloudVolume(
        org.opendedup.grpc.SyncFromVolRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSyncFromCloudVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.opendedup.grpc.SyncVolResponse> syncCloudVolume(
        org.opendedup.grpc.SyncVolRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSyncCloudVolumeMethod(), getCallOptions()), request);
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
        case METHODID_SET_VOLUME_CAPACITY:
          serviceImpl.setVolumeCapacity((org.opendedup.grpc.SetVolumeCapacityRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SetVolumeCapacityResponse>) responseObserver);
          break;
        case METHODID_GET_CONNECTED_VOLUMES:
          serviceImpl.getConnectedVolumes((org.opendedup.grpc.CloudVolumesRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.CloudVolumesResponse>) responseObserver);
          break;
        case METHODID_GET_GCSCHEDULE:
          serviceImpl.getGCSchedule((org.opendedup.grpc.GCScheduleRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.GCScheduleResponse>) responseObserver);
          break;
        case METHODID_SET_CACHE_SIZE:
          serviceImpl.setCacheSize((org.opendedup.grpc.SetCacheSizeRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SetCacheSizeResponse>) responseObserver);
          break;
        case METHODID_SET_PASSWORD:
          serviceImpl.setPassword((org.opendedup.grpc.SetPasswordRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SetPasswordResponse>) responseObserver);
          break;
        case METHODID_SET_READ_SPEED:
          serviceImpl.setReadSpeed((org.opendedup.grpc.SpeedRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SpeedResponse>) responseObserver);
          break;
        case METHODID_SET_WRITE_SPEED:
          serviceImpl.setWriteSpeed((org.opendedup.grpc.SpeedRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SpeedResponse>) responseObserver);
          break;
        case METHODID_SYNC_FROM_CLOUD_VOLUME:
          serviceImpl.syncFromCloudVolume((org.opendedup.grpc.SyncFromVolRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SyncFromVolResponse>) responseObserver);
          break;
        case METHODID_SYNC_CLOUD_VOLUME:
          serviceImpl.syncCloudVolume((org.opendedup.grpc.SyncVolRequest) request,
              (io.grpc.stub.StreamObserver<org.opendedup.grpc.SyncVolResponse>) responseObserver);
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
              .build();
        }
      }
    }
    return result;
  }
}
