// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Shutdown.proto

package org.opendedup.grpc;

public final class Shutdown {
  private Shutdown() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_org_opendedup_grpc_ShutdownRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_org_opendedup_grpc_ShutdownRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_org_opendedup_grpc_ShutdownResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_org_opendedup_grpc_ShutdownResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\016Shutdown.proto\022\022org.opendedup.grpc\032\016Fi" +
      "leInfo.proto\"\021\n\017ShutdownRequest\"T\n\020Shutd" +
      "ownResponse\022\r\n\005error\030\001 \001(\t\0221\n\terrorCode\030" +
      "\004 \001(\0162\036.org.opendedup.grpc.errorCodesB\002P" +
      "\001b\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          org.opendedup.grpc.FileInfo.getDescriptor(),
        });
    internal_static_org_opendedup_grpc_ShutdownRequest_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_org_opendedup_grpc_ShutdownRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_org_opendedup_grpc_ShutdownRequest_descriptor,
        new java.lang.String[] { });
    internal_static_org_opendedup_grpc_ShutdownResponse_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_org_opendedup_grpc_ShutdownResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_org_opendedup_grpc_ShutdownResponse_descriptor,
        new java.lang.String[] { "Error", "ErrorCode", });
    org.opendedup.grpc.FileInfo.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
