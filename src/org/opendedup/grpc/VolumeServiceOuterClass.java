// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: VolumeService.proto

package org.opendedup.grpc;

public final class VolumeServiceOuterClass {
  private VolumeServiceOuterClass() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_org_opendedup_grpc_VolumeInfoRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_org_opendedup_grpc_VolumeInfoRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_org_opendedup_grpc_VolumeInfoResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_org_opendedup_grpc_VolumeInfoResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_org_opendedup_grpc_MessageQueue_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_org_opendedup_grpc_MessageQueue_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\023VolumeService.proto\022\022org.opendedup.grp" +
      "c\"\023\n\021VolumeInfoRequest\"\211\005\n\022VolumeInfoRes" +
      "ponse\022\014\n\004path\030\001 \001(\t\022\014\n\004name\030\002 \001(\t\022\023\n\013cur" +
      "rentSize\030\003 \001(\003\022\021\n\tcapactity\030\004 \001(\003\022\031\n\021max" +
      "PercentageFull\030\005 \001(\001\022\026\n\016duplicateBytes\030\006" +
      " \001(\003\022\021\n\treadBytes\030\007 \001(\001\022\022\n\nwriteBytes\030\010 " +
      "\001(\003\022\024\n\014serialNumber\030\t \001(\003\022\017\n\007dseSize\030\n \001" +
      "(\003\022\023\n\013dseCompSize\030\013 \001(\003\022\017\n\007readOps\030\014 \001(\001" +
      "\022\020\n\010writeOps\030\r \001(\001\022\022\n\nreadErrors\030\016 \001(\003\022\023" +
      "\n\013writeErrors\030\017 \001(\003\022\r\n\005files\030\020 \001(\003\022\030\n\020cl" +
      "osedGracefully\030\021 \001(\010\022\032\n\022allowExternalLin" +
      "ks\030\022 \001(\010\022\022\n\nusePerfMon\030\023 \001(\010\022\021\n\tclusterI" +
      "d\030\024 \001(\t\022\027\n\017VolumeClustered\030\025 \001(\010\022\032\n\022read" +
      "TimeoutSeconds\030\026 \001(\005\022\033\n\023writeTimeoutSeco" +
      "nds\030\027 \001(\005\022\032\n\022compressedMetaData\030\030 \001(\010\022\021\n" +
      "\tsyncFiles\030\031 \001(\010\022\023\n\013maxPageSize\030\032 \001(\003\0226\n" +
      "\014messageQueue\030\033 \003(\0132 .org.opendedup.grpc" +
      ".MessageQueue\022\023\n\013perfMonFile\030\034 \001(\t\"\254\001\n\014M" +
      "essageQueue\022\020\n\010hostName\030\001 \001(\t\0227\n\006mqType\030" +
      "\002 \001(\0162\'.org.opendedup.grpc.MessageQueue." +
      "MQType\022\014\n\004port\030\003 \001(\005\022\r\n\005topic\030\004 \001(\t\022\020\n\010a" +
      "uthInfo\030\005 \001(\t\"\"\n\006MQType\022\014\n\010RabbitMQ\020\000\022\n\n" +
      "\006PubSub\020\0012o\n\rVolumeService\022^\n\rGetVolumeI" +
      "nfo\022%.org.opendedup.grpc.VolumeInfoReque" +
      "st\032&.org.opendedup.grpc.VolumeInfoRespon" +
      "seB\002P\001b\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_org_opendedup_grpc_VolumeInfoRequest_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_org_opendedup_grpc_VolumeInfoRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_org_opendedup_grpc_VolumeInfoRequest_descriptor,
        new java.lang.String[] { });
    internal_static_org_opendedup_grpc_VolumeInfoResponse_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_org_opendedup_grpc_VolumeInfoResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_org_opendedup_grpc_VolumeInfoResponse_descriptor,
        new java.lang.String[] { "Path", "Name", "CurrentSize", "Capactity", "MaxPercentageFull", "DuplicateBytes", "ReadBytes", "WriteBytes", "SerialNumber", "DseSize", "DseCompSize", "ReadOps", "WriteOps", "ReadErrors", "WriteErrors", "Files", "ClosedGracefully", "AllowExternalLinks", "UsePerfMon", "ClusterId", "VolumeClustered", "ReadTimeoutSeconds", "WriteTimeoutSeconds", "CompressedMetaData", "SyncFiles", "MaxPageSize", "MessageQueue", "PerfMonFile", });
    internal_static_org_opendedup_grpc_MessageQueue_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_org_opendedup_grpc_MessageQueue_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_org_opendedup_grpc_MessageQueue_descriptor,
        new java.lang.String[] { "HostName", "MqType", "Port", "Topic", "AuthInfo", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
