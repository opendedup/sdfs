// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: IOService.proto

package org.opendedup.grpc;

public interface UtimeRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:org.opendedup.grpc.UtimeRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string path = 1;</code>
   * @return The path.
   */
  java.lang.String getPath();
  /**
   * <code>string path = 1;</code>
   * @return The bytes for path.
   */
  com.google.protobuf.ByteString
      getPathBytes();

  /**
   * <code>int64 atime = 2;</code>
   * @return The atime.
   */
  long getAtime();

  /**
   * <code>int64 mtime = 3;</code>
   * @return The mtime.
   */
  long getMtime();
}