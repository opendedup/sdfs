// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: SDFSEvent.proto

package org.opendedup.grpc;

/**
 * Protobuf type {@code org.opendedup.grpc.SDFSEventListResponse}
 */
public final class SDFSEventListResponse extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:org.opendedup.grpc.SDFSEventListResponse)
    SDFSEventListResponseOrBuilder {
private static final long serialVersionUID = 0L;
  // Use SDFSEventListResponse.newBuilder() to construct.
  private SDFSEventListResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private SDFSEventListResponse() {
    error_ = "";
    errorCode_ = 0;
    events_ = java.util.Collections.emptyList();
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new SDFSEventListResponse();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private SDFSEventListResponse(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 10: {
            java.lang.String s = input.readStringRequireUtf8();

            error_ = s;
            break;
          }
          case 16: {
            int rawValue = input.readEnum();

            errorCode_ = rawValue;
            break;
          }
          case 26: {
            if (!((mutable_bitField0_ & 0x00000001) != 0)) {
              events_ = new java.util.ArrayList<org.opendedup.grpc.SDFSEvent>();
              mutable_bitField0_ |= 0x00000001;
            }
            events_.add(
                input.readMessage(org.opendedup.grpc.SDFSEvent.parser(), extensionRegistry));
            break;
          }
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      if (((mutable_bitField0_ & 0x00000001) != 0)) {
        events_ = java.util.Collections.unmodifiableList(events_);
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return org.opendedup.grpc.SDFSEventOuterClass.internal_static_org_opendedup_grpc_SDFSEventListResponse_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return org.opendedup.grpc.SDFSEventOuterClass.internal_static_org_opendedup_grpc_SDFSEventListResponse_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            org.opendedup.grpc.SDFSEventListResponse.class, org.opendedup.grpc.SDFSEventListResponse.Builder.class);
  }

  public static final int ERROR_FIELD_NUMBER = 1;
  private volatile java.lang.Object error_;
  /**
   * <code>string error = 1;</code>
   * @return The error.
   */
  @java.lang.Override
  public java.lang.String getError() {
    java.lang.Object ref = error_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      error_ = s;
      return s;
    }
  }
  /**
   * <code>string error = 1;</code>
   * @return The bytes for error.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getErrorBytes() {
    java.lang.Object ref = error_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      error_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int ERRORCODE_FIELD_NUMBER = 2;
  private int errorCode_;
  /**
   * <code>.org.opendedup.grpc.errorCodes errorCode = 2;</code>
   * @return The enum numeric value on the wire for errorCode.
   */
  @java.lang.Override public int getErrorCodeValue() {
    return errorCode_;
  }
  /**
   * <code>.org.opendedup.grpc.errorCodes errorCode = 2;</code>
   * @return The errorCode.
   */
  @java.lang.Override public org.opendedup.grpc.errorCodes getErrorCode() {
    @SuppressWarnings("deprecation")
    org.opendedup.grpc.errorCodes result = org.opendedup.grpc.errorCodes.valueOf(errorCode_);
    return result == null ? org.opendedup.grpc.errorCodes.UNRECOGNIZED : result;
  }

  public static final int EVENTS_FIELD_NUMBER = 3;
  private java.util.List<org.opendedup.grpc.SDFSEvent> events_;
  /**
   * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
   */
  @java.lang.Override
  public java.util.List<org.opendedup.grpc.SDFSEvent> getEventsList() {
    return events_;
  }
  /**
   * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
   */
  @java.lang.Override
  public java.util.List<? extends org.opendedup.grpc.SDFSEventOrBuilder> 
      getEventsOrBuilderList() {
    return events_;
  }
  /**
   * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
   */
  @java.lang.Override
  public int getEventsCount() {
    return events_.size();
  }
  /**
   * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
   */
  @java.lang.Override
  public org.opendedup.grpc.SDFSEvent getEvents(int index) {
    return events_.get(index);
  }
  /**
   * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
   */
  @java.lang.Override
  public org.opendedup.grpc.SDFSEventOrBuilder getEventsOrBuilder(
      int index) {
    return events_.get(index);
  }

  private byte memoizedIsInitialized = -1;
  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (!getErrorBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, error_);
    }
    if (errorCode_ != org.opendedup.grpc.errorCodes.NOERR.getNumber()) {
      output.writeEnum(2, errorCode_);
    }
    for (int i = 0; i < events_.size(); i++) {
      output.writeMessage(3, events_.get(i));
    }
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getErrorBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, error_);
    }
    if (errorCode_ != org.opendedup.grpc.errorCodes.NOERR.getNumber()) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(2, errorCode_);
    }
    for (int i = 0; i < events_.size(); i++) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(3, events_.get(i));
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof org.opendedup.grpc.SDFSEventListResponse)) {
      return super.equals(obj);
    }
    org.opendedup.grpc.SDFSEventListResponse other = (org.opendedup.grpc.SDFSEventListResponse) obj;

    if (!getError()
        .equals(other.getError())) return false;
    if (errorCode_ != other.errorCode_) return false;
    if (!getEventsList()
        .equals(other.getEventsList())) return false;
    if (!unknownFields.equals(other.unknownFields)) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    hash = (37 * hash) + ERROR_FIELD_NUMBER;
    hash = (53 * hash) + getError().hashCode();
    hash = (37 * hash) + ERRORCODE_FIELD_NUMBER;
    hash = (53 * hash) + errorCode_;
    if (getEventsCount() > 0) {
      hash = (37 * hash) + EVENTS_FIELD_NUMBER;
      hash = (53 * hash) + getEventsList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.opendedup.grpc.SDFSEventListResponse parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @java.lang.Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(org.opendedup.grpc.SDFSEventListResponse prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @java.lang.Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code org.opendedup.grpc.SDFSEventListResponse}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:org.opendedup.grpc.SDFSEventListResponse)
      org.opendedup.grpc.SDFSEventListResponseOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.opendedup.grpc.SDFSEventOuterClass.internal_static_org_opendedup_grpc_SDFSEventListResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.opendedup.grpc.SDFSEventOuterClass.internal_static_org_opendedup_grpc_SDFSEventListResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.opendedup.grpc.SDFSEventListResponse.class, org.opendedup.grpc.SDFSEventListResponse.Builder.class);
    }

    // Construct using org.opendedup.grpc.SDFSEventListResponse.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
        getEventsFieldBuilder();
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      error_ = "";

      errorCode_ = 0;

      if (eventsBuilder_ == null) {
        events_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
      } else {
        eventsBuilder_.clear();
      }
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return org.opendedup.grpc.SDFSEventOuterClass.internal_static_org_opendedup_grpc_SDFSEventListResponse_descriptor;
    }

    @java.lang.Override
    public org.opendedup.grpc.SDFSEventListResponse getDefaultInstanceForType() {
      return org.opendedup.grpc.SDFSEventListResponse.getDefaultInstance();
    }

    @java.lang.Override
    public org.opendedup.grpc.SDFSEventListResponse build() {
      org.opendedup.grpc.SDFSEventListResponse result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public org.opendedup.grpc.SDFSEventListResponse buildPartial() {
      org.opendedup.grpc.SDFSEventListResponse result = new org.opendedup.grpc.SDFSEventListResponse(this);
      int from_bitField0_ = bitField0_;
      result.error_ = error_;
      result.errorCode_ = errorCode_;
      if (eventsBuilder_ == null) {
        if (((bitField0_ & 0x00000001) != 0)) {
          events_ = java.util.Collections.unmodifiableList(events_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.events_ = events_;
      } else {
        result.events_ = eventsBuilder_.build();
      }
      onBuilt();
      return result;
    }

    @java.lang.Override
    public Builder clone() {
      return super.clone();
    }
    @java.lang.Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.setField(field, value);
    }
    @java.lang.Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @java.lang.Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @java.lang.Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @java.lang.Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.addRepeatedField(field, value);
    }
    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof org.opendedup.grpc.SDFSEventListResponse) {
        return mergeFrom((org.opendedup.grpc.SDFSEventListResponse)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(org.opendedup.grpc.SDFSEventListResponse other) {
      if (other == org.opendedup.grpc.SDFSEventListResponse.getDefaultInstance()) return this;
      if (!other.getError().isEmpty()) {
        error_ = other.error_;
        onChanged();
      }
      if (other.errorCode_ != 0) {
        setErrorCodeValue(other.getErrorCodeValue());
      }
      if (eventsBuilder_ == null) {
        if (!other.events_.isEmpty()) {
          if (events_.isEmpty()) {
            events_ = other.events_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureEventsIsMutable();
            events_.addAll(other.events_);
          }
          onChanged();
        }
      } else {
        if (!other.events_.isEmpty()) {
          if (eventsBuilder_.isEmpty()) {
            eventsBuilder_.dispose();
            eventsBuilder_ = null;
            events_ = other.events_;
            bitField0_ = (bitField0_ & ~0x00000001);
            eventsBuilder_ = 
              com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                 getEventsFieldBuilder() : null;
          } else {
            eventsBuilder_.addAllMessages(other.events_);
          }
        }
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      org.opendedup.grpc.SDFSEventListResponse parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (org.opendedup.grpc.SDFSEventListResponse) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object error_ = "";
    /**
     * <code>string error = 1;</code>
     * @return The error.
     */
    public java.lang.String getError() {
      java.lang.Object ref = error_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        error_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string error = 1;</code>
     * @return The bytes for error.
     */
    public com.google.protobuf.ByteString
        getErrorBytes() {
      java.lang.Object ref = error_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        error_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string error = 1;</code>
     * @param value The error to set.
     * @return This builder for chaining.
     */
    public Builder setError(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      error_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string error = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearError() {
      
      error_ = getDefaultInstance().getError();
      onChanged();
      return this;
    }
    /**
     * <code>string error = 1;</code>
     * @param value The bytes for error to set.
     * @return This builder for chaining.
     */
    public Builder setErrorBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      error_ = value;
      onChanged();
      return this;
    }

    private int errorCode_ = 0;
    /**
     * <code>.org.opendedup.grpc.errorCodes errorCode = 2;</code>
     * @return The enum numeric value on the wire for errorCode.
     */
    @java.lang.Override public int getErrorCodeValue() {
      return errorCode_;
    }
    /**
     * <code>.org.opendedup.grpc.errorCodes errorCode = 2;</code>
     * @param value The enum numeric value on the wire for errorCode to set.
     * @return This builder for chaining.
     */
    public Builder setErrorCodeValue(int value) {
      
      errorCode_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>.org.opendedup.grpc.errorCodes errorCode = 2;</code>
     * @return The errorCode.
     */
    @java.lang.Override
    public org.opendedup.grpc.errorCodes getErrorCode() {
      @SuppressWarnings("deprecation")
      org.opendedup.grpc.errorCodes result = org.opendedup.grpc.errorCodes.valueOf(errorCode_);
      return result == null ? org.opendedup.grpc.errorCodes.UNRECOGNIZED : result;
    }
    /**
     * <code>.org.opendedup.grpc.errorCodes errorCode = 2;</code>
     * @param value The errorCode to set.
     * @return This builder for chaining.
     */
    public Builder setErrorCode(org.opendedup.grpc.errorCodes value) {
      if (value == null) {
        throw new NullPointerException();
      }
      
      errorCode_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>.org.opendedup.grpc.errorCodes errorCode = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearErrorCode() {
      
      errorCode_ = 0;
      onChanged();
      return this;
    }

    private java.util.List<org.opendedup.grpc.SDFSEvent> events_ =
      java.util.Collections.emptyList();
    private void ensureEventsIsMutable() {
      if (!((bitField0_ & 0x00000001) != 0)) {
        events_ = new java.util.ArrayList<org.opendedup.grpc.SDFSEvent>(events_);
        bitField0_ |= 0x00000001;
       }
    }

    private com.google.protobuf.RepeatedFieldBuilderV3<
        org.opendedup.grpc.SDFSEvent, org.opendedup.grpc.SDFSEvent.Builder, org.opendedup.grpc.SDFSEventOrBuilder> eventsBuilder_;

    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public java.util.List<org.opendedup.grpc.SDFSEvent> getEventsList() {
      if (eventsBuilder_ == null) {
        return java.util.Collections.unmodifiableList(events_);
      } else {
        return eventsBuilder_.getMessageList();
      }
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public int getEventsCount() {
      if (eventsBuilder_ == null) {
        return events_.size();
      } else {
        return eventsBuilder_.getCount();
      }
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public org.opendedup.grpc.SDFSEvent getEvents(int index) {
      if (eventsBuilder_ == null) {
        return events_.get(index);
      } else {
        return eventsBuilder_.getMessage(index);
      }
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public Builder setEvents(
        int index, org.opendedup.grpc.SDFSEvent value) {
      if (eventsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureEventsIsMutable();
        events_.set(index, value);
        onChanged();
      } else {
        eventsBuilder_.setMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public Builder setEvents(
        int index, org.opendedup.grpc.SDFSEvent.Builder builderForValue) {
      if (eventsBuilder_ == null) {
        ensureEventsIsMutable();
        events_.set(index, builderForValue.build());
        onChanged();
      } else {
        eventsBuilder_.setMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public Builder addEvents(org.opendedup.grpc.SDFSEvent value) {
      if (eventsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureEventsIsMutable();
        events_.add(value);
        onChanged();
      } else {
        eventsBuilder_.addMessage(value);
      }
      return this;
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public Builder addEvents(
        int index, org.opendedup.grpc.SDFSEvent value) {
      if (eventsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureEventsIsMutable();
        events_.add(index, value);
        onChanged();
      } else {
        eventsBuilder_.addMessage(index, value);
      }
      return this;
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public Builder addEvents(
        org.opendedup.grpc.SDFSEvent.Builder builderForValue) {
      if (eventsBuilder_ == null) {
        ensureEventsIsMutable();
        events_.add(builderForValue.build());
        onChanged();
      } else {
        eventsBuilder_.addMessage(builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public Builder addEvents(
        int index, org.opendedup.grpc.SDFSEvent.Builder builderForValue) {
      if (eventsBuilder_ == null) {
        ensureEventsIsMutable();
        events_.add(index, builderForValue.build());
        onChanged();
      } else {
        eventsBuilder_.addMessage(index, builderForValue.build());
      }
      return this;
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public Builder addAllEvents(
        java.lang.Iterable<? extends org.opendedup.grpc.SDFSEvent> values) {
      if (eventsBuilder_ == null) {
        ensureEventsIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, events_);
        onChanged();
      } else {
        eventsBuilder_.addAllMessages(values);
      }
      return this;
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public Builder clearEvents() {
      if (eventsBuilder_ == null) {
        events_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
      } else {
        eventsBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public Builder removeEvents(int index) {
      if (eventsBuilder_ == null) {
        ensureEventsIsMutable();
        events_.remove(index);
        onChanged();
      } else {
        eventsBuilder_.remove(index);
      }
      return this;
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public org.opendedup.grpc.SDFSEvent.Builder getEventsBuilder(
        int index) {
      return getEventsFieldBuilder().getBuilder(index);
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public org.opendedup.grpc.SDFSEventOrBuilder getEventsOrBuilder(
        int index) {
      if (eventsBuilder_ == null) {
        return events_.get(index);  } else {
        return eventsBuilder_.getMessageOrBuilder(index);
      }
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public java.util.List<? extends org.opendedup.grpc.SDFSEventOrBuilder> 
         getEventsOrBuilderList() {
      if (eventsBuilder_ != null) {
        return eventsBuilder_.getMessageOrBuilderList();
      } else {
        return java.util.Collections.unmodifiableList(events_);
      }
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public org.opendedup.grpc.SDFSEvent.Builder addEventsBuilder() {
      return getEventsFieldBuilder().addBuilder(
          org.opendedup.grpc.SDFSEvent.getDefaultInstance());
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public org.opendedup.grpc.SDFSEvent.Builder addEventsBuilder(
        int index) {
      return getEventsFieldBuilder().addBuilder(
          index, org.opendedup.grpc.SDFSEvent.getDefaultInstance());
    }
    /**
     * <code>repeated .org.opendedup.grpc.SDFSEvent events = 3;</code>
     */
    public java.util.List<org.opendedup.grpc.SDFSEvent.Builder> 
         getEventsBuilderList() {
      return getEventsFieldBuilder().getBuilderList();
    }
    private com.google.protobuf.RepeatedFieldBuilderV3<
        org.opendedup.grpc.SDFSEvent, org.opendedup.grpc.SDFSEvent.Builder, org.opendedup.grpc.SDFSEventOrBuilder> 
        getEventsFieldBuilder() {
      if (eventsBuilder_ == null) {
        eventsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
            org.opendedup.grpc.SDFSEvent, org.opendedup.grpc.SDFSEvent.Builder, org.opendedup.grpc.SDFSEventOrBuilder>(
                events_,
                ((bitField0_ & 0x00000001) != 0),
                getParentForChildren(),
                isClean());
        events_ = null;
      }
      return eventsBuilder_;
    }
    @java.lang.Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @java.lang.Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:org.opendedup.grpc.SDFSEventListResponse)
  }

  // @@protoc_insertion_point(class_scope:org.opendedup.grpc.SDFSEventListResponse)
  private static final org.opendedup.grpc.SDFSEventListResponse DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new org.opendedup.grpc.SDFSEventListResponse();
  }

  public static org.opendedup.grpc.SDFSEventListResponse getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<SDFSEventListResponse>
      PARSER = new com.google.protobuf.AbstractParser<SDFSEventListResponse>() {
    @java.lang.Override
    public SDFSEventListResponse parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new SDFSEventListResponse(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<SDFSEventListResponse> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<SDFSEventListResponse> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public org.opendedup.grpc.SDFSEventListResponse getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
