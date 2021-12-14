package org.opendedup.sdfs.mgmt.grpc;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import com.google.common.io.BaseEncoding;
import com.sun.management.UnixOperatingSystemMXBean;

import org.json.JSONObject;
import org.opendedup.grpc.Shutdown.ShutdownRequest;
import org.opendedup.grpc.Shutdown.ShutdownResponse;
import org.opendedup.grpc.VolumeServiceGrpc;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.SDFSCli.SdfsPermissions;
import org.opendedup.grpc.SDFSCli.SdfsUser;
import org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.AuthenticationResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.CleanStoreResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.CloudVolumesResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.ConnectedVolumeInfo;
import org.opendedup.grpc.VolumeServiceOuterClass.DSEInfo;
import org.opendedup.grpc.VolumeServiceOuterClass.DSERequest;
import org.opendedup.grpc.VolumeServiceOuterClass.DSEResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.DeleteCloudVolumeResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.GCScheduleResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.SetCacheSizeResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.SetMaxAgeResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.SetPasswordResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.SetVolumeCapacityResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.SpeedRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.SpeedResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.SyncFromVolResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.SyncVolRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.SyncVolResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.SystemInfo;
import org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.SystemInfoResponse;
import org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoRequest;
import org.opendedup.grpc.VolumeServiceOuterClass.VolumeInfoResponse;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.filestore.cloud.RemoteVolumeInfo;
import org.opendedup.sdfs.filestore.gc.ManualGC;
import org.opendedup.sdfs.mgmt.SetCacheSize;
import org.opendedup.sdfs.mgmt.SetReadSpeed;
import org.opendedup.sdfs.mgmt.SetWriteSpeed;
import org.opendedup.sdfs.mgmt.SyncFSCmd;
import org.opendedup.sdfs.mgmt.SyncFromConnectedVolume;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.OSValidator;

import io.grpc.stub.StreamObserver;
import net.sf.jpam.Pam;

class VolumeImpl extends VolumeServiceGrpc.VolumeServiceImplBase {
  private static final int EXPIRY_DAYS = 90;

  private static Pam pam = null;
  static {
    if (!OSValidator.isWindows()) {
      pam = new Pam("login");
    }
  }

  @Override
  public void getVolumeInfo(VolumeInfoRequest req, StreamObserver<VolumeInfoResponse> responseObserver) {

    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.VOLUME_READ)) {
      VolumeInfoResponse.Builder b = VolumeInfoResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    VolumeInfoResponse reply = Main.volume.toProtoc();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void authenticateUser(AuthenticationRequest req, StreamObserver<AuthenticationResponse> responseObserver) {
    AuthenticationResponse.Builder b = AuthenticationResponse.newBuilder();
    try {
      String hash = HashFunctions.getSHAHash(req.getPassword().trim().getBytes(), Main.sdfsPasswordSalt.getBytes());
      // SDFSLogger.getLog().info("username = " + req.getUsername() + " password = " +
      // req.getPassword() + " hash = " + hash );
      if (req.getUsername().equalsIgnoreCase(Main.sdfsUserName) && hash.equals(Main.sdfsPassword)) {
        JSONObject jwtPayload = new JSONObject();
        jwtPayload.put("status", 0);

        jwtPayload.put("sub", req.getUsername());
        SdfsPermissions.Builder permb = SdfsPermissions.newBuilder();
        permb.setADMIN(true);
        jwtPayload.put("aud", BaseEncoding.base64().encode(permb.build().toByteArray()));
        LocalDateTime ldt = LocalDateTime.now().plusDays(EXPIRY_DAYS);
        jwtPayload.put("exp", ldt.toEpochSecond(ZoneOffset.UTC)); // this needs to be configured

        String token = new JWebToken(jwtPayload).toString();
        b.setToken(token);
      } else {
        SdfsUser user = SdfsUserServiceImpl.getUser(req.getUsername());
        if (user == null) {
          b.setError("Unable to Authenticate User " + req.getUsername());
          b.setErrorCode(errorCodes.EACCES);

        } else {
          hash = HashFunctions.getSHAHash(req.getPassword().trim().getBytes(), user.getSalt().getBytes());
          if (!hash.equals(user.getPasswordHash())) {
            b.setError("Unable to Authenticate User");
            b.setErrorCode(errorCodes.EACCES);
            SdfsUserServiceImpl.setLastFailedLogin(req.getUsername());

          } else {
            JSONObject jwtPayload = new JSONObject();
            jwtPayload.put("status", 0);

            jwtPayload.put("sub", req.getUsername());

            jwtPayload.put("aud", BaseEncoding.base64().encode(user.getPermissions().toByteArray()));
            LocalDateTime ldt = LocalDateTime.now().plusDays(EXPIRY_DAYS);
            jwtPayload.put("exp", ldt.toEpochSecond(ZoneOffset.UTC)); // this needs to be configured

            String token = new JWebToken(jwtPayload).toString();
            b.setToken(token);
            SdfsUserServiceImpl.setLastLogin(req.getUsername());
          }
        }
      }
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to authenticat user", e);
      b.setError("Unknown error authenticating user");
      b.setErrorCode(errorCodes.EIO);
    }
    responseObserver.onNext(b.build());
    responseObserver.onCompleted();
  }

  @Override
  public void shutdownVolume(ShutdownRequest req, StreamObserver<ShutdownResponse> responseObserver) {
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
      ShutdownResponse.Builder b = ShutdownResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    ShutdownResponse.Builder b = ShutdownResponse.newBuilder();
    try {
      SDFSLogger.getLog().info("shutting down volume");
      System.out.println("shutting down volume");
      System.exit(0);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }

  }

  @Override
  public void getConnectedVolumes(CloudVolumesRequest request, StreamObserver<CloudVolumesResponse> responseObserver) {
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.VOLUME_READ)) {
      CloudVolumesResponse.Builder b = CloudVolumesResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    CloudVolumesResponse.Builder b = CloudVolumesResponse.newBuilder();
    try {
      RemoteVolumeInfo[] l = FileReplicationService.getConnectedVolumes();
      if (l != null) {
        for (RemoteVolumeInfo vl : l) {
          ConnectedVolumeInfo.Builder info = ConnectedVolumeInfo.newBuilder();
          info.setId(vl.id);
          if (vl.id == Main.DSEID) {
            info.setLocal(true);
          } else {
            info.setLocal(false);
          }
          info.setHostname(vl.hostname);
          info.setPort(vl.port);
          info.setSize(vl.data);
          info.setCompressedSize(vl.compressed);
          if (vl.sdfsversion != null)
            info.setSdfsVersion(vl.sdfsversion);
          info.setLastUpdate(vl.lastupdated);
          info.setVersion(vl.version);
          b.addVolumeInfo(info);
        }
      }
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void setVolumeCapacity(SetVolumeCapacityRequest request,
      StreamObserver<SetVolumeCapacityResponse> responseObserver) {
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
      SetVolumeCapacityResponse.Builder b = SetVolumeCapacityResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    SetVolumeCapacityResponse.Builder b = SetVolumeCapacityResponse.newBuilder();
    try {
      Main.volume.setCapacity(request.getSize(), true);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void getGCSchedule(GCScheduleRequest request, StreamObserver<GCScheduleResponse> responseObserver) {
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_READ)) {
      GCScheduleResponse.Builder b = GCScheduleResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    GCScheduleResponse.Builder b = GCScheduleResponse.newBuilder();
    try {
      b.setSchedule(Main.fDkiskSchedule);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void setPassword(SetPasswordRequest request, StreamObserver<SetPasswordResponse> responseObserver) {
    if (Main.sdfsCliRequireAuth
        && !IOServer.AuthorizationInterceptor.USER_IDENTITY.get().getSubject().equals("admin")) {
      SetPasswordResponse.Builder b = SetPasswordResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } else {
      SetPasswordResponse.Builder b = SetPasswordResponse.newBuilder();
      String oldPassword = Main.sdfsPassword;
      String oeCloudSecretKey = Main.eCloudSecretKey;
      try {
        String newPassword = request.getPassword();

        String password = HashFunctions.getSHAHash(newPassword.getBytes(), Main.sdfsPasswordSalt.getBytes());
        Main.sdfsPassword = password;

        if (Main.eCloudSecretKey != null) {
          byte[] ec = EncryptUtils.encryptCBC(Main.cloudSecretKey.getBytes(), newPassword, Main.chunkStoreEncryptionIV);
          Main.eCloudSecretKey = BaseEncoding.base64Url().encode(ec);
        }
        Main.volume.writeUpdate();
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
      } catch (Exception e) {
        SDFSLogger.getLog().error("password could not be changed" + e.toString(), e);
        Main.sdfsPassword = oldPassword;
        Main.eCloudSecretKey = oeCloudSecretKey;
        b.setError("unable to fulfill request");
        b.setErrorCode(errorCodes.EIO);
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
      }
    }

  }

  @Override
  public void setReadSpeed(SpeedRequest request, StreamObserver<SpeedResponse> responseObserver) {
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
      SpeedResponse.Builder b = SpeedResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    SpeedResponse.Builder b = SpeedResponse.newBuilder();
    try {
      SetReadSpeed r = new SetReadSpeed();
      r.setSpeed(request.getRequestedSpeed());
      b.setEventID(r.evt.uid);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void setWriteSpeed(SpeedRequest request, StreamObserver<SpeedResponse> responseObserver) {
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
      SpeedResponse.Builder b = SpeedResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    SpeedResponse.Builder b = SpeedResponse.newBuilder();
    try {
      SetWriteSpeed r = new SetWriteSpeed();
      r.setSpeed(request.getRequestedSpeed());
      b.setEventID(r.evt.uid);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void syncFromCloudVolume(SyncFromVolRequest request, StreamObserver<SyncFromVolResponse> responseObserver) {
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
      SyncFromVolResponse.Builder b = SyncFromVolResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    SyncFromVolResponse.Builder b = SyncFromVolResponse.newBuilder();
    try {
      SyncFromConnectedVolume v = new SyncFromConnectedVolume();
      v.syncVolume(request.getVolumeid());
      b.setEventID(v.evt.uid);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void syncCloudVolume(SyncVolRequest request, StreamObserver<SyncVolResponse> responseObserver) {
    if (Main.sdfsCliRequireAuth && !AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
      SyncVolResponse.Builder b = SyncVolResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    SyncVolResponse.Builder b = SyncVolResponse.newBuilder();
    try {
      SyncFSCmd f = new SyncFSCmd();
      f.getResult();
      b.setEventID(f.evt.uid);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void setCacheSize(SetCacheSizeRequest request, StreamObserver<SetCacheSizeResponse> responseObserver) {
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
      SetCacheSizeResponse.Builder b = SetCacheSizeResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    SetCacheSizeResponse.Builder b = SetCacheSizeResponse.newBuilder();
    try {
      SetCacheSize scz = new SetCacheSize();
      scz.setCache(request.getCacheSize());
      b.setEventID(scz.evt.uid);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }

  }

  @Override
  public void systemInfo(SystemInfoRequest request, StreamObserver<SystemInfoResponse> responseObserver) {
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_READ)) {
      SystemInfoResponse.Builder b = SystemInfoResponse.newBuilder();
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    SystemInfoResponse.Builder b = SystemInfoResponse.newBuilder();
    SystemInfo.Builder info = SystemInfo.newBuilder();
    try {
      info.setActiveThreads(Thread.activeCount());
      File f = new File(Main.chunkStore);
      info.setTotalSpace(f.getTotalSpace());
      info.setFreeSpace(f.getFreeSpace());
      if (OSValidator.isUnix()) {
        UnixOperatingSystemMXBean perf = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        info.setTotalCpuLoad(perf.getSystemLoadAverage());
        info.setSdfsCpuLoad(perf.getProcessCpuLoad());

      }
      info.setFreeMemory(Runtime.getRuntime().maxMemory());
      info.setFreeMemory(Runtime.getRuntime().freeMemory());
      b.setInfo(info);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }

  }

  @Override
  public void setMaxAge(SetMaxAgeRequest request, StreamObserver<SetMaxAgeResponse> responseObserver) {
    SetMaxAgeResponse.Builder b = SetMaxAgeResponse.newBuilder();
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    Main.maxAge = request.getMaxAge();
    responseObserver.onNext(b.build());
    responseObserver.onCompleted();

  }

  @Override
  public void dSEInfo(DSERequest request, StreamObserver<DSEResponse> responseObserver) {
    DSEResponse.Builder b = DSEResponse.newBuilder();
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_READ)) {
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    DSEInfo.Builder info = DSEInfo.newBuilder();
    try {
      info.setMaxSize(HCServiceProxy.getMaxSize() * HashFunctionPool.avg_page_size);
      info.setCurrentSize(HCServiceProxy.getChunkStore().size());
      info.setEntries(HCServiceProxy.getSize());
      info.setCompressedSize(HCServiceProxy.getChunkStore().compressedSize());
      info.setFreeBlocks(HCServiceProxy.getFreeBlocks());
      info.setPageSize(HCServiceProxy.getPageSize());
      info.setStorageType(Main.chunkStoreClass);
      info.setListenPort(Main.sdfsCliPort);
      info.setListenHost(Main.sdfsCliListenAddr);
      info.setReadSpeed(HCServiceProxy.getReadSpeed());
      info.setWriteSpeed(HCServiceProxy.getWriteSpeed());
      info.setCacheSize(HCServiceProxy.getCacheSize());
      info.setMaxCacheSize(HCServiceProxy.getMaxCacheSize());
      info.setListenEncrypted(Main.sdfsCliSSL);
      if(Main.eChunkStoreEncryptionKey != null) {
        info.setEncryptionKey(Main.eChunkStoreEncryptionKey);
      } else {
      info.setEncryptionKey(Main.chunkStoreEncryptionKey);
      }
      info.setEncryptionIV(Main.chunkStoreEncryptionIV);
      info.setBucketName(HCServiceProxy.getChunkStore().getName());
      info.setMaxAge(Main.maxAge);
      if (Main.cloudAccessKey != null)
        info.setCloudAccessKey(Main.cloudAccessKey);
      if (Main.cloudSecretKey != null)
        info.setCloudSecretKey(Main.cloudSecretKey);
      info.build();
      b.setInfo(info);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void cleanStore(CleanStoreRequest request, StreamObserver<CleanStoreResponse> responseObserver) {

    CleanStoreResponse.Builder b = CleanStoreResponse.newBuilder();
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    Thread th = new Thread(new RunCleanStore(request.getCompact()));
    th.start();
    try {
      while (ManualGC.evt == null) {
        Thread.sleep(1);
      }
      b.setEventID(ManualGC.evt.uid);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void deleteCloudVolume(DeleteCloudVolumeRequest request,
      StreamObserver<DeleteCloudVolumeResponse> responseObserver) {

    DeleteCloudVolumeResponse.Builder b = DeleteCloudVolumeResponse.newBuilder();
    if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
      b.setError("User is not a member of any group with access");
      b.setErrorCode(errorCodes.EACCES);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
      return;
    }
    org.opendedup.sdfs.notification.SDFSEvent evt = org.opendedup.sdfs.notification.SDFSEvent
        .deleteCloudVolumeEvent(request.getPvolumeID());
    Thread th = new Thread(new DeleteCloudVolume(evt, request.getPvolumeID()));
    th.start();
    try {
      b.setEventID(evt.uid);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  protected static class RunCleanStore implements Runnable {
    boolean compact = false;

    protected RunCleanStore(boolean compact) {
      this.compact = compact;
    }

    @Override
    public void run() {
      try {

        long chunks = ManualGC.clearChunks(compact);

        SDFSLogger.getLog().info("Expunged [" + chunks + "] unclaimed compact [" + this.compact + "]");
      } catch (Exception e) {
        SDFSLogger.getLog().error(
            "ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in because :" + e.toString(),
            e);

      }

    }

  }

  protected static class DeleteCloudVolume implements Runnable {
    org.opendedup.sdfs.notification.SDFSEvent evt;
    long volumeid;

    protected DeleteCloudVolume(org.opendedup.sdfs.notification.SDFSEvent evt, long volumeid) {
      this.evt = evt;
      this.volumeid = volumeid;
    }

    @Override
    public void run() {
      try {
        RemoteVolumeInfo[] l = FileReplicationService.getConnectedVolumes();
        for (RemoteVolumeInfo lv : l) {
          if (lv.id == volumeid) {
            FileReplicationService.removeVolume(volumeid);
            evt.endEvent();
            return;
          }
        }
        evt.endEvent("volume [" + volumeid + "] not found", org.opendedup.sdfs.notification.SDFSEvent.WARN);
      } catch (Exception e) {
        SDFSLogger.getLog().error("unable to fulfill request", e);
        evt.endEvent("unable to fulfill request", org.opendedup.sdfs.notification.SDFSEvent.ERROR);
      }

    }
  }

}
