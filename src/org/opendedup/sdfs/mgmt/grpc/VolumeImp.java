package org.opendedup.sdfs.mgmt.grpc;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import com.google.common.io.BaseEncoding;
import com.sun.management.UnixOperatingSystemMXBean;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opendedup.grpc.Shutdown.ShutdownRequest;
import org.opendedup.grpc.Shutdown.ShutdownResponse;
import org.opendedup.grpc.VolumeServiceGrpc;
import org.opendedup.grpc.FileInfo.errorCodes;
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

class VolumeImpl extends VolumeServiceGrpc.VolumeServiceImplBase {
  private static final int EXPIRY_DAYS = 90;

  @Override
  public void getVolumeInfo(VolumeInfoRequest req, StreamObserver<VolumeInfoResponse> responseObserver) {
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

        JSONArray audArray = new JSONArray();
        audArray.put("admin");
        jwtPayload.put("sub", req.getUsername());

        jwtPayload.put("aud", audArray);
        LocalDateTime ldt = LocalDateTime.now().plusDays(EXPIRY_DAYS);
        jwtPayload.put("exp", ldt.toEpochSecond(ZoneOffset.UTC)); // this needs to be configured

        String token = new JWebToken(jwtPayload).toString();
        b.setToken(token);
      } else {
        b.setError("Unable to Authenticate User");
        b.setErrorCode(errorCodes.EACCES);
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
          if(vl.sdfsversion != null)
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

  @Override
  public void setReadSpeed(SpeedRequest request, StreamObserver<SpeedResponse> responseObserver) {
    SpeedResponse.Builder b = SpeedResponse.newBuilder();
    try {
      SetReadSpeed r = new SetReadSpeed();
      r.setSpeed(request.getRequestedSpeed());
      b.setEventID(r.evt.uid);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void setWriteSpeed(SpeedRequest request, StreamObserver<SpeedResponse> responseObserver) {
    SpeedResponse.Builder b = SpeedResponse.newBuilder();
    try {
      SetWriteSpeed r = new SetWriteSpeed();
      r.setSpeed(request.getRequestedSpeed());
      b.setEventID(r.evt.uid);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }catch (Exception e) {
      SDFSLogger.getLog().error("unable to fulfill request", e);
      b.setError("unable to fulfill request");
      b.setErrorCode(errorCodes.EIO);
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void syncFromCloudVolume(SyncFromVolRequest request, StreamObserver<SyncFromVolResponse> responseObserver) {
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
  public void dSEInfo(DSERequest request, StreamObserver<DSEResponse> responseObserver) {
    DSEResponse.Builder b = DSEResponse.newBuilder();
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
      info.setEncryptionKey(Main.chunkStoreEncryptionKey);
      info.setEncryptionIV(Main.chunkStoreEncryptionIV);
      info.setBucketName(HCServiceProxy.getChunkStore().getName());
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
    Thread th = new Thread(new RunCleanStore(request.getCompact()));
    CleanStoreResponse.Builder b = CleanStoreResponse.newBuilder();
    th.start();
    try {
      while(ManualGC.evt == null) {
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
    org.opendedup.sdfs.notification.SDFSEvent evt = org.opendedup.sdfs.notification.SDFSEvent
        .deleteCloudVolumeEvent(request.getVolumeid());
    Thread th = new Thread(new DeleteCloudVolume(evt, request.getVolumeid()));
    DeleteCloudVolumeResponse.Builder b = DeleteCloudVolumeResponse.newBuilder();
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
