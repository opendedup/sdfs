package org.opendedup.sdfs.mgmt.grpc;

import io.grpc.stub.StreamObserver;
import org.opendedup.grpc.*;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.HashFunctions;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.filestore.cloud.RemoteVolumeInfo;
import org.opendedup.sdfs.filestore.gc.ManualGC;
import org.opendedup.sdfs.notification.SDFSEvent.Level;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.OSValidator;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import com.sun.management.UnixOperatingSystemMXBean;

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
  public void setLogicalVolumeCapacity(SetLogicalVolumeCapacityRequest request,
      StreamObserver<SetLogicalVolumeCapacityResponse> responseObserver) {
        SetLogicalVolumeCapacityResponse.Builder b = SetLogicalVolumeCapacityResponse.newBuilder();
    try {
      Main.volume.setCapacity(request.getSize(),true);
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
      info.setMaxSize(HCServiceProxy.getMaxSize()
      * HashFunctionPool.avg_page_size));
			info.setCurrentSize(HCServiceProxy.getChunkStore().size());
      info.setEntries(HCServiceProxy.getSize());
      info.setCompressedSize(HCServiceProxy
      .getChunkStore().compressedSize());
      info.setFreeBlocks(HCServiceProxy.getFreeBlocks());
      info.setPageSize(HCServiceProxy.getPageSize());
      info.setStorageType( Main.chunkStoreClass);
      info.setListenPort(Main.sdfsCliPort);
      info.setListenHost(Main.sdfsCliListenAddr);
      info.setReadSpeed(HCServiceProxy.getReadSpeed());
      info.setWriteSpeed(HCServiceProxy.getWriteSpeed());
      info.setCacheSize(HCServiceProxy.getCacheSize());
      info.setMaxCacheSize(HCServiceProxy.getMaxCacheSize());
      info.setListenEncrypted(Main.sdfsCliSSL);
      info.setEncryptionKey(Main.chunkStoreEncryptionKey);
      info.setEncryptionIV(Main.chunkStoreEncryptionIV);
      if(Main.cloudAccessKey != null)
        info.setCloudAccessKey(Main.cloudAccessKey);
      if(Main.cloudSecretKey != null)
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
