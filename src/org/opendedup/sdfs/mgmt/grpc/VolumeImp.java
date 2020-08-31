package org.opendedup.sdfs.mgmt.grpc;


import io.grpc.stub.StreamObserver;
import org.opendedup.grpc.*;
import org.opendedup.hashing.HashFunctions;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

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
        //SDFSLogger.getLog().info("username = " + req.getUsername() + " password = " + req.getPassword() + " hash = " + hash );
        if (req.getUsername().equalsIgnoreCase(Main.sdfsUserName) && hash.equals(Main.sdfsPassword)) {
          JSONObject jwtPayload = new JSONObject();
          jwtPayload.put("status", 0);

          JSONArray audArray = new JSONArray();
          audArray.put("admin");
          jwtPayload.put("sub", req.getUsername() );

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

  }

 