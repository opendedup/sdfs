package org.opendedup.sdfs.mgmt.grpc;

import java.io.IOException;

import com.google.common.eventbus.Subscribe;

import org.opendedup.grpc.SDFSEventListRequest;
import org.opendedup.grpc.SDFSEventListResponse;
import org.opendedup.grpc.SDFSEventRequest;
import org.opendedup.grpc.SDFSEventResponse;
import org.opendedup.grpc.SDFSEventServiceGrpc;
import org.opendedup.grpc.errorCodes;
import org.opendedup.logging.SDFSLogger;

import io.grpc.stub.StreamObserver;

public class SDFSEventImpl extends SDFSEventServiceGrpc.SDFSEventServiceImplBase {

    @Override
    public void getEvent(SDFSEventRequest request, StreamObserver<SDFSEventResponse> responseObserver) {
        SDFSEventResponse.Builder b = SDFSEventResponse.newBuilder();
        try {
            b.setEvent(org.opendedup.sdfs.notification.SDFSEvent.getPotoBufEvent(request.getUuid()));
        } catch (NullPointerException e) {
            b.setError(e.getMessage());
            b.setErrorCode(errorCodes.ENOENT);
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to serialize message", e);
            b.setError("unable to serialize message");
            b.setErrorCode(errorCodes.EIO);
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
        return;
    }

    @Override
    public void subscribeEvent(SDFSEventRequest request, StreamObserver<SDFSEventResponse> responseObserver) {
        SDFSEventResponse.Builder b = SDFSEventResponse.newBuilder();
        org.opendedup.sdfs.notification.SDFSEvent evt = null;
        SDFSEventListener l = null;
        try {
            SDFSLogger.getLog().info("Calling Event Listener");
            
            try {
                SDFSLogger.getLog().info("1");
                evt = org.opendedup.sdfs.notification.SDFSEvent.getEvent(request.getUuid());
                SDFSLogger.getLog().info("2");
            } catch (NullPointerException e) {
                SDFSLogger.getLog().info("oSent Event");
                b.setError(e.getMessage());
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            l= new SDFSEventListener(evt, responseObserver, b);
            SDFSLogger.getLog().info("3");
            evt.registerListener(l);
            SDFSLogger.getLog().info("4");
            while (!evt.isDone()) {
                try {
                    Thread.sleep(100);
                    SDFSLogger.getLog().info("5 " + evt.endTime);
                } catch (InterruptedException e) {
                    SDFSLogger.getLog().info("lSent Event");

                    return;
                }
            }
            SDFSLogger.getLog().info("Done");
        } catch (Exception e) {
            SDFSLogger.getLog().error("Unable to listen for event", e);
            b.setError(e.getMessage());
            b.setErrorCode(errorCodes.EIO);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();

        } finally {
            SDFSLogger.getLog().info("7");
            if(evt != null && l != null) {
                SDFSLogger.getLog().info("8");
                evt.unregisterListener(l);
            }
            SDFSLogger.getLog().info("9");
        }

    }

    public class SDFSEventListener {

        org.opendedup.sdfs.notification.SDFSEvent evt = null;
        StreamObserver<SDFSEventResponse> responseObserver;
        SDFSEventResponse.Builder b;

        public SDFSEventListener(org.opendedup.sdfs.notification.SDFSEvent evt,
                StreamObserver<SDFSEventResponse> responseObserver, SDFSEventResponse.Builder b) {
            this.evt = evt;
            this.responseObserver = responseObserver;
            this.b = b;
            evt.registerListener(this);
        }

        @Subscribe
        public void nvent(org.opendedup.sdfs.notification.SDFSEvent _evt) {

            try {
                b.setEvent(_evt.toProtoBuf());
            } catch (Exception e) {
                SDFSLogger.getLog().info("nSent Event");
                b.setError("Unable to marshal event");
                b.setErrorCode(errorCodes.EIO);
            }
            responseObserver.onNext(b.build());
            SDFSLogger.getLog().info("Sent Event");
            if (_evt.isDone()) {
                responseObserver.onCompleted();
                evt.unregisterListener(this);
            }
        }
    }

    @Override
    public void listEvents(SDFSEventListRequest request, StreamObserver<SDFSEventListResponse> responseObserver) {
        SDFSEventListResponse.Builder b = SDFSEventListResponse.newBuilder();
        try {
            b.addAllEvents(org.opendedup.sdfs.notification.SDFSEvent.getProtoBufEvents());
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to serialize message", e);
            b.setError("unable to serialize message");
            b.setErrorCode(errorCodes.EIO);
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
        return;
    }

}