package org.opendedup.sdfs.mgmt.grpc;

import com.google.common.eventbus.Subscribe;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListRequest;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventListResponse;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse;
import org.opendedup.grpc.SDFSEventServiceGrpc;
import org.opendedup.grpc.FileInfo.errorCodes;
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
            
            try {
                evt = org.opendedup.sdfs.notification.SDFSEvent.getEvent(request.getUuid());
            } catch (NullPointerException e) {
                b.setError(e.getMessage());
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            l= new SDFSEventListener(evt, responseObserver, b);
            evt.registerListener(l);
            while (!evt.isDone()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
            }
           
            int i = 0;
            while (!l.evtsent) {
                Thread.sleep(100);
                i++;
                if(i>10) {
                    break;
                }
            }
            if (!l.evtsent) {
                
                b.setEvent(evt.toProtoBuf());
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        } catch (Exception e) {
            SDFSLogger.getLog().error("Unable to listen for event", e);
            b.setError(e.getMessage());
            b.setErrorCode(errorCodes.EIO);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } finally {
            if(evt != null && l != null) {
                evt.unregisterListener(l);
            }
        }

    }

    public class SDFSEventListener {

        org.opendedup.sdfs.notification.SDFSEvent evt = null;
        StreamObserver<SDFSEventResponse> responseObserver;
        SDFSEventResponse.Builder b;
        boolean evtsent;

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
            evtsent = true;
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