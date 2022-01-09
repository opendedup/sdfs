package org.opendedup.sdfs.mgmt.grpc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import org.opendedup.grpc.EncryptionServiceGrpc.EncryptionServiceImplBase;
import org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest;
import org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.logging.SDFSLogger;

import io.grpc.stub.StreamObserver;



public class EncryptionService extends EncryptionServiceImplBase {
    CertificateFactory certFactory = null;
    @Override
    public void validateCertificate(EncryptionKeyVerifyRequest request,
            StreamObserver<EncryptionKeyVerifyResponse> responseObserver) {
                EncryptionKeyVerifyResponse.Builder b = EncryptionKeyVerifyResponse.newBuilder();
                if (!AuthUtils.validateUser(AuthUtils.ACTIONS.EVENT_READ)) {
                    b.setError("User is not a member of any group with access");
                    b.setErrorCode(errorCodes.EACCES);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                try {
                    if (certFactory == null) {
                       certFactory= CertificateFactory.getInstance("X.509");
                    }
                    byte [] bt = new byte[request.getData().size()];
                    request.getData().copyTo(bt, 0);
                    InputStream in = new ByteArrayInputStream(bt);
                    X509Certificate cert = (X509Certificate)certFactory.generateCertificate(in);
                    SDFSLogger.getLog().info("Recieved Cert " + cert.getSubjectDN());
                    b.setAccept(true);
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



}