package org.opendedup.sdfs.mgmt.grpc;

import java.io.ByteArrayOutputStream;
import java.io.FileFilter;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.nio.file.Files;

import javax.xml.bind.DatatypeConverter;

import com.google.protobuf.ByteString;

import org.opendedup.grpc.EncryptionServiceGrpc.EncryptionServiceImplBase;
import org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertRequest;
import org.opendedup.grpc.EncryptionServiceOuterClass.DeleteExportedCertResponse;
import org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyRequest;
import org.opendedup.grpc.EncryptionServiceOuterClass.EncryptionKeyVerifyResponse;
import org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertRequest;
import org.opendedup.grpc.EncryptionServiceOuterClass.ExportServerCertResponse;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

import io.grpc.stub.StreamObserver;

public class EncryptionService extends EncryptionServiceImplBase implements Runnable {
    CertificateFactory certFactory = null;
    private HashSet<String> keys = new HashSet<String>();
    private final String trustStoreDir;
    private final ReentrantReadWriteLock hl = new ReentrantReadWriteLock();
    private final PrivateKey pvtKey;
    private final X509Certificate serverCertChain;
    String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
    String certChainFilePath = keydir + File.separator + "tls_key.pem";
    String privateKeyFilePath = keydir + File.separator + "tls_key.key";

    public EncryptionService(String trustStoreDir, PrivateKey pvtKey, X509Certificate serverCertChain)
            throws Exception {
        super();
        this.trustStoreDir = trustStoreDir;
        this.pvtKey = pvtKey;
        this.serverCertChain = serverCertChain;
        this.loadTrustManager();
        Thread th = new Thread(this);
        th.start();
    }

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
            this.hl.readLock().lock();
            boolean found;

            found = this.keys.contains(request.getHash());
            if (!found) {
                this.hl.readLock().unlock();
                try {
                    this.hl.writeLock().lock();
                    found = this.keys.contains(request.getHash());
                    if (!found) {
                        this.loadTrustManager();
                        found = this.keys.contains(request.getHash());
                    }
                } finally {
                    this.hl.writeLock().unlock();
                    this.hl.readLock().lock();
                }
            }
            b.setAccept(found);
        } catch (NullPointerException e) {
            b.setError(e.getMessage());
            b.setErrorCode(errorCodes.ENOENT);
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to serialize message", e);
            b.setError("unable to serialize message");
            b.setErrorCode(errorCodes.EIO);
        } finally {
            this.hl.readLock().unlock();
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
        return;
    }

    @Override
    public void exportServerCertificate(ExportServerCertRequest request,
            StreamObserver<ExportServerCertResponse> responseObserver) {
        ExportServerCertResponse.Builder b = ExportServerCertResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.ENCRYPTION_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        try {
            this.hl.writeLock().lock();
            new File(keydir).mkdirs();
            ByteArrayOutputStream fout = new ByteArrayOutputStream();
            try {
                writeKey(fout, pvtKey);
                b.setPrivateKey(ByteString.copyFrom(fout.toByteArray()));
            } catch (Exception e) {
                SDFSLogger.getLog().error(e);
            }
            fout = new ByteArrayOutputStream();
            try {
                writeCertificate(fout, serverCertChain);
                b.setCertChain(ByteString.copyFrom(fout.toByteArray()));
            } catch (Exception e) {
                SDFSLogger.getLog().error(e);
            }
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to serialize message", e);
            b.setError("unable to serialize message");
            b.setErrorCode(errorCodes.EIO);
        } finally {
            this.hl.writeLock().unlock();
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
        return;
    }

    @Override
    public void deleteExportedCert(DeleteExportedCertRequest request,
            StreamObserver<DeleteExportedCertResponse> responseObserver) {
        DeleteExportedCertResponse.Builder b = DeleteExportedCertResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.ENCRYPTION_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        try {
            this.hl.writeLock().lock();
            File fp = new File(privateKeyFilePath);
            File sc = new File(certChainFilePath);
            SDFSLogger.getLog().info(" sdc =" + sc.exists() + " fpc=" + fp.exists());

        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to delete server keys", e);
            b.setError("unable to delete keys");
            b.setErrorCode(errorCodes.EIO);
        } finally {
            this.hl.writeLock().unlock();
        }

    }

    static void writeCertificate(OutputStream out, X509Certificate crt) throws Exception {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write("-----BEGIN CERTIFICATE-----\r\n".getBytes());
        writeBufferBase64(baos, crt.getEncoded());
        baos.write("-----END CERTIFICATE-----\r\n".getBytes());
        out.write(baos.toByteArray());
        out.flush();
        out.close();
    }

    static void writeKey(OutputStream out, PrivateKey pk) throws Exception {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String fmt = pk.getFormat();
        if ("PKCS#8".equals(fmt)) {
            baos.write("-----BEGIN PRIVATE KEY-----\r\n".getBytes());
            writeBufferBase64(baos, pk.getEncoded());
            baos.write("-----END PRIVATE KEY-----\r\n".getBytes());
        } else if ("PKCS#1".equals(fmt)) {
            baos.write("-----BEGIN RSA PRIVATE KEY-----\r\n".getBytes());
            writeBufferBase64(baos, pk.getEncoded());
            baos.write("-----END RSA PRIVATE KEY-----\r\n".getBytes());
        }
        out.write(baos.toByteArray());
        out.flush();
        out.close();
    }

    static void writeBufferBase64(OutputStream out, byte[] bufIn) throws IOException {
        final byte[] buf = DatatypeConverter.printBase64Binary(bufIn).getBytes();
        final int BLOCK_SIZE = 64;
        for (int i = 0; i < buf.length; i += BLOCK_SIZE) {
            out.write(buf, i, Math.min(BLOCK_SIZE, buf.length - i));
            out.write('\r');
            out.write('\n');
        }
    }

    @Override
    public void run() {
        int k = 0;
        for (;;) {
            try {
                Thread.sleep(15 * 1000);
                if (k == 60) {
                    loadTrustManager();
                    k = 0;
                }
                k++;
                long tm = System.currentTimeMillis() - (15 * 1000);
                try {
                    this.hl.writeLock().lock();
                    File fp = new File(privateKeyFilePath);
                    File sc = new File(certChainFilePath);
                    if (sc.exists() && sc.lastModified() < tm) {
                        sc.delete();
                    }
                    if (fp.exists() && fp.lastModified() < tm) {
                        fp.delete();
                    }

                } catch (Exception e) {
                    SDFSLogger.getLog().error("unable to delete server keys", e);
                } finally {
                    this.hl.writeLock().unlock();
                }
            } catch (InterruptedException e) {

            } catch (Exception e) {
                SDFSLogger.getLog().error("error in EncryptionService Thread", e);
            }
        }

    }

    private String getThumbprint(X509Certificate cert)
            throws NoSuchAlgorithmException, CertificateEncodingException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] der = cert.getEncoded();
        md.update(der);
        byte[] digest = md.digest();
        String digestHex = DatatypeConverter.printHexBinary(digest);
        return digestHex.toLowerCase();
    }

    private void loadTrustManager() throws Exception {
        try {

            hl.writeLock().lock();
            keys.clear();
            File trustedCertificatesDir = new File(this.trustStoreDir);
            SDFSLogger.getLog().debug("Load Trust Manager from " + trustedCertificatesDir.getPath());

            // Implementing FileFilter to retrieve only the files in the directory
            FileFilter fileFilter = new FileFilter() {
                @Override
                public boolean accept(File file) {
                    if (file.isDirectory()) {
                        SDFSLogger.getLog().debug("Directory, skipping it to add in TrustStore.");
                        return false;
                    } else {
                        return true;
                    }
                }
            };

            File[] trustedCertFiles = trustedCertificatesDir.listFiles(fileFilter);

            if (trustedCertFiles == null) {
                throw new RuntimeException(
                        "Could not find or list files in trusted directory: " + trustedCertificatesDir);
            } else if (trustedCertFiles.length == 0) {
                throw new RuntimeException(
                        "Client auth is required but no trust anchors found in: " + trustedCertificatesDir);
            }

            for (File trustedCertFile : trustedCertFiles) {
                CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
                try (InputStream input = Files.newInputStream(trustedCertFile.toPath())) {
                    while (input.available() > 0) {
                        try {
                            X509Certificate cert = (X509Certificate) certificateFactory.generateCertificate(input);
                            keys.add(this.getThumbprint(cert));
                        } catch (CertificateException e) {
                            SDFSLogger.getLog().debug("Not a certificate file, skipping it to add in TrustStore.");
                            continue;
                        } catch (Exception e) {
                            throw new CertificateException("Error loading certificate file: " + trustedCertFile, e);
                        }
                    }
                }
            }
        } finally {
            hl.writeLock().unlock();
        }
    }

}