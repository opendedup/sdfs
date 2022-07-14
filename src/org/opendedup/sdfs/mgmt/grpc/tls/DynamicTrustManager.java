package org.opendedup.sdfs.mgmt.grpc.tls;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendedup.logging.SDFSLogger;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;

public class DynamicTrustManager implements X509TrustManager {
    private static final Log LOG = LogFactory.getLog(DynamicTrustManager.class);
    private X509TrustManager standardTrustManager = null;
    protected File trustedCertificatesDir ;


    /**
     * Constructor for DynamicTrustManager.
     */
    public DynamicTrustManager(String trustStoreDir) throws NoSuchAlgorithmException, KeyStoreException {
        super();
        trustedCertificatesDir = new File(trustStoreDir);
        try {
            loadTrustManager(ClientAuth.REQUIRE);
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            throw new KeyStoreException("Error occurred while setting up trust manager." + e1.getCause(), e1);
        }
    }






    /**
     * @see javax.net.ssl.X509TrustManager#checkClientTrusted(X509Certificate[],
     *      String authType)
     */
    public void checkClientTrusted(X509Certificate[] certificates, String authType) throws CertificateException {
        SDFSLogger.getLog().error("testing 123");
        try {
            SDFSLogger.getLog().info("EASYX509 checkClientTrusted");
            standardTrustManager.checkClientTrusted(certificates, authType);
        } catch (Exception e) {
            SDFSLogger.getLog().info("EASYX509 checkClientTrusted caught exception, try to reload trust mananager.");
            try {
                loadTrustManager(ClientAuth.REQUIRE);
                standardTrustManager.checkClientTrusted(certificates, authType);
            } catch (Exception e1) {
                SDFSLogger.getLog().info("EASYX509 checkClientTrusted caught exception");
                throw new CertificateException("Error occurred while setting up trust manager." + e1.getCause(), e1);
            }
        }
    }

    /**
     * @see javax.net.ssl.X509TrustManager#checkServerTrusted(X509Certificate[],
     *      String authType)
     */
    public void checkServerTrusted(X509Certificate[] certificates, String authType) throws CertificateException {
        SDFSLogger.getLog().error("testing");
        if ((certificates != null) && LOG.isDebugEnabled()) {
            LOG.debug("Server certificate chain:");
            for (int i = 0; i < certificates.length; i++) {
                LOG.debug("X509Certificate[" + i + "]=" + certificates[i]);
            }
        }
        if ((certificates != null) && (certificates.length == 1)) {
            certificates[0].checkValidity();
        } else {
            standardTrustManager.checkServerTrusted(certificates, authType);
        }
    }

    /**
     * @see javax.net.ssl.X509TrustManager#getAcceptedIssuers()
     */
    public X509Certificate[] getAcceptedIssuers() {
        return null;
    }

    public void loadTrustManager(ClientAuth clientAuth) throws Exception {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        File[] trustedCertFiles = trustedCertificatesDir.listFiles();

        if (trustedCertFiles == null) {
            throw new RuntimeException("Could not find or list files in trusted directory: " + trustedCertificatesDir);
        } else if (clientAuth == ClientAuth.REQUIRE && trustedCertFiles.length == 0) {
            throw new RuntimeException(
                    "Client auth is required but no trust anchors found in: " + trustedCertificatesDir);
        }

        int i = 0;
        for (File trustedCertFile : trustedCertFiles) {
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            try (InputStream input = Files.newInputStream(trustedCertFile.toPath())) {
                while (input.available() > 0) {
                    try {
                        X509Certificate cert = (X509Certificate) certificateFactory.generateCertificate(input);
                        trustStore.setCertificateEntry(Integer.toString(i++), cert);
                    } catch (CertificateException e) {
                        SDFSLogger.getLog().error("Certification exception caught, not a cert file");
                        continue;
                    } catch (Exception e) {
                        throw new CertificateException("Error loading certificate file: " + trustedCertFile, e);
                    }
                }
            }
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        for (TrustManager t : trustManagers) {
            if (t instanceof X509TrustManager) {
                standardTrustManager = (X509TrustManager) t;
                return;
            }
        }

        throw new NoSuchAlgorithmException("No X509TrustManager in TrustManagerFactory");

    }

}
