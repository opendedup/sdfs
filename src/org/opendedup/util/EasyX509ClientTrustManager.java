package org.opendedup.util;

/*
 * ====================================================================
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */

import java.io.File;
import java.io.FileFilter;
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
import org.opendedup.sdfs.mgmt.grpc.IOServer;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;

/**
 * <p>
 * EasyX509TrustManager unlike default {@link X509TrustManager} accepts
 * self-signed certificates.
 * </p>
 * <p>
 * This trust manager SHOULD NOT be used for productive systems due to security
 * reasons, unless it is a concious decision and you are perfectly aware of
 * security implications of accepting self-signed certificates
 * </p>
 *
 * @author <a href="mailto:adrian.sutton@ephox.com">Adrian Sutton</a>
 * @author <a href="mailto:oleg@ural.ru">Oleg Kalnichevski</a>
 *
 *         <p>
 *         DISCLAIMER: HttpClient developers DO NOT actively support this
 *         component. The component is provided as a reference material, which
 *         may be inappropriate for use without additional customization.
 *         </p>
 */

public class EasyX509ClientTrustManager implements X509TrustManager {
	private X509TrustManager standardTrustManager = null;

	/** Log object for this class. */
	private static final Log LOG = LogFactory
			.getLog(EasyX509ClientTrustManager.class);

	/**
	 * Constructor for EasyX509TrustManager.
	 */
	public EasyX509ClientTrustManager()
			throws NoSuchAlgorithmException, KeyStoreException {
		super();
		File trustedCertificatesDir=new File(IOServer.trustStoreDir);
		try {
			loadTrustManager(trustedCertificatesDir,ClientAuth.REQUIRE);
		} catch (Exception e1) {
			throw new KeyStoreException("Error occurred while setting up trust manager." + e1.getCause(), e1);
		}
	}

	/**
	 * @see javax.net.ssl.X509TrustManager#checkClientTrusted(X509Certificate[],
	 *      String authType)
	 */
	public void checkClientTrusted(X509Certificate[] certificates,
			String authType) throws CertificateException {
		try {
			SDFSLogger.getLog().info("EASYX509 checkClientTrusted authtype="+authType);
			File trustedCertificatesDir=new File(IOServer.trustStoreDir);
			loadTrustManager(trustedCertificatesDir,ClientAuth.REQUIRE);
			standardTrustManager.checkClientTrusted(certificates, authType);
        } catch (Exception e) {
        	SDFSLogger.getLog().error("EASYX509 checkClientTrusted caught exception",e);
			throw new CertificateException("Error occurred while setting up trust manager." + e.getCause(), e);
        }
	}

	/**
	 * @see javax.net.ssl.X509TrustManager#checkServerTrusted(X509Certificate[],
	 *      String authType)
	 */
	public void checkServerTrusted(X509Certificate[] certificates,
			String authType) throws CertificateException {
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
		return this.standardTrustManager.getAcceptedIssuers();
	}

	public void loadTrustManager(File trustedCertificatesDir,ClientAuth clientAuth) throws Exception {
		SDFSLogger.getLog().info("Load Trust Manager");
		KeyStore trustStore = KeyStore.getInstance( KeyStore.getDefaultType() );
	    trustStore.load( null, null );

	    //Implementing FileFilter to retrieve only the files in the directory
		FileFilter fileFilter = new FileFilter() {
		    @Override
		    public boolean accept(File file) {
		        if(file.isDirectory()) {
		        	SDFSLogger.getLog().warn("Directory, skipping it to add in TrustStore.");
		            return false;
		        } else {
		            return true;
		        }
		    }
		};

	    File[] trustedCertFiles = trustedCertificatesDir.listFiles(fileFilter);

	    if ( trustedCertFiles == null ) {
	          throw new RuntimeException( "Could not find or list files in trusted directory: " +trustedCertificatesDir  );
	    } else if ( trustedCertFiles.length == 0 ) {
	          throw new RuntimeException( "Client auth is required but no trust anchors found in: " + trustedCertificatesDir  );
	    }

	    int i = 0;
	    for ( File trustedCertFile : trustedCertFiles ) {
	        CertificateFactory certificateFactory = CertificateFactory.getInstance( "X.509" );
            try ( InputStream input = Files.newInputStream( trustedCertFile.toPath() ) ) {
            	while ( input.available() > 0 ) {
            		try {
            			X509Certificate cert = (X509Certificate) certificateFactory.generateCertificate( input );
                        trustStore.setCertificateEntry( Integer.toString( i++ ), cert );
                    } catch (CertificateException e) {
                    	SDFSLogger.getLog().debug("Not a certificate file, skipping it to add in TrustStore.");
                        continue;
                    } catch ( Exception e )
            		{
                    	throw new CertificateException( "Error loading certificate file: " + trustedCertFile, e );
                    }
                }
	        }
	    }

	    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() );
	    trustManagerFactory.init( trustStore );

	    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
	    for (TrustManager t : trustManagers) {
	    	if (t instanceof X509TrustManager) {
	        	  standardTrustManager = (X509TrustManager) t;
	        	  return;
	        }
	    }

	    throw new NoSuchAlgorithmException(
		    	        "No X509TrustManager in TrustManagerFactory");
	}
}