package org.opendedup.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;


import org.bouncycastle.jce.X509Principal;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.x509.X509V1CertificateGenerator;
import org.opendedup.logging.SDFSLogger;

public class KeyGenerator {

	public static void generateKey(File key) throws KeyStoreException,
			NoSuchAlgorithmException, CertificateException, IOException,
			NoSuchProviderException, InvalidKeyException, SignatureException {
		key.getParentFile().mkdirs();
		KeyStore keyStore = KeyStore.getInstance("JKS");
		keyStore.load(null, null);


		// yesterday
	    
	    // GENERATE THE PUBLIC/PRIVATE RSA KEY PAIR
	    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
	    keyPairGenerator.initialize(1024, new SecureRandom());

	    KeyPair keyPair = keyPairGenerator.generateKeyPair();

	    // GENERATE THE X509 CERTIFICATE
	    X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();

	    certGen.setSerialNumber(BigInteger.valueOf(System.currentTimeMillis()));
	    certGen.setIssuerDN(new X509Principal("CN=sdfs, OU=None, O=None L=None, C=None"));
	    certGen.setSubjectDN(new X509Principal("CN=sdfs, OU=None, O=None L=None, C=None"));
	    certGen.setNotBefore(new Date(System.currentTimeMillis() - 1000L * 60 * 60 * 24 * 30));
	    certGen.setNotAfter(new Date(System.currentTimeMillis() + (1000L * 60 * 60 * 24 * 365*10)));
	    certGen.setPublicKey(keyPair.getPublic());
	    certGen.setSignatureAlgorithm("SHA256WithRSAEncryption");

	    X509Certificate cert = certGen.generate(keyPair.getPrivate(), "BC");

		keyStore.setKeyEntry("sdfs", keyPair.getPrivate(), "sdfs".toCharArray(),  new java.security.cert.Certificate[]{cert});

		keyStore.store(new FileOutputStream(key), "sdfs".toCharArray());
		SDFSLogger.getLog().info(
				"generated certificate for ssl communication at " + key);

	}
	
	static {
	    // adds the Bouncy castle provider to java security
	    Security.addProvider(new BouncyCastleProvider());
	}
	

}
