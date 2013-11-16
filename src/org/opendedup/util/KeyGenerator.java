package org.opendedup.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;

import org.opendedup.logging.SDFSLogger;

import sun.security.x509.CertAndKeyGen;
import sun.security.x509.X500Name;




public class KeyGenerator {
	
	public static void generateKey(File key) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, NoSuchProviderException, InvalidKeyException, SignatureException   {
			key.getParentFile().mkdirs();
		 KeyStore keyStore = KeyStore.getInstance("JKS");
	        keyStore.load(null, null);

	        CertAndKeyGen keypair = new CertAndKeyGen("RSA", "SHA1WithRSA", null);

	        X500Name x500Name = new X500Name(InetAddress.getLocalHost().getCanonicalHostName(), "sdfs", "opendedup", "portland", "or", "US");

	        keypair.generate(1024);
	        PrivateKey privKey = keypair.getPrivateKey();

	        X509Certificate[] chain = new X509Certificate[1];

	        chain[0] = keypair.getSelfCertificate(x500Name, new Date(), (long) 1096 * 24 * 60 * 60);

	        keyStore.setKeyEntry("sdfs", privKey, "sdfs".toCharArray(), chain);

	        keyStore.store(new FileOutputStream(key), "sdfs".toCharArray());
	        SDFSLogger.getLog().info("generated certificate for ssl communication at " + key);

	}

}
