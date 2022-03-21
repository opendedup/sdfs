/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.X509Extensions;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.X509Principal;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.x509.X509V3CertificateGenerator;
import org.opendedup.logging.SDFSLogger;

public class KeyGenerator {
	private static final String BC_PROVIDER = "BC";
	private static final String KEY_ALGORITHM = "RSA";
	private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";

	/*
	public static void generateKey(File key) throws IOException {
		try {
			key.getParentFile().mkdirs();
			key.delete();
			File keyStoreFile = new File(key.getParentFile(), "root-cert.pfx");
			if (!keyStoreFile.exists()) {
				createSigner(key);
			}
			FileInputStream fis = new FileInputStream(keyStoreFile);
			String password = "sdfs6442";
			KeyStore keyStore = KeyStore.getInstance("pkcs12");
			keyStore.load(fis, password.toCharArray());
			Enumeration<String> es = keyStore.aliases();
			String alias = "";
			boolean isAliasWithPrivateKey = false;
			while (es.hasMoreElements()) {
				alias = (String) es.nextElement();

				// if alias refers to a private key break at that point
				// as we want to use that certificate
				if (isAliasWithPrivateKey = keyStore.isKeyEntry(alias)) {
					break;
				}
			}
			if (isAliasWithPrivateKey) {

				KeyStore.PrivateKeyEntry pKey = (KeyStore.PrivateKeyEntry) keyStore.getEntry(alias,
						new KeyStore.PasswordProtection(password.toCharArray()));
				//// Load certificate chain
				Certificate[] chain = keyStore.getCertificateChain("root-cert");
				X509Certificate rootCert = (X509Certificate) chain[0];

				X500Name rootCertIssuer = new JcaX509CertificateHolder(rootCert).getSubject();
				String keyFile = new File(key.getParentFile(), "tls_key").getPath();
				String hostName = InetAddress.getLocalHost().getHostName();


				// GENERATE THE PUBLIC/PRIVATE RSA KEY PAIR
				KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM, BC_PROVIDER);
				keyPairGenerator.initialize(4096, new SecureRandom());

				KeyPair keyPair = keyPairGenerator.generateKeyPair();

				X500Name issuedCertSubject = new X500Name("CN=" + hostName +", OU=None, O=None L=None, C=None");
				BigInteger issuedCertSerialNum = new BigInteger(Long.toString(new SecureRandom().nextLong()));
				KeyPair issuedCertKeyPair = keyPairGenerator.generateKeyPair();

				PKCS10CertificationRequestBuilder p10Builder = new JcaPKCS10CertificationRequestBuilder(
						issuedCertSubject, issuedCertKeyPair.getPublic());
				JcaContentSignerBuilder csrBuilder = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
						.setProvider(BC_PROVIDER);

				// Sign the new KeyPair with the root cert Private Key
				ContentSigner csrContentSigner = csrBuilder.build(pKey.getPrivateKey());
				PKCS10CertificationRequest csr = p10Builder.build(csrContentSigner);

				Date startDate = new Date(System.currentTimeMillis() - 1000L * 60 * 60 * 24 * 30);

				Date endDate = new Date(System.currentTimeMillis() + (1000L * 60 * 60 * 24 * 365 * 10));
				// Use the Signed KeyPair and CSR to generate an issued Certificate
				// Here serial number is randomly generated. In general, CAs use
				// a sequence to generate Serial number and avoid collisions
				X509v3CertificateBuilder issuedCertBuilder = new X509v3CertificateBuilder(rootCertIssuer,
						issuedCertSerialNum, startDate, endDate, csr.getSubject(), csr.getSubjectPublicKeyInfo());

				JcaX509ExtensionUtils issuedCertExtUtils = new JcaX509ExtensionUtils();

				// Add Extensions
				// Use BasicConstraints to say that this Cert is not a CA
				//issuedCertBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));

				// Add Issuer cert identifier as Extension

				issuedCertBuilder.addExtension(Extension.authorityKeyIdentifier, false,
						issuedCertExtUtils.createAuthorityKeyIdentifier(rootCert));
				issuedCertBuilder.addExtension(Extension.subjectKeyIdentifier, false,
						issuedCertExtUtils.createSubjectKeyIdentifier(csr.getSubjectPublicKeyInfo()));
				// Add intended key usage extension if needed
				//issuedCertBuilder.addExtension(Extension.keyUsage, false, new KeyUsage(KeyUsage.keyEncipherment));

				// Add DNS name is cert is to used for SSL
				issuedCertBuilder.addExtension(Extension.subjectAlternativeName, false,
						new DERSequence(new ASN1Encodable[] { new GeneralName(GeneralName.dNSName, hostName) }));
				X509CertificateHolder issuedCertHolder = issuedCertBuilder.build(csrContentSigner);
				X509Certificate issuedCert = new JcaX509CertificateConverter().setProvider(BC_PROVIDER)
						.getCertificate(issuedCertHolder);

				// Verify the issued cert signature against the root (issuer) cert
				issuedCert.verify(rootCert.getPublicKey(), BC_PROVIDER);

				keyStore.setKeyEntry("sdfs", keyPair.getPrivate(), password.toCharArray(),
						new java.security.cert.Certificate[] { issuedCert });
				Key pvt = keyPair.getPrivate();
				Key pub = keyPair.getPublic();
				PemObject pemObject = new PemObject("PRIVATE KEY", pvt.getEncoded());
				JcaPEMWriter pemWriter = new JcaPEMWriter(
						new OutputStreamWriter(new FileOutputStream(keyFile + ".key")));
				try {
					pemWriter.writeObject(pemObject);
				} finally {
					pemWriter.close();
				}
				keyStore.store(new FileOutputStream(key), password.toCharArray());
				pemObject = new PemObject("CERTIFICATE", issuedCert.getEncoded());
				pemWriter = new JcaPEMWriter(new OutputStreamWriter(new FileOutputStream(keyFile + ".pem")));
				try {
					pemWriter.writeObject(pemObject);
				} finally {
					pemWriter.close();
				}
				pemObject = new PemObject("PUBLIC KEY", pub.getEncoded());
				pemWriter = new JcaPEMWriter(new OutputStreamWriter(new FileOutputStream(keyFile + ".pub")));
				try {
					pemWriter.writeObject(pemObject);
				} finally {
					pemWriter.close();
				}
				SDFSLogger.getLog().info("generated certificate for tls communication at " + key);
			} else {
				System.out.println("No Root key found!");
				System.exit(1);
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("Error creating Server Cert", e);
			throw new IOException(e);
		}
	}
	*/

	public static void generateKey(File key) throws IOException {
		try {
			key.getParentFile().mkdirs();
			key.delete();
			String keyFile = new File(key.getParentFile(), "tls_key").getPath();
			KeyStore keyStore = KeyStore.getInstance("JKS");
			keyStore.load(null, null);
			String hostName = InetAddress.getLocalHost().getHostName();

			// yesterday

			// GENERATE THE PUBLIC/PRIVATE RSA KEY PAIR
			KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
			keyPairGenerator.initialize(4096, new SecureRandom());

			KeyPair keyPair = keyPairGenerator.generateKeyPair();

			// GENERATE THE X509 CERTIFICATE
			X509V3CertificateGenerator certGen = new X509V3CertificateGenerator();

			certGen.setSerialNumber(BigInteger.valueOf(System.currentTimeMillis()));
			certGen.setIssuerDN(new X509Principal("CN=" + hostName + ", OU=None, O=None L=None, C=None"));
			certGen.setSubjectDN(new X509Principal("CN=" + hostName + ", OU=None, O=None L=None, C=None"));
			certGen.setNotBefore(new Date(System.currentTimeMillis() - 1000L * 60 * 60 * 24 * 30));
			certGen.setNotAfter(new Date(System.currentTimeMillis() + (1000L * 60 * 60 * 24 * 365 * 10)));
			certGen.setPublicKey(keyPair.getPublic());
			certGen.setSignatureAlgorithm("SHA256WithRSAEncryption");
			GeneralNames subjectAltName = new GeneralNames(new GeneralName(GeneralName.dNSName, hostName));
			certGen.addExtension(X509Extensions.SubjectAlternativeName, false, subjectAltName);
			X509Certificate cert = certGen.generate(keyPair.getPrivate(), "BC");

			keyStore.setKeyEntry("sdfs", keyPair.getPrivate(), "sdfs".toCharArray(),
					new java.security.cert.Certificate[] { cert });
			Key pvt = keyPair.getPrivate();
			Key pub = keyPair.getPublic();
			PemObject pemObject = new PemObject("PRIVATE KEY", pvt.getEncoded());
			JcaPEMWriter pemWriter = new JcaPEMWriter(new OutputStreamWriter(new FileOutputStream(keyFile + ".key")));
			try {
				pemWriter.writeObject(pemObject);
			} finally {
				pemWriter.close();
			}
			keyStore.store(new FileOutputStream(key), "sdfs".toCharArray());
			pemObject = new PemObject("CERTIFICATE", cert.getEncoded());
			pemWriter = new JcaPEMWriter(new OutputStreamWriter(new FileOutputStream(keyFile + ".pem")));
			try {
				pemWriter.writeObject(pemObject);
			} finally {
				pemWriter.close();
			}
			pemObject = new PemObject("PUBLIC KEY", pub.getEncoded());
			pemWriter = new JcaPEMWriter(new OutputStreamWriter(new FileOutputStream(keyFile + ".pub")));
			try {
				pemWriter.writeObject(pemObject);
			} finally {
				pemWriter.close();
			}
			SDFSLogger.getLog().info("generated certificate for ssl communication at " + key);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Error creating Server Cert", e);
			throw new IOException(e);
		}
	}

	public static void createSigner(File key) throws IOException {
		try {
			String keyFile = new File(key.getParentFile(), "signer_key").getPath();
			KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM, BC_PROVIDER);
			keyPairGenerator.initialize(4096);

			// Setup start date to yesterday and end date for 1 year validity
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DATE, -1);
			Date startDate = calendar.getTime();

			calendar.add(Calendar.YEAR, 10);
			Date endDate = calendar.getTime();

			// First step is to create a root certificate
			// First Generate a KeyPair,
			// then a random serial number
			// then generate a certificate using the KeyPair
			KeyPair rootKeyPair = keyPairGenerator.generateKeyPair();
			BigInteger rootSerialNum = new BigInteger(Long.toString(new SecureRandom().nextLong()));

			// Issued By and Issued To same for root certificate
			String hostName = InetAddress.getLocalHost().getHostName();
			X500Name rootCertIssuer = new X500Name("CN="+hostName+",OU=None, O=None L=None, C=None");
			X500Name rootCertSubject = rootCertIssuer;
			ContentSigner rootCertContentSigner = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
					.setProvider(BC_PROVIDER).build(rootKeyPair.getPrivate());
			X509v3CertificateBuilder rootCertBuilder = new JcaX509v3CertificateBuilder(rootCertIssuer, rootSerialNum,
					startDate, endDate, rootCertSubject, rootKeyPair.getPublic());

			// Add Extensions
			// A BasicConstraint to mark root certificate as CA certificate
			JcaX509ExtensionUtils rootCertExtUtils = new JcaX509ExtensionUtils();
			rootCertBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
			rootCertBuilder.addExtension(Extension.subjectKeyIdentifier, false,
					rootCertExtUtils.createSubjectKeyIdentifier(rootKeyPair.getPublic()));

			// Create a cert holder and export to X509Certificate
			X509CertificateHolder rootCertHolder = rootCertBuilder.build(rootCertContentSigner);
			X509Certificate rootCert = new JcaX509CertificateConverter().setProvider(BC_PROVIDER)
					.getCertificate(rootCertHolder);
			String keyStoreFile = new File(key.getParentFile(), "root-cert.pfx").getPath();
			exportKeyPairToKeystoreFile(rootKeyPair, rootCert, "root-cert", keyStoreFile, "PKCS12", "sdfs6442");
			Key pvt = rootKeyPair.getPrivate();
			Key pub = rootKeyPair.getPublic();
			PemObject pemObject = new PemObject("CERTIFICATE", rootCert.getEncoded());
			JcaPEMWriter pemWriter = new JcaPEMWriter(new OutputStreamWriter(new FileOutputStream(keyFile + ".crt")));
			try {
				pemWriter.writeObject(pemObject);
			} finally {
				pemWriter.close();
			}
			pemObject = new PemObject("PUBLIC KEY", pub.getEncoded());
			pemWriter = new JcaPEMWriter(new OutputStreamWriter(new FileOutputStream(keyFile + ".pub")));
			try {
				pemWriter.writeObject(pemObject);
			} finally {
				pemWriter.close();
			}
			byte[] privBytes = pvt.getEncoded();
			PrivateKeyInfo pkInfo = PrivateKeyInfo.getInstance(privBytes);
			ASN1Encodable encodable = pkInfo.parsePrivateKey();
			ASN1Primitive primitive = encodable.toASN1Primitive();
			byte[] privateKeyPKCS1 = primitive.getEncoded();
			pemObject = new PemObject("RSA PRIVATE KEY", privateKeyPKCS1);
			pemWriter = new JcaPEMWriter(new OutputStreamWriter(new FileOutputStream(keyFile + ".key")));
			try {
				pemWriter.writeObject(pemObject);
			} finally {
				pemWriter.close();
			}
			SDFSLogger.getLog().info("generated certificate for cert signing " + keyFile);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Error creating Signing Cert", e);
			throw new IOException(e);
		}

	}

	static void exportKeyPairToKeystoreFile(KeyPair keyPair, Certificate certificate, String alias, String fileName,
			String storeType, String storePass) throws Exception {
		KeyStore sslKeyStore = KeyStore.getInstance(storeType, BC_PROVIDER);
		sslKeyStore.load(null, null);
		sslKeyStore.setKeyEntry(alias, keyPair.getPrivate(), null, new Certificate[] { certificate });
		FileOutputStream keyStoreOs = new FileOutputStream(fileName);
		sslKeyStore.store(keyStoreOs, storePass.toCharArray());
	}

	static void writeCertToFileBase64Encoded(Certificate certificate, String fileName) throws Exception {
		FileOutputStream certificateOut = new FileOutputStream(fileName);
		certificateOut.write("-----BEGIN CERTIFICATE-----".getBytes());
		certificateOut.write(Base64.encode(certificate.getEncoded()));
		certificateOut.write("-----END CERTIFICATE-----".getBytes());
		certificateOut.close();
	}

	static {
		// adds the Bouncy castle provider to java security
		Security.addProvider(new BouncyCastleProvider());
	}

}
