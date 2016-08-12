package org.opendedup.util;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.Security;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.IOUtils;
import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class EncryptUtils {
	private static byte[] keyBytes = null;
	private static SecretKeySpec key = null;
	public static final byte[] iv = StringUtils
			.getHexBytes(Main.chunkStoreEncryptionIV);
	private static final IvParameterSpec spec = new IvParameterSpec(iv);
	static {
		try {
			keyBytes = HashFunctions
					.getSHAHashBytes(Main.chunkStoreEncryptionKey.getBytes());
			Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
			key = new SecretKeySpec(keyBytes, "AES");
		} catch (Exception e) {
			SDFSLogger.getLog().error("uable to create key", e);
			e.printStackTrace();
			System.exit(-1);
		}

	}

	public static byte[] encrypt(byte[] chunk) throws IOException {

		return encryptCBC(chunk);
	}

	public static byte[] decrypt(byte[] encryptedChunk) throws IOException {
		return decryptCBC(encryptedChunk);
	}

	public static byte[] encryptCBC(byte[] chunk) throws IOException {

		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.ENCRYPT_MODE, key, spec);
			byte[] encrypted = cipher.doFinal(chunk);
			return encrypted;
		} catch (Exception ce) {
			SDFSLogger.getLog().error("uable to encrypt", ce);
			throw new IOException(ce);
		}

	}

	public static byte[] decryptCBC(byte[] encChunk) throws IOException {

		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.DECRYPT_MODE, key, spec);
			byte[] decrypted = cipher.doFinal(encChunk);
			return decrypted;
		} catch (Exception ce) {
			SDFSLogger.getLog().error("uable to decrypt", ce);
			throw new IOException(ce);
		}

	}

	public static byte[] encryptCBC(byte[] chunk, String passwd, String iv)
			throws IOException {

		try {
			byte[] _iv = StringUtils.getHexBytes(iv);
			IvParameterSpec _spec = new IvParameterSpec(_iv);
			byte[] _keyBytes = HashFunctions.getSHAHashBytes(passwd.getBytes());
			SecretKeySpec _key = new SecretKeySpec(_keyBytes, "AES");
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.ENCRYPT_MODE, _key, _spec);
			byte[] encrypted = cipher.doFinal(chunk);
			return encrypted;
		} catch (Exception ce) {
			SDFSLogger.getLog().error("uable to encrypt", ce);
			throw new IOException(ce);
		}

	}
	
	public static byte [] decryptCBC(byte[] encChunk, IvParameterSpec cspec) throws IOException {
		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.DECRYPT_MODE, key, cspec);
			byte[] decrypted = cipher.doFinal(encChunk);
			return decrypted;
		} catch (Exception ce) {
			SDFSLogger.getLog().error("uable to decrypt", ce);
			throw new IOException(ce);
		}
	}
	
	public static byte [] encryptCBC(byte[] chunk, IvParameterSpec cspec) throws IOException {
		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.ENCRYPT_MODE, key, cspec);
			byte[] encrypted = cipher.doFinal(chunk);
			return encrypted;
		} catch (Exception ce) {
			SDFSLogger.getLog().error("uable to encrypt", ce);
			throw new IOException(ce);
		}
	}

	public static byte[] decryptCBC(byte[] encChunk, String passwd, String iv)
			throws IOException {

		try {
			byte[] _iv = StringUtils.getHexBytes(iv);
			IvParameterSpec _spec = new IvParameterSpec(_iv);
			byte[] _keyBytes = HashFunctions.getSHAHashBytes(passwd.getBytes());
			SecretKeySpec _key = new SecretKeySpec(_keyBytes, "AES");
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.DECRYPT_MODE, _key, _spec);
			byte[] decrypted = cipher.doFinal(encChunk);
			return decrypted;
		} catch (Exception ce) {
			SDFSLogger.getLog().error("uable to decrypt", ce);
			throw new IOException(ce);
		}

	}

	public static void encryptFile(File src, File dst) throws Exception {
		if (!dst.getParentFile().exists())
			dst.getParentFile().mkdirs();

		Cipher encrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
		encrypt.init(Cipher.ENCRYPT_MODE, key, spec);
		// opening streams
		FileOutputStream fos = new FileOutputStream(dst);
		FileInputStream fis = new FileInputStream(src);
		CipherOutputStream cout = new CipherOutputStream(fos, encrypt);
		IOUtils.copy(fis, cout);
		cout.flush();
		cout.close();
		fis.close();
	}

	public static void decryptFile(File src, File dst) throws Exception {
		if (!dst.getParentFile().exists())
			dst.getParentFile().mkdirs();

		Cipher encrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
		encrypt.init(Cipher.DECRYPT_MODE, key, spec);
		// opening streams
		FileOutputStream fos = new FileOutputStream(dst);
		FileInputStream fis = new FileInputStream(src);
		CipherInputStream cis = new CipherInputStream(fis, encrypt);
		IOUtils.copy(cis, fos);
		fos.flush();
		fos.close();
		cis.close();
	}

	public static void decryptFile(File src, File dst, String passwd, String iv)
			throws Exception {
		if (!dst.getParentFile().exists())
			dst.getParentFile().mkdirs();
		byte[] _iv = StringUtils.getHexBytes(iv);
		IvParameterSpec _spec = new IvParameterSpec(_iv);
		byte[] _keyBytes = HashFunctions.getSHAHashBytes(passwd.getBytes());
		SecretKeySpec _key = new SecretKeySpec(_keyBytes, "AES");
		Cipher encrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
		encrypt.init(Cipher.DECRYPT_MODE, _key, _spec);
		// opening streams
		FileOutputStream fos = new FileOutputStream(dst);
		FileInputStream fis = new FileInputStream(src);
		CipherInputStream cis = new CipherInputStream(fis, encrypt);
		IOUtils.copy(cis, fos);
		fos.flush();
		fos.close();
		cis.close();
	}

	private static final ThreadLocal<Cipher> localDigest = new ThreadLocal<Cipher>() {
		@Override
		protected Cipher initialValue() {
			try {
				return Cipher.getInstance("AES/CBC/PKCS5Padding");
			} catch (Exception e) {
				// ugly but necessary
				throw new RuntimeException(e);
			}
		}
	};

	public static void main(String[] args) {
		new Thread(new MyRunnable()).start();
		new Thread(new MyRunnable()).start();
		new Thread(new MyRunnable()).start();
		new Thread(new MyRunnable()).start();
		new Thread(new MyRunnable()).start();
		new Thread(new MyRunnable()).start();
		new Thread(new MyRunnable()).start();
	}

	private static class MyRunnable implements Runnable {
		@Override
		public void run() {
			try {
				Cipher cipher = localDigest.get();
				System.out.println("Got digest "
						+ System.identityHashCode(cipher));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
