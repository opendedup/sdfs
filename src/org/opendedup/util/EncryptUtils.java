package org.opendedup.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.crypto.BlockCipher;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class EncryptUtils {
	private static byte[] keyBytes = null;
	private static SecretKeySpec key = null;
	private static final byte [] iv = StringUtils.getHexBytes(Main.chunkStoreEncryptionIV);
	private static final IvParameterSpec spec = new IvParameterSpec(iv);
	static {
		try {
			keyBytes = HashFunctions
					.getSHAHashBytes(Main.chunkStoreEncryptionKey.getBytes());
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchProviderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
		key = new SecretKeySpec(keyBytes, "AES");
	}

	public static byte[] encryptDep(byte[] chunk) throws IOException {

		byte[] encryptedChunk = new byte[chunk.length];
		try {
			Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");
			cipher.init(Cipher.ENCRYPT_MODE, key);
			int ctLength = cipher.update(chunk, 0, chunk.length,
					encryptedChunk, 0);
			ctLength += cipher.doFinal(encryptedChunk, ctLength);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to encrypt chunk", e);
			throw new IOException(e);
		}
		return encryptedChunk;
	}

	public static byte[] decryptDep(byte[] encryptedChunk) throws IOException {
		byte[] chunk = new byte[encryptedChunk.length];
		try {
			Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");
			cipher.init(Cipher.DECRYPT_MODE, key);
			int ptLength = cipher.update(encryptedChunk, 0, chunk.length,
					chunk, 0);
			ptLength += cipher.doFinal(chunk, ptLength);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to encrypt chunk", e);
			throw new IOException(e);
		}
		return chunk;
	}

	public static byte[] encrypt(byte[] chunk) throws IOException {
		BlockCipher engine = new AESEngine();
		PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(engine);

		cipher.init(true, new KeyParameter(keyBytes));
		int size = cipher.getOutputSize(chunk.length);
		byte[] cipherText = new byte[size];

		int olen = cipher.processBytes(chunk, 0, chunk.length, cipherText, 0);
		try {
			olen += cipher.doFinal(cipherText, olen);
			if (olen < size) {
				byte[] tmp = new byte[olen];
				System.arraycopy(cipherText, 0, tmp, 0, olen);
				cipherText = tmp;
			}
		} catch (CryptoException ce) {
			SDFSLogger.getLog().error("uable to decrypt", ce);
			throw new IOException(ce);
		}
		return cipherText;
	}

	public static byte[] decrypt(byte[] encChunk) throws IOException {
		BlockCipher engine = new AESEngine();
		PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(engine);
		
		cipher.init(false, new KeyParameter(keyBytes));
		int size = cipher.getOutputSize(encChunk.length);
		byte[] clearText = new byte[size];

		int olen = cipher.processBytes(encChunk, 0, encChunk.length, clearText,
				0);
		try {
			olen += cipher.doFinal(clearText, olen);
			if (olen < size) {
				byte[] tmp = new byte[olen];
				System.arraycopy(clearText, 0, tmp, 0, olen);
				clearText = tmp;
			}
		} catch (CryptoException ce) {
			SDFSLogger.getLog().error("uable to decrypt", ce);
			throw new IOException(ce);
		}
		return clearText;
	}
	
	public static void encryptFile(File src,File dst) throws Exception {
		dst.getParentFile().mkdirs();
	         
	         Cipher encrypt =  Cipher.getInstance("AES/CBC/PKCS5Padding");  
	         encrypt.init(Cipher.ENCRYPT_MODE, key,spec);
	         //opening streams
	         FileOutputStream fos =new FileOutputStream(dst);
	         FileInputStream fis =new FileInputStream(src);
	         CipherOutputStream cout=new CipherOutputStream(fos, encrypt);
	         IOUtils.copy(fis,cout);
	         cout.flush();
	         cout.close();
	         fis.close();
	}
	
	public static void decryptFile(File src,File dst) throws Exception {
		dst.getParentFile().mkdirs();
	         
	         Cipher encrypt =  Cipher.getInstance("AES/CBC/PKCS5Padding");  
	         encrypt.init(Cipher.DECRYPT_MODE, key,spec);
	         //opening streams
	         FileOutputStream fos =new FileOutputStream(dst);
	         FileInputStream fis =new FileInputStream(src);
	         CipherInputStream cis=new CipherInputStream(fis, encrypt);
	         IOUtils.copy(cis,fos);
	        fos.flush();
	        fos.close();
	        cis.close();
	}
	
	

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		encryptFile(new File("/home/samsilverberg/Downloads/24199E7D-B252-A19F-8AF4-CC0A6177871F.tar.gz"),new File("/home/samsilverberg/Downloads/enc.tar.gz"));
		decryptFile(new File("/home/samsilverberg/Downloads/enc.tar.gz"),new File("/home/samsilverberg/Downloads/poo.tar.gz"));
		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms");
	}

}
