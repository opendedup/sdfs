package org.opendedup.util;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Random;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.util.Arrays;
import org.opendedup.sdfs.Main;

public class EncryptUtils {
	private static byte[] keyBytes = null;
	private static SecretKeySpec key = null;
	static {
		keyBytes = HashFunctions.getMD5ByteHash(Main.chunkStoreEncryptionKey
				.getBytes());
		Security
				.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
		key = new SecretKeySpec(keyBytes, "AES");
	}

	public static byte[] encrypt(byte[] chunk) throws IOException {

		byte[] encryptedChunk = new byte[chunk.length];
		try {
			Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
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
	
	public static byte [] decrypt(byte [] encryptedChunk) throws IOException {
		byte [] chunk = new byte[encryptedChunk.length];
		try {
			Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
			cipher.init(Cipher.DECRYPT_MODE, key);
		    int ptLength = cipher.update(encryptedChunk, 0, chunk.length, chunk, 0);
		    ptLength += cipher.doFinal(chunk, ptLength);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to encrypt chunk", e);
			throw new IOException(e);
		}
		return chunk;
	}
	
	public static void main(String [] args) throws IOException {
		String testStr = "blaaaaaaaaaaaaa!";
		byte [] enc = EncryptUtils.encrypt(testStr.getBytes());
		byte [] dec = EncryptUtils.decrypt(enc);
		String bla = new String(dec);
		System.out.println(bla + " equals " + bla.equals(testStr));
		long start = System.currentTimeMillis();
		Random rnd = new Random();
		for (int i = 0; i < 800; i++) {
			byte[] b = new byte[128000];
			rnd.nextBytes(b);
			enc = EncryptUtils.encrypt(b);
			dec = EncryptUtils.decrypt(enc);
			if(!Arrays.areEqual(dec, b))
				System.out.println("Encryption Error");
		}
		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms");
	}

}
