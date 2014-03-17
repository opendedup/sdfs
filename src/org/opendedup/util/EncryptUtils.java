package org.opendedup.util;

import java.io.IOException;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.crypto.BlockCipher;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.util.Arrays;
import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class EncryptUtils {
	private static byte[] keyBytes = null;
	private static SecretKeySpec key = null;
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

	public static byte[] encrypt(byte[] chunk) {
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
			System.err.println(ce);
			System.exit(1);
		}
		return cipherText;
	}

	public static byte[] decrypt(byte[] encChunk) {
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
			System.err.println(ce);
			System.exit(1);
		}
		return clearText;
	}

	public static void main(String[] args) throws IOException {
		String testStr = "blaaaaaaaaaaaaa!sssssss";
		byte[] enc = EncryptUtils.encrypt(testStr.getBytes());
		byte[] dec = EncryptUtils.decrypt(enc);
		String bla = new String(dec);
		System.out.println(bla + " equals " + bla.equals(testStr));
		if (!Arrays.areEqual(dec, testStr.getBytes()))
			System.out.println("Encryption Error!!");
		long start = System.currentTimeMillis();
		Random rnd = new Random();
		for (int i = 0; i < 800; i++) {
			byte[] b = new byte[128 * 1024];
			rnd.nextBytes(b);
			enc = EncryptUtils.encrypt(b);
			dec = EncryptUtils.decrypt(enc);
			if (!Arrays.areEqual(dec, b))
				System.out.println("Encryption Error ["
						+ HashFunctions.getMD5Hash(b) + "] ["
						+ HashFunctions.getMD5Hash(dec) + "]");
		}
		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms");
	}

}
