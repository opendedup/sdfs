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
	private static SecretKeySpec oldKey = null;
	public static final byte[] iv = StringUtils.getHexBytes(Main.chunkStoreEncryptionIV);
	public static byte[] oldKeyBytes = null;
	private static final IvParameterSpec spec = new IvParameterSpec(iv);
	static {
		try {
			keyBytes = HashFunctions.getSHAHashBytes(Main.chunkStoreEncryptionKey.getBytes());
			oldKeyBytes = HashFunctions.getSHAHashBytes("Password".getBytes());
			Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
			key = new SecretKeySpec(keyBytes, "AES");
			oldKey = new SecretKeySpec(oldKeyBytes, "AES");
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
		return encryptCBC(chunk,false);
	}
	
	public static byte[] encryptCBC(byte[] chunk,boolean useOldKey) throws IOException {

		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			if(useOldKey)
				cipher.init(Cipher.ENCRYPT_MODE, oldKey, spec);
			else
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
			// SDFSLogger.getLog().error("uable to decrypt", ce);
			try {
				Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
				cipher.init(Cipher.DECRYPT_MODE, oldKey, spec);
				byte[] decrypted = cipher.doFinal(encChunk);
				return decrypted;
			} catch (Exception ce1) {
				SDFSLogger.getLog().error("uable to decrypt", ce);
				throw new IOException(ce);
			}
		}

	}

	public static byte[] encryptCBC(byte[] chunk, String passwd, String iv) throws IOException {

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

	public static byte[] decryptCBC(byte[] encChunk, IvParameterSpec cspec) throws IOException {
		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.DECRYPT_MODE, key, cspec);
			byte[] decrypted = cipher.doFinal(encChunk);
			return decrypted;
		} catch (Exception ce) {
			SDFSLogger.getLog().error("uable to decrypt", ce);
			try {
				Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
				cipher.init(Cipher.DECRYPT_MODE, oldKey, cspec);
				byte[] decrypted = cipher.doFinal(encChunk);
				return decrypted;
			} catch (Exception ce1) {
				SDFSLogger.getLog().error("uable to decrypt", ce);
				throw new IOException(ce1);
			}
		}
	}

	public static byte[] encryptCBC(byte[] chunk, IvParameterSpec cspec) throws IOException {
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

	public static byte[] decryptCBC(byte[] encChunk, String passwd, String iv) throws IOException {

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

	public static void encryptFile(File src, File dst, IvParameterSpec ivspec) throws Exception {
		if (!dst.getParentFile().exists())
			dst.getParentFile().mkdirs();

		Cipher encrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
		encrypt.init(Cipher.ENCRYPT_MODE, key, ivspec);
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
		try {
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
		} catch (Exception e) {
			if (!dst.getParentFile().exists())
				dst.getParentFile().mkdirs();

			Cipher encrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
			encrypt.init(Cipher.DECRYPT_MODE, oldKey, spec);
			// opening streams
			FileOutputStream fos = new FileOutputStream(dst);
			FileInputStream fis = new FileInputStream(src);
			CipherInputStream cis = new CipherInputStream(fis, encrypt);
			IOUtils.copy(cis, fos);
			fos.flush();
			fos.close();
			cis.close();
		}
	}

	public static void decryptFile(File src, File dst, IvParameterSpec ivspec) throws Exception {
		if (!dst.getParentFile().exists())
			dst.getParentFile().mkdirs();
		try {
		Cipher encrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
		encrypt.init(Cipher.DECRYPT_MODE, key, ivspec);
		// opening streams
		FileOutputStream fos = new FileOutputStream(dst);
		FileInputStream fis = new FileInputStream(src);
		CipherInputStream cis = new CipherInputStream(fis, encrypt);
		IOUtils.copy(cis, fos);
		fos.flush();
		fos.close();
		cis.close();
		}catch (Exception e) {
			

			Cipher encrypt = Cipher.getInstance("AES/CBC/PKCS5Padding");
			encrypt.init(Cipher.DECRYPT_MODE, oldKey, ivspec);
			// opening streams
			FileOutputStream fos = new FileOutputStream(dst);
			FileInputStream fis = new FileInputStream(src);
			CipherInputStream cis = new CipherInputStream(fis, encrypt);
			IOUtils.copy(cis, fos);
			fos.flush();
			fos.close();
			cis.close();
		}
	}

	public static void decryptFile(File src, File dst, String passwd, String iv) throws Exception {
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

	public static void main(String[] args) throws Exception {
		Main.chunkStoreEncryptionKey = "bla";
		EncryptUtils.decryptFile(new File("c:/temp/aws0-volume-cfg.xml.enc"),
				new File("c:/temp/aws0-volume-cfg.xml.nw"));

	}

}
