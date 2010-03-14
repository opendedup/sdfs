package org.opendedup.util;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.zip.Adler32;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.opendedup.sdfs.servers.HashChunkService;


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 * 
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */
public class HashFunctions {

	static MessageDigest algorithm;

	static {

		try {
			Security.addProvider(new BouncyCastleProvider());
			algorithm = MessageDigest.getInstance("Tiger","BC");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String getMurmurhash(byte[] data) {
		int seed = 1;
		int m = 0x5bd1e995;
		int r = 24;

		int h = seed ^ data.length;

		int len = data.length;
		int len_4 = len >> 2;

		for (int i = 0; i < len_4; i++) {
			int i_4 = i << 2;
			int k = data[i_4 + 3];
			k = k << 8;
			k = k | (data[i_4 + 2] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 1] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 0] & 0xff);
			k *= m;
			k ^= k >>> r;
			k *= m;
			h *= m;
			h ^= k;
		}

		int len_m = len_4 << 2;
		int left = len - len_m;

		if (left != 0) {
			if (left >= 3) {
				h ^= (int) data[len - 3] << 16;
			}
			if (left >= 2) {
				h ^= (int) data[len - 2] << 8;
			}
			if (left >= 1) {
				h ^= (int) data[len - 1];
			}

			h *= m;
		}

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;
		return Integer.toHexString(h);
	}

	public static byte[] getMurmurHashBytes(byte[] data) {
		int seed = 1;
		int m = 0x5bd1e995;
		int r = 24;

		// Initialize the hash to a 'random' value
		int len = data.length;
		int h = seed ^ len;

		int i = 0;
		while (len >= 4) {
			int k = data[i + 0] & 0xFF;
			k |= (data[i + 1] & 0xFF) << 8;
			k |= (data[i + 2] & 0xFF) << 16;
			k |= (data[i + 3] & 0xFF) << 24;

			k *= m;
			k ^= k >>> r;
			k *= m;

			h *= m;
			h ^= k;

			i += 4;
			len -= 4;
		}

		switch (len) {
		case 3:
			h ^= (data[i + 2] & 0xFF) << 16;
		case 2:
			h ^= (data[i + 1] & 0xFF) << 8;
		case 1:
			h ^= (data[i + 0] & 0xFF);
			h *= m;
		}

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		byte[] b = new byte[4];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.putInt(h);
		return buf.array();

	}

	public static void testWords(String file) throws NoSuchAlgorithmException,
			NoSuchProviderException {
		HashMap<String, String> map = new HashMap<String, String>();
		try {
			// use buffering, reading one line at a time
			// FileReader always assumes default encoding is OK!
			BufferedReader input = new BufferedReader(new FileReader(file));
			try {
				String line = null; // not declared within while loop
				/*
				 * readLine is a bit quirky : it returns the content of a line
				 * MINUS the newline. it returns null only for the END of the
				 * stream. it returns an empty String if two newlines appear in
				 * a row.
				 */
				while ((line = input.readLine()) != null) {
					String str = getMurmurhash(line.getBytes());
					// System.out.println(line + " " + str + " " +
					// str.getBytes().length);
					if (line.equals("inviter"))
						System.out.println(str);
					if (map.containsKey(str)
							&& !map.get(str).equals(line.trim())) {
						System.out.println("Collision found between "
								+ map.get(str) + " and " + line + " " + str);
						System.out.println(getSHAHash(line.getBytes()));
						return;
					}
					map.put(str, line);
				}
			} finally {
				input.close();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public static void testFile(String file, int buffer_len)
			throws IOException, NoSuchAlgorithmException {

		FileInputStream from = null; // Stream to read from source
		long currMS = System.currentTimeMillis();

		System.out.println("Current time = "
				+ ElapsedTime.getDateTime(new Date(currMS)));

		File f = new File(file);
		System.out.println("file size = " + f.length());
		try {
			from = new FileInputStream(file); // Create input stream
			byte[] buffer = new byte[buffer_len]; // A buffer to hold file
			// contents
			int bytes_read; // How many bytes in buffer
			// Read a chunk of bytes into the buffer, then write them out,
			// looping until we reach the end of the file (when read() returns
			// -1).
			// Note the combination of assignment and comparison in this while
			// loop. This is a common I/O programming idiom.
			long current_position = 0;
			while ((bytes_read = from.read(buffer)) != -1) {
				// Read bytes until EOF
				current_position = current_position + bytes_read;
				System.out.println(getMD5Hash(buffer));
				current_position = current_position + bytes_read;
			}
			System.out.println("Elapsed time = "
					+ (System.currentTimeMillis() - currMS) / 1000
					+ " to read ");
		}
		// Always close the streams, even if exceptions were thrown
		finally {
			if (from != null)
				try {
					from.close();
				} catch (IOException e) {
					;
				}
		}
	}

	public static void testChannelFile(String file, int buffer_len)
			throws IOException, NoSuchAlgorithmException {

		FileInputStream from = null; // Stream to read from source
		long currMS = System.currentTimeMillis();

		System.out.println("Current time = "
				+ ElapsedTime.getDateTime(new Date(currMS)));

		File f = new File(file);
		System.out.println("file size = " + f.length());
		try {
			from = new FileInputStream(file); // Create input stream
			ReadableByteChannel channel = from.getChannel();
			byte[] buffer = new byte[buffer_len]; // A buffer to hold file
			ByteBuffer buf = ByteBuffer.wrap(buffer);
			int numRead = 0;
			while (numRead >= 0) {
				// read() places read bytes at the buffer's position so the
				// position should always be properly set before calling read()
				// This method sets the position to 0

				// Read bytes from the channel
				numRead = channel.read(buf);
				if (buf.position() == 0)
					break;
				// The read() method also moves the position so in order to
				// read the new bytes, the buffer's position must be set back to
				// 0
				String hash = StringUtils.getHexString(getMD5ByteHash(buf
						.array()));
				String newHash = StringUtils.getHexString(StringUtils
						.getHexBytes(hash));
				if (hash.equalsIgnoreCase(newHash))
					System.out.println(hash);
				else
					System.out.println(hash + " " + newHash);
				buf.clear();
			}
			System.out.println("Elapsed time = "
					+ (System.currentTimeMillis() - currMS) / 1000
					+ " to read ");

		}
		// Always close the streams, even if exceptions were thrown
		finally {
			if (from != null)
				try {
					from.close();
				} catch (IOException e) {
					;
				}
		}
	}

	// Testing ...
	static int NUM = 100000;

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		Random rnd = new Random();
		for (int i = 0; i < 8000000; i++) {
			byte[] b = new byte[64];
			rnd.nextBytes(b);
			byte[] hash = HashFunctions.getTigerHashBytes(b);
		}
		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms");
	}

	public static void insertRecorts(long number) throws Exception {
		System.out.println("Inserting [" + number + "] Records....");
		long start = System.currentTimeMillis();
		Random rnd = new Random();
		for (int i = 0; i < number; i++) {
			byte[] b = new byte[64];
			rnd.nextBytes(b);

			byte[] hash = HashFunctions.getMD5ByteHash(b);
			HashChunkService.writeChunk(hash, b, 0, b.length, false);
		}
		System.out.println("Took " + (System.currentTimeMillis() - start)
				+ " ms");
	}

	public static String getSHAHash(byte[] input)
			throws NoSuchAlgorithmException, UnsupportedEncodingException,
			NoSuchProviderException {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		digest.reset();
		return StringUtils.getHexString(digest.digest(input));
	}

	public static byte[] getSHAHashBytes(byte[] input)
			throws NoSuchAlgorithmException, UnsupportedEncodingException,
			NoSuchProviderException {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		digest.reset();
		return digest.digest(input);
	}

	public static byte[] getTigerHashBytes(byte[] input)
			throws NoSuchAlgorithmException, UnsupportedEncodingException,
			NoSuchProviderException {
		
		algorithm.reset();
		return algorithm.digest(input);
	}

	/*
	public static String getMD5Hash(byte[] input) {
		MD5 md5 = new MD5();
		md5.Update(input);
		return md5.asHex();
	}

	public static byte[] getMD5ByteHash(byte[] input) {
		MD5 md5 = new MD5();
		md5.Update(input);
		byte[] b = md5.Final();
		md5 = null;
		return b;
	}
	*/

	public static String getMD5Hash(byte[] input) {
		try {
			return StringUtils.getHexString(getMD5ByteHash(input));
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static byte[] getMD5ByteHash(byte[] input) {
		try {
			MessageDigest digest = MessageDigest.getInstance("MD5");
			digest.reset();
			digest.update(input);
			return digest.digest();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static byte[] getAlder32ByteHash(byte[] input) {

		Adler32 alder = new Adler32();
		alder.update(input);
		ByteBuffer buf = ByteBuffer.wrap(new byte[8]);
		buf.putLong(alder.getValue());
		return buf.array();
	}

}
