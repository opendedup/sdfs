package org.opendedup.util;

import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Random;

import org.opendedup.hashing.HashFunctions;

/**
 * Used to generate random GUID(s) with SDFS. GUIDS are generated off of the
 * SecureRandom function within java @see java.security.SecureRandom .
 */
public class RandomGUID extends Object {

	public String valueBeforeMD5 = "";
	public String valueAfterMD5 = "";
	private static Random myRand;
	private static SecureRandom mySecureRand;

	private static String s_id;

	/**
	 * Static block to take care of one time secureRandom seed. It takes a few
	 * seconds to initialize SecureRandom. You might want to consider removing
	 * this static block or replacing it with a "time since first loaded" seed
	 * to reduce this time. This block will run only once per JVM instance.
	 */

	static {
		mySecureRand = new SecureRandom();
		long secureInitializer = mySecureRand.nextLong();
		myRand = new Random(secureInitializer);
		try {
			s_id = InetAddress.getLocalHost().toString();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Default constructor. With no specification of security option, this
	 * constructor defaults to lower security, high performance.
	 */
	public RandomGUID() {
		getRandomGUID(true);
	}

	/**
	 * Constructor with security option. Setting secure true enables each random
	 * number generated to be cryptographically strong. Secure false defaults to
	 * the standard Random function seeded with a single cryptographically
	 * strong random number.
	 */
	public RandomGUID(boolean secure) {
		getRandomGUID(secure);
	}

	/**
	 * This method is used to create random SHA-256 hashes
	 * 
	 * @return the SHA-256 hash from random bytes.
	 * @throws IOException
	 */

	public byte[] getRandomShaHash() throws IOException {
		byte[] b = new byte[64];
		mySecureRand.nextBytes(b);
		try {
			return HashFunctions.getSHAHashBytes(b);
		} catch (Exception e) {
			throw new IOException("unable to get random hash " + e.toString());
		}
	}

	/**
	 * Method to generate the random GUID
	 */
	private void getRandomGUID(boolean secure) {
		StringBuffer sbValueBeforeMD5 = new StringBuffer();

		try {
			long time = System.currentTimeMillis();
			long rand = 0;

			if (secure) {
				rand = mySecureRand.nextLong();
			} else {
				rand = myRand.nextLong();
			}
			sbValueBeforeMD5.append(s_id);
			sbValueBeforeMD5.append(":");
			sbValueBeforeMD5.append(Long.toString(time));
			sbValueBeforeMD5.append(":");
			sbValueBeforeMD5.append(Long.toString(rand));

			valueBeforeMD5 = sbValueBeforeMD5.toString();

			valueAfterMD5 = HashFunctions.getMD5Hash(valueBeforeMD5.getBytes());

		} catch (Exception e) {
			System.out.println("Error:" + e);
		}
	}

	/**
	 * 
	 * Convert to the standard format for GUID (Useful for SQL Server
	 * UniqueIdentifiers, etc.) Example: C2FEEEAC-CFCD-11D1-8B05-00600806D9B6
	 * 
	 * @return the random GUID
	 * 
	 */
	public String toString() {
		String raw = valueAfterMD5.toUpperCase();
		StringBuffer sb = new StringBuffer();
		sb.append(raw.substring(0, 8));
		sb.append("-");
		sb.append(raw.substring(8, 12));
		sb.append("-");
		sb.append(raw.substring(12, 16));
		sb.append("-");
		sb.append(raw.substring(16, 20));
		sb.append("-");
		sb.append(raw.substring(20));
		return sb.toString();
	}

	/**
	 * A static utility method to create a random GUID. Example:
	 * C2FEEEAC-CFCD-11D1-8B05-00600806D9B6
	 * 
	 * @return the random GUID
	 */

	public static String getGuid() {
		RandomGUID myGUID = new RandomGUID();
		return myGUID.toString();
	}

	/**
	 * A static utility method used to generate unique CIDs for VMDK files.
	 * 
	 * @return the VMDK CID
	 */

	public static String getVMDKCID() {
		RandomGUID guid = new RandomGUID(true);
		String uuidStr = guid.valueAfterMD5.toLowerCase();
		return uuidStr.substring(0, 8);
	}

	/**
	 * A utility method used to generate unique CIDs for VMDK files.
	 * 
	 * @return the VMDK CID
	 */

	public static String getVMDKGUID() {
		RandomGUID guid = new RandomGUID(true);

		String uuidStr = guid.valueAfterMD5.toLowerCase();
		StringBuffer sb = new StringBuffer();
		sb.append(uuidStr.substring(0, 2));
		sb.append(" ");
		sb.append(uuidStr.substring(2, 4));
		sb.append(" ");
		sb.append(uuidStr.substring(4, 6));
		sb.append(" ");
		sb.append(uuidStr.substring(6, 8));
		sb.append(" ");
		sb.append(uuidStr.substring(8, 10));
		sb.append(" ");
		sb.append(uuidStr.substring(10, 12));
		sb.append(" ");
		sb.append(uuidStr.substring(12, 14));
		sb.append(" ");
		sb.append(uuidStr.substring(14, 16));
		sb.append("-");
		sb.append(uuidStr.substring(16, 18));
		sb.append(" ");
		sb.append(uuidStr.substring(18, 20));
		sb.append(" ");
		sb.append(uuidStr.substring(20, 22));
		sb.append(" ");
		sb.append(uuidStr.substring(22, 24));
		sb.append(" ");
		sb.append(uuidStr.substring(24, 26));
		sb.append(" ");
		sb.append(uuidStr.substring(26, 28));
		sb.append(" ");
		sb.append(uuidStr.substring(28, 30));
		sb.append(" ");
		sb.append(uuidStr.substring(30, 32));
		return sb.toString();
	}

	/*
	 * Demonstraton and self test of class
	 */
	public static void main(String args[]) {
		RandomGUID uuid = new RandomGUID();
		String randomUUIDString = uuid.valueAfterMD5.toLowerCase();
		System.out.println(randomUUIDString);
	}
}