package org.opendedup.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class StringUtils {
	final static long tbc = 1099511627776L;
	final static long gbc = 1024 * 1024 * 1024;
	final static int mbc = 1024 * 1024;

	static final byte[] HEX_CHAR_TABLE = { (byte) '0', (byte) '1', (byte) '2',
			(byte) '3', (byte) '4', (byte) '5', (byte) '6', (byte) '7',
			(byte) '8', (byte) '9', (byte) 'a', (byte) 'b', (byte) 'c',
			(byte) 'd', (byte) 'e', (byte) 'f' };

	public static String getHexString(byte[] raw) {
		String hexStr = null;
		byte[] hex = new byte[2 * raw.length];
		int index = 0;

		for (byte b : raw) {
			int v = b & 0xFF;
			hex[index++] = HEX_CHAR_TABLE[v >>> 4];
			hex[index++] = HEX_CHAR_TABLE[v & 0xF];
		}
		try {
			hexStr = new String(hex, "ASCII");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hexStr;
	}

	public static byte[] getHexBytes(String hex) {
		byte[] bts = new byte[hex.length() / 2];
		for (int i = 0; i < bts.length; i++) {
			bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2),
					16);
		}
		return bts;
	}

	public static long parseSize(String capString) throws IOException {
		String units = capString.substring(capString.length() - 2);
		float sz = Float
				.parseFloat(capString.substring(0, capString.length() - 2));
		long fSize = 0;
		if (units.equalsIgnoreCase("TB"))
			fSize = (long) (sz * tbc);
		else if (units.equalsIgnoreCase("GB"))
			fSize = (long) (sz * gbc);
		else if (units.equalsIgnoreCase("MB"))
			fSize = (long) (sz * mbc);
		else {

			throw new IOException("unable to determine capacity of volume "
					+ capString);
		}
		return fSize;
	}

	public static void main(String[] args) throws IOException {
		System.out.println("Volume = " + parseSize("1TB"));
	}
}
