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

import java.io.IOException;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.regex.Pattern;

public class StringUtils {
	final static long pbc = 1125899906842624L;
	final static long tbc = 1099511627776L;
	final static long gbc = 1024 * 1024 * 1024;
	final static int mbc = 1024 * 1024;
	final static int kbc = 1024;

	public static void writeString(ObjectOutput out, String string)
			throws IOException {
		byte[] b = string.getBytes();
		out.writeShort((short) b.length);
		out.write(b);
	}

	public static String readString(ObjectInput in) throws IOException {
		short l = in.readShort();
		byte[] b = new byte[l];
		in.readFully(b);
		return new String(b);
	}
	
	static Pattern utfp = Pattern.compile("\\A(\n" +
			    "  [\\x09\\x0A\\x0D\\x20-\\x7E]             # ASCII\\n" +
			    "| [\\xC2-\\xDF][\\x80-\\xBF]               # non-overlong 2-byte\n" +
			    "|  \\xE0[\\xA0-\\xBF][\\x80-\\xBF]         # excluding overlongs\n" +
			    "| [\\xE1-\\xEC\\xEE\\xEF][\\x80-\\xBF]{2}  # straight 3-byte\n" +
			    "|  \\xED[\\x80-\\x9F][\\x80-\\xBF]         # excluding surrogates\n" +
			    "|  \\xF0[\\x90-\\xBF][\\x80-\\xBF]{2}      # planes 1-3\n" +
			    "| [\\xF1-\\xF3][\\x80-\\xBF]{3}            # planes 4-15\n" +
			    "|  \\xF4[\\x80-\\x8F][\\x80-\\xBF]{2}      # plane 16\n" +
			    ")*\\z", Pattern.COMMENTS);
	public static boolean checkIfString(byte [] b) throws UnsupportedEncodingException {
		

				  String phonyString = new String(b, "ISO-8859-1");
				  return utfp.matcher(phonyString).matches(); 
	}

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
		float sz = Float.parseFloat(capString.substring(0,
				capString.length() - 2));
		long fSize = 0;
		if (units.equalsIgnoreCase("PB"))
			fSize = (long) (sz * pbc);
		else if (units.equalsIgnoreCase("TB"))
			fSize = (long) (sz * tbc);
		else if (units.equalsIgnoreCase("GB"))
			fSize = (long) (sz * gbc);
		else if (units.equalsIgnoreCase("MB"))
			fSize = (long) (sz * mbc);
		else if (units.equalsIgnoreCase("KB"))
			fSize = (long) (sz * kbc);
		else {

			throw new IOException("unable to determine capacity of volume "
					+ capString);
		}
		return fSize;
	}

	public static int getSpecialCharacterCount(String s) {
		if (s == null || s.trim().isEmpty()) {
			return 0;
		}
		int theCount = 0;
		String specialChars = "/*!@#$%^&*()\"{}[]|\\?/<>,.';:+~`";
		for (int i = 0; i < s.length(); i++) {
			if (specialChars.contains(s.substring(i, i + 1))) {
				theCount++;
			}
		}
		return theCount;
	}

	public static void main(String[] args) throws IOException {
		byte [] b = new byte[256];
		Random rnd = new Random();
		rnd.nextBytes(b);
		System.out.println(checkIfString(b));
		System.out.println(checkIfString("alsjdf;ldsa;mcamcieowqureqpo".getBytes()));
		
	}
}
