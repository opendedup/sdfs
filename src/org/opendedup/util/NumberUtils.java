package org.opendedup.util;

import java.nio.ByteBuffer;

public class NumberUtils {
	public static byte[] getBytesfromLong(long num) {
		ByteBuffer buf = ByteBuffer.wrap(new byte[8]);
		buf.putLong(num);
		return buf.array();
	}

	public static void main(String[] args) {
		int cl = 131072;
		long pos = 4096;
		int len = 4096;
		double spos = Math.ceil(((double) pos / (double) cl));
		long ep = pos + len;
		double epos = Math.floor(((double) ep / (double) cl));
		long ls = (long) spos;
		long es = (long) epos;
		if (es <= ls)
			System.out.println("eeks");
		System.out.println("spos=" + ls + " epos=" + es);
	}

}
