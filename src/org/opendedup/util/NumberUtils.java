package org.opendedup.util;

import java.nio.ByteBuffer;

public class NumberUtils {
	public static byte[] getBytesfromLong(long num) {
		ByteBuffer buf = ByteBuffer.wrap(new byte[8]);
		buf.putLong(num);
		return buf.array();
	}

}
