package org.opendedup.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongConverter {
    public static long toLong(byte[] data) {
    	if (data != null) {
    	      ByteBuffer longBuffer = ByteBuffer.allocate(8)
    	          .order(ByteOrder.nativeOrder());
    	      longBuffer.put(data, 0, 8);
    	      longBuffer.flip();
    	      return longBuffer.getLong();
    	    }
    	    return 0;
    }
    public static byte[] toBytes(long data) {
    	ByteBuffer longBuffer = ByteBuffer.allocate(8)
    	        .order(ByteOrder.nativeOrder());
    	    longBuffer.clear();
    	    longBuffer.putLong(data);
    	    return longBuffer.array();
    }
}
