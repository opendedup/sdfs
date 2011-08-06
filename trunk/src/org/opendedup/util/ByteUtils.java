package org.opendedup.util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

public class ByteUtils {

	public static byte[] serializeHashMap(HashMap<String, String> map) {
		StringBuffer keys = new StringBuffer();
		Iterator<String> iter = map.keySet().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			keys.append(key);
			if (iter.hasNext())
				keys.append(",");
		}
		StringBuffer values = new StringBuffer();
		iter = map.values().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			values.append(key);
			if (iter.hasNext())
				values.append(",");
		}
		byte[] kb = keys.toString().getBytes();
		byte[] vb = values.toString().getBytes();
		byte[] out = new byte[kb.length + vb.length + 8];
		ByteBuffer buf = ByteBuffer.wrap(out);
		buf.putInt(kb.length);
		buf.put(kb);
		buf.putInt(vb.length);
		buf.put(vb);
		return buf.array();
	}

	public static HashMap<String, String> deSerializeHashMap(byte[] b) {
		ByteBuffer buf = ByteBuffer.wrap(b);
		byte[] kb = new byte[buf.getInt()];
		buf.get(kb);
		byte[] vb = new byte[buf.getInt()];
		buf.get(vb);
		String[] keys = new String(kb).split(",");
		String[] values = new String(vb).split(",");
		HashMap<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < keys.length; i++) {
			map.put(keys[i], values[i]);
		}
		return map;
	}

}
