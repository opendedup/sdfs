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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang3.StringEscapeUtils;
import org.opendedup.logging.SDFSLogger;

public class ByteUtils {

	public static byte[] serializeHashMap(HashMap<String, String> map) {
		StringBuffer keys = new StringBuffer();
		Iterator<String> iter = map.keySet().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
				keys.append(StringEscapeUtils.escapeCsv(key));
				if (iter.hasNext())
					keys.append(",");

		}
		StringBuffer values = new StringBuffer();
		iter = map.values().iterator();
		while (iter.hasNext()) {
			String key = iter.next();
				values.append(StringEscapeUtils.escapeCsv(key));
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
			try {
				map.put(StringEscapeUtils.unescapeCsv(keys[i]), StringEscapeUtils.unescapeCsv(values[i]));
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to get value for " + i + " " + keys[i] + " vl" +values.length,e);
			}
		}
		return map;
	}

}
