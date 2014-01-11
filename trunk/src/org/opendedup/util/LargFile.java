package org.opendedup.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class LargFile {
	public static void writeFile(String path, int size, int bs, boolean unique)
			throws IOException {
		long len = 1024L * 1024L * 1024L * size;
		long sz = 0;
		File log = new File(path + "log");
		File f = new File(path);
		java.io.FileWriter writer = new FileWriter(log);
		System.out.println("unique data=" + unique);
		FileOutputStream str = new FileOutputStream(f, true);
		Random rnd = new Random();
		byte[] b = new byte[bs];
		if (!unique)
			rnd.nextBytes(b);
		System.out.println("1:" + len);
		long time = System.currentTimeMillis();
		int writes = 0;
		int interval = (32768 * 10000) / bs;
		while (sz < len) {
			if (unique) {
				rnd.nextBytes(b);
			}
			ByteBuffer buf = ByteBuffer.wrap(b);
			str.getChannel().write(buf);
			sz = sz + b.length;
			if (writes > interval) {

				float mb = (float) (writes * bs) / (1024 * 1024);
				float duration = (float) (System.currentTimeMillis() - time) / 1000;
				float mbps = mb / duration;
				System.out.println(mbps + " (mb/s)");
				writer.write(Float.toString(mbps) + "\n");
				time = System.currentTimeMillis();
				writes = 0;

			} else {
				writes++;
			}
		}
		writer.flush();
		writer.close();
		str.close();
	}

	public static void main(String[] args) throws IOException {
		writeFile(args[0], Integer.parseInt(args[1]), 1048576,
				Boolean.parseBoolean(args[2]));
	}

}
