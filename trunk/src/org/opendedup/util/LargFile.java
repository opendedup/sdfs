package org.opendedup.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class LargFile {
	public static void writeFile(String path, int size) throws IOException {
		long len = 1024L * 1024L * 1024L * size;
		long sz = 0;
		File f = new File(path);
		FileOutputStream str = new FileOutputStream(f, true);
		Random rnd = new Random();
		byte[] b = new byte[32768];
		System.out.println("1:" + len);
		long time = System.currentTimeMillis();
		int writes = 0;
		while (sz < len) {
			rnd.nextBytes(b);
			ByteBuffer buf = ByteBuffer.wrap(b);
			str.getChannel().write(buf);
			sz = sz + b.length;
			if (writes > 5000) {
				float mb = (float) (writes * 32768) / (1024 * 1024);
				float duration = (float) (System.currentTimeMillis() - time) / 1000;
				float mbps = mb / duration;
				System.out.println(mbps + " (mb/s)");
				time = System.currentTimeMillis();
				writes = 0;

			} else {
				writes++;
			}
		}
	}

	public static void main(String[] args) throws IOException {
		for (int i = 0; i < 40; i++) {
			writeFile("/media/dedup/test-" + i + ".bin", 10);
		}
	}

}
