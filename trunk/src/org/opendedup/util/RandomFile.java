package org.opendedup.util;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import org.opendedup.sdfs.Main;

public class RandomFile {
	public static void writeRandomFile(String fileName, double size)
			throws IOException {
		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(fileName));
		long currentpos = 0;
		Random r = new Random();
		while (currentpos < size) {
			byte[] rndB = new byte[Main.CHUNK_LENGTH];
			r.nextBytes(rndB);
			out.write(rndB);
			currentpos = currentpos + rndB.length;
			out.flush();
		}
		out.flush();
		out.close();
	}

	public static void main(String[] args) throws IOException {
		long size = 100 * 1024L * 1024L * 1024L;
		writeRandomFile("/media/dedup/rnd.bin", size);
	}

}
