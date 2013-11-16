package org.opendedup.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.lucene.util.OpenBitSet;

public class OpenBitSetSerialize {
	public static void writeOut(String fileName, OpenBitSet set)
			throws IOException {
		FileChannel fc = null;
		try {
			File f = new File(fileName);
			if (f.exists())
				f.delete();
			Path p = Paths.get(f.getPath());
			fc = FileChannel.open(p, StandardOpenOption.CREATE,
					StandardOpenOption.SPARSE, StandardOpenOption.WRITE,
					StandardOpenOption.READ);
			synchronized (set) {
				ByteBuffer buff = ByteBuffer.allocate(4);
				buff.putInt(set.getNumWords());
				buff.flip();
				fc.write(buff);
				long[] nums = set.getBits();
				buff.clear();
				buff.putInt(nums.length);
				buff.flip();
				fc.write(buff);

				buff = ByteBuffer.allocate(8);

				for (long num : nums) {
					buff.position(0);
					buff.putLong(num);
					buff.flip();
					fc.write(buff);
				}
			}
		} finally {
			if (fc != null)
				fc.close();
		}
	}

	public static OpenBitSet readIn(String fileName) throws IOException {
		FileChannel fc = null;
		try {
			File f = new File(fileName);
			Path p = Paths.get(f.getPath());
			fc = FileChannel.open(p, StandardOpenOption.READ);
			ByteBuffer buff = ByteBuffer.allocate(4);
			fc.read(buff);
			int numWords = buff.getInt(0);
			buff.clear();
			buff.position(0);
			fc.read(buff);
			int numL = buff.getInt(0);
			buff = ByteBuffer.allocate(8);
			long[] l = new long[numL];
			for (int i = 0; i < numL; i++) {
				buff.position(0);
				fc.read(buff);
				buff.position(0);
				l[i] = buff.getLong();
			}
			fc.close();
			return new OpenBitSet(l, numWords);
		} finally {
			if (fc != null)
				fc.close();
		}

	}

	public static void main(String[] args) throws IOException {
		OpenBitSet set = new OpenBitSet(1000000000L);
		long pos = 1010;
		set.set(pos);
		System.out.println(set.get(pos));
		writeOut("/tmp/bitset.bt", set);
		System.out.println(readIn("/tmp/bitset.bt").get(pos));

	}

}
