package org.opendedup.util;

import java.io.File;

import java.security.SecureRandom;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.opendedup.collections.LongLongValueSerializer;
import org.opendedup.collections.SerializerKey;

import ec.util.MersenneTwisterFast;

public class MapDBTest {
	static BTreeMap<byte[], byte[]> indexMap = null;
	static DB hashDB = null;
	private static MersenneTwisterFast rnd = null;

	public static void main(String[] args) {
		hashDB = DBMaker.fileDB("c:\\temp\\watchhashes.db").closeOnJvmShutdown().fileMmapEnable()
				.allocateIncrement(1024 * 1024 * 5)
				// TODO memory mapped files enable here
				.make();
		File f  = new File("c:\\temp\\watchhashes.db");
		SecureRandom _rnd = new SecureRandom();
		rnd = new MersenneTwisterFast(_rnd.nextInt());
		indexMap = hashDB.treeMap("map", new SerializerKey(),new LongLongValueSerializer() ).createOrOpen();
		byte[] k = new byte[16];
		byte[] v = new byte[16];
		long tm = System.currentTimeMillis();
		int it = 0;
		for (int i = 0; i < 100_000_000; i++) {
			rnd.nextBytes(k);
			rnd.nextBytes(v);
			if(!indexMap.containsKey(k))
				indexMap.put(k, v);
			
			if (it == 1_000_000) {
				long ct = (System.currentTimeMillis() - tm) / 1000;
				long fl = f.length();
				System.out.println( ct + "," + i + "," +fl);
				it = 0;
				tm = System.currentTimeMillis();
			}
			it++;
		}
		indexMap.close();
		hashDB.close();

	}

}
