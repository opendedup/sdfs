package org.opendedup.util;

import java.io.File;

import java.nio.ByteBuffer;
import java.util.Random;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.opendedup.collections.LongLongValueSerializer;
import org.opendedup.collections.SerializerKey;

public class MapDBTest {
	static BTreeMap<byte[], byte[]> indexMap = null;
	static DB hashDB = null;

	public static void main(String[] args) {
		hashDB = DBMaker.fileDB("c:\\temp\\watchhashes.db").closeOnJvmShutdown().fileMmapEnable()
				.allocateIncrement(1024 * 1024 * 5)
				// TODO memory mapped files enable here
				.make();
		File f  = new File("c:\\temp\\watchhashes.db");
		indexMap = hashDB.treeMap("map", new SerializerKey(),new LongLongValueSerializer() ).createOrOpen();
		byte[] k = new byte[16];
		byte[] v = new byte[16];
		long tm = System.currentTimeMillis();
		int it = 0;
		ByteBuffer bf = ByteBuffer.wrap(v);
		for (int i = 0; i < 100_000_000; i++) {
			//rnd.nextBytes(k);
			bf.position(0);
			bf.putInt(i);
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
		
		Random r = new Random();
		int ki = -1;
		it=0;
		tm = System.currentTimeMillis();
		for (int i = 0; i < 20_000_000; i++) {
			
			while(ki < 0)
				ki = r.nextInt(10_000_000);
			
			bf.position(0);
			bf.putInt(ki);
			
			byte [] z = indexMap.get(v);
			if(z == null)
				System.out.println(ki);
				
			ki = -1;
			if (it == 1_000_000) {
				long ct = (System.currentTimeMillis() - tm);
				System.out.println( ct + "," + i);
				it = 0;
				tm = System.currentTimeMillis();
				
			}
			it++;
		}
		indexMap.close();
		hashDB.close();

	}

}
