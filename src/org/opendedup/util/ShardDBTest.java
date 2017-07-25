package org.opendedup.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.ShardedProgressiveFileBasedCSMap2;
import org.opendedup.sdfs.filestore.ChunkData;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class ShardDBTest {
	static ShardedProgressiveFileBasedCSMap2 hashDB = null;
	
	public static void main(String[] args) throws IOException, HashtableFullException {
		hashDB = new ShardedProgressiveFileBasedCSMap2();
		
		
		File f  = new File("c:\\temp\\shards\\shard3");
		hashDB.init(150_000_000, f.getPath(), .001);
		byte[] v = new byte[16];
		ByteBuffer bf = ByteBuffer.wrap(v);
		long tm = System.currentTimeMillis();
		int it = 0;
		HashFunction hf = Hashing.murmur3_128(6442);
		for (int i = 0; i < 100_000_000; i++) {
			bf.position(0);
			bf.putInt(i);
			byte [] k = hf.hashBytes(v).asBytes();
			if(!hashDB.containsKey(k)) {
				ChunkData cm = new ChunkData(k,i);
				hashDB.put(cm,false);
			}
			if (it == 1_000_000) {
				long ct = (System.currentTimeMillis() - tm);
				long fl = f.length();
				System.out.println( ct + "," + i + "," +fl);
				it = 0;
				tm = System.currentTimeMillis();
			}
			it++;
		}Random r = new Random();
		int ki = -1;
		it=0;
		tm = System.currentTimeMillis();
		for (int i = 0; i < 10_000_000; i++) {
			while(ki < 0)
				ki = r.nextInt(10_000_000);
			bf.position(0);
			bf.putInt(ki);
			byte [] k = hf.hashBytes(v).asBytes();
			if(hashDB.get(k) == -1) {
				System.out.println("no hit" + ki);
			}
			ki =-1;
			if (it == 1_000_000) {
				long ct = (System.currentTimeMillis() - tm);
				long fl = f.length();
				System.out.println( ct + "," + i + "," +fl);
				it = 0;
				tm = System.currentTimeMillis();
			}
			it++;
		}
		hashDB.close();

	}

}
