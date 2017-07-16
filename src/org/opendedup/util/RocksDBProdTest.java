package org.opendedup.util;

import java.io.File;



import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.MapDBMap;
import org.opendedup.collections.RocksDBMap;
import org.opendedup.collections.ShardedProgressiveFileBasedCSMap2;
import org.opendedup.collections.ShardedProgressiveFileBasedCSMap3;
import org.opendedup.hashing.VariableSipHashEngine;
import org.opendedup.sdfs.filestore.ChunkData;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;



public class RocksDBProdTest {
	static AbstractHashesMap hashDB = null;
	
	static AtomicLong inserts = new AtomicLong();
	static VariableSipHashEngine ve = null;
	public static void main(String[] args) throws IOException, HashtableFullException, InterruptedException, NoSuchAlgorithmException {
		int rb = 1;
		File f  = null;
		ve = new VariableSipHashEngine();
		if(rb==0) {
		hashDB = new ShardedProgressiveFileBasedCSMap2();
		f  = new File("c:\\temp\\psharddb2\\shards");
		}
		else if(rb == 1) {
			hashDB = new RocksDBMap();
			f  = new File("c:\\temp\\rdbshard");
		}
		else if(rb==2) {
			hashDB = new ShardedProgressiveFileBasedCSMap3();
			f  = new File("c:\\temp\\psharddb3\\shards");
		}else {
			hashDB = new MapDBMap();
			f  = new File("c:\\temp\\mdbshard");
		}
		
		
		hashDB.init(1_000_000_000, f.getPath(), .001);
		ArrayList<DataWriter> dr = new ArrayList<DataWriter>();
		for(int i = 0;i<16;i++) {
			DataWriter d = new DataWriter(10_000_000,300_000_000);
			Thread th = new Thread(d);
			th.start();
			dr.add(d);
		}
		boolean running = true;
		long st = System.currentTimeMillis();
		long ci = inserts.get();
		while(running) {
			Thread.sleep(30000);
			long nci = inserts.get();
			long tps = (nci-ci)/30;
			System.out.println("Transactions = " + (nci-ci) + " tps = " + tps);
			ci = nci;
			running = false;
			for(DataWriter d : dr) {
				if(!d.done) {
					running = true;
				}
			}
			
		}
		long dur = (System.currentTimeMillis() - st)/1000;
		long tps = inserts.get()/dur;
		System.out.println("took " +dur + " seconds to insert " +inserts.get() +" tps = " + tps);
		
		hashDB.close();

	}
	
	private static class DataWriter implements Runnable {
		int numRuns;
		int range;
		boolean done = false;
		public DataWriter(int numRuns,int range) {
			this.numRuns = numRuns;
			this.range =range;
		}
		

		@Override
		public void run() {
			Random r = new Random();
			byte[] v = new byte[16];
			ByteBuffer bf = ByteBuffer.wrap(v);
			
			int ki = -1;
			for (int i = 0; i < numRuns; i++) {
				while(ki < 0)
					ki = r.nextInt(range);
				bf.position(0);
				bf.putInt(ki);
				byte [] k = ve.getHash(v);
				ChunkData cm = new ChunkData(k,i);
				inserts.incrementAndGet();
				try {
					hashDB.put(cm,false);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(-1);
				} catch (HashtableFullException e) {
					e.printStackTrace();
					System.exit(-1);
				}
				ki =-1;
			}
			done = true;
			
		}
		
	}

}
