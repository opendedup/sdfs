package org.opendedup.util;

import java.io.File;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.RocksDBMap;
import org.opendedup.collections.ShardedProgressiveFileBasedCSMap2;
import org.opendedup.hashing.VariableSipHashEngine;
import org.opendedup.sdfs.filestore.ChunkData;

import ec.util.MersenneTwisterFast;



public class RocksDBProdTest {
	static AbstractHashesMap hashDB = null;
	
	static AtomicLong transactions = new AtomicLong();
	static AtomicLong inserts = new AtomicLong();
	static AtomicLong dupes = new AtomicLong();
	static VariableSipHashEngine ve = null;
	public static void main(String[] args) throws IOException, HashtableFullException, InterruptedException, NoSuchAlgorithmException {
		int rb = Integer.parseInt(args[1]);
		String fp = args[0];
		long maxSz= Long.parseLong(args[2]);
		int threads = Integer.parseInt(args[3]);
		int numRuns = Integer.parseInt(args[4]);
		long range = Long.parseLong(args[5]);
		File f  = new File(fp);
		ve = new VariableSipHashEngine();
		if(rb==0) {
		hashDB = new ShardedProgressiveFileBasedCSMap2();
		
		}
		else if(rb == 1) {
			hashDB = new RocksDBMap();
		}
		
		
		
		hashDB.init(maxSz, f.getPath(), .001);
		inserts.set(hashDB.getSize());
		ArrayList<DataWriter> dr = new ArrayList<DataWriter>();
		for(int i = 0;i<threads;i++) {
			DataWriter d = new DataWriter(numRuns,range);
			Thread th = new Thread(d);
			th.start();
			dr.add(d);
		}
		boolean running = true;
		long st = System.currentTimeMillis();
		long ci = transactions.get();
		long dd = dupes.get();
		ShutdownHook shutdownHook = new ShutdownHook(dr,
				hashDB);
		Runtime.getRuntime().addShutdownHook(shutdownHook);
		while(running) {
			Thread.sleep(30000);
			long nci = transactions.get();
			long dd1 = dupes.get();
			long ddif = dd1-dd;
			long ic = inserts.get();
			long ts = (nci-ci);
			double ddr = (double)ddif/(double)ts;
			long tps = (nci-ci)/30;
			System.out.println("Transactions = " + ts + " tps = " + tps + " dedupe rate = " + ddr+ " dedupes = " + ddif+" transactions=" + nci + " inserts=" + ic);
			ci = nci;
			dd =dd1;
			running = false;
			for(DataWriter d : dr) {
				if(!d.done) {
					running = true;
				}
			}
			
		}
		long dur = (System.currentTimeMillis() - st)/1000;
		long tps = transactions.get()/dur;
		System.out.println("took " +dur + " seconds to insert " +transactions.get() +" tps = " + tps);
		
		hashDB.close();

	}
	
	private static class DataWriter implements Runnable {
		int numRuns;
		long range;
		boolean done = false;
		private MersenneTwisterFast rnd = null;
		public DataWriter(int numRuns,long range) {
			this.numRuns = numRuns;
			this.range =range;
			SecureRandom srnd = new SecureRandom();
			rnd = new MersenneTwisterFast(srnd.nextInt());
		}
		

		@Override
		public void run() {
			byte[] v = new byte[16];
			ByteBuffer bf = ByteBuffer.wrap(v);
			
			long ki = -1;
			for (int i = 0; i < numRuns; i++) {
				if(done == true) {
					break;
				}
				while(ki < 0)
					ki = rnd.nextLong(range);
				bf.position(0);
				bf.putLong(ki);
				byte [] k = ve.getHash(v);
				ChunkData cm = new ChunkData(k,i);
				transactions.incrementAndGet();
				try {
					InsertRecord ir = hashDB.put(cm,false);
					if(ir.getInserted())
						inserts.incrementAndGet();
					else
						dupes.incrementAndGet();
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
	
	static class ShutdownHook extends Thread {
		ArrayList<DataWriter> dr;
		AbstractHashesMap hashDB;

		public ShutdownHook(ArrayList<DataWriter> dr,AbstractHashesMap hashDB) {
			this.dr = dr;
			this.hashDB = hashDB;
		}

		@Override
		public void run() {

			System.out.println("Please Wait while shutting down the service");
			System.out.println("Data Can be lost if this is interrupted");
			for(DataWriter d :dr ) {
				d.done = true;
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			hashDB.close();
			System.out.println("Shut Down Cleanly");

		}
	}

}
