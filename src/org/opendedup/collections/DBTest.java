package org.opendedup.collections;

import java.io.IOException;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.hashing.Murmur3HashEngine;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.util.StringUtils;

import ec.util.MersenneTwisterFast;

public class DBTest implements Runnable {

	SecureRandom srnd = new SecureRandom();
	MersenneTwisterFast rnd = new MersenneTwisterFast(srnd.nextInt());
	private static AtomicInteger k = new AtomicInteger();
	private static AtomicLong ct = new AtomicLong();
	private long rns = 0;
	private static ShardedProgressiveFileBasedCSMap m;
	private Murmur3HashEngine hc = new Murmur3HashEngine();

	public DBTest(long ins) {
		k.incrementAndGet();
		this.rns = ins;
	}

	@Override
	public void run() {
		
		try {
			
			for (int i = 0; i < rns; i++) {
				byte[] b = new byte[16];
				rnd.nextBytes(b);
				long z =rnd.nextLong();
				byte [] key = hc.getHash(b);
				ChunkData cm = new ChunkData(key, z);
				InsertRecord ir = m.put(cm,false);
				if(ir.getInserted())
					ct.incrementAndGet();
				/*
				if(z!= m.get(key)) {
					System.out.println("not found");
					System.exit(0);
				}
				*/
				m.claimKey(key, z);
				m.claimKey(key, z);
				m.removeClaimKey(key, z);
			}
			
			
			k.decrementAndGet();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashtableFullException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	public static void main(String[] args) throws IOException, InterruptedException, HashtableFullException {
		//db = DBMaker.fileDB(new File ("c:\\temp\\refentries.db")).fileChannelEnable().allocateStartSize(1024*1024).allocateIncrement(1024*1024*5).make();
		//cp = db.treeMap("refcount",Serializer.BYTE_ARRAY, Serializer.LONG).maxNodeSize(16).counterEnable().createOrOpen();
		System.out.println("blankHash=" + StringUtils.getHexString(new Murmur3HashEngine().getHash(new byte[4096])));
		/*
		String cc = args[0];
		long ins = Long.parseLong(args[1]);
		int threads = Integer.parseInt(args[2]);
		long mx = (long)(ins * 1.3);
		m=new ProgressiveFileBasedCSMap();
		m.init(mx, cc,.001);
		for (int i = 0; i < 	threads; i++) {
			DBTest tst = new DBTest(ins/threads);
			Thread th = new Thread(tst);
			th.start();
		}
		while(k.get() > 0) {
			long pins = ct.get();
			long tm = 30;
			Thread.sleep(tm*1000);
			long ci = ct.get()-pins;
			System.out.println("inserts per second=" +(ci/tm)+ " total=" +ct.get());
		}
		
		m.close();
		*/
	}

}
