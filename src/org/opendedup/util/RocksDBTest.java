package org.opendedup.util;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Random;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import ec.util.MersenneTwisterFast;

public class RocksDBTest {
	private static MersenneTwisterFast rnd = null;

	
	
	public static void main(String[] args) {
		RocksDB.loadLibrary();
		SecureRandom _rnd = new SecureRandom();
		rnd = new MersenneTwisterFast(_rnd.nextInt());
		try (@SuppressWarnings("resource")
		final Options options = new Options().setCreateIfMissing(true)) {
		      options.setCompactionStyle(CompactionStyle.UNIVERSAL);
		      options.setCompressionType(CompressionType.NO_COMPRESSION);
		      BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
		      blockConfig.setFilter(new BloomFilter(16,false));
		      blockConfig.setBlockSize(64*1024);
		      blockConfig.setFormatVersion(2);
		      //blockConfig.setCacheIndexAndFilterBlocks(true);
		      options.setTableFormatConfig(blockConfig);
		      options.setWriteBufferSize(128*1024*1024);
		      options.setAllowMmapWrites(true);
		      options.setAllowMmapReads(true);
		      options.setMaxOpenFiles(-1);
		      options.setTargetFileSizeBase(1024*1024*1024*8);
		      options.setAdviseRandomOnOpen(true);
		      //options.setAllowMmapReads(true);
			    // a factory method that returns a RocksDB instance
		      WriteOptions wo = new WriteOptions();
		      wo.setDisableWAL(true);
			    try (final RocksDB db = RocksDB.open(options, "c:\\temp\\rocksdb\\")) {
			    	System.out.println("size=" +db.getLongProperty("rocksdb.estimate-num-keys"));
			    	db.compactRange();
					byte[] v = new byte[16];
					rnd.nextBytes(v);
					ByteBuffer bf = ByteBuffer.wrap(v);
					long tm = System.currentTimeMillis();
					int it = 0;
					for (int i = 0; i < 100_000_000; i++) {
						//rnd.nextBytes(k);
						bf.position(0);
						bf.putInt(i);
						byte [] z =db.get(v);
						if(z == null)
						db.put(wo,v, v);
						else
							System.out.println(z.length);
						
						if (it == 1_000_000) {
							long ct = (System.currentTimeMillis() - tm);
							System.out.println( ct + "," + i);
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
						
						byte [] z = db.get(v);
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
					db.close();
			        
			    }
			  } catch (RocksDBException e) {
			    e.printStackTrace();
			    System.exit(1);
			  }

	}

}
