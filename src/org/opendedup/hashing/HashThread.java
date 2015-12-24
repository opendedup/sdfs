package org.opendedup.hashing;

import java.util.ArrayList;



import java.util.List;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.BufferClosedException;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class HashThread implements Runnable {

	WritableCacheBuffer runnable=null;


	public HashThread(WritableCacheBuffer buf) {
		runnable = buf;
	}

	@Override
	public void run() {
		try {
		
					if (Main.chunkStoreLocal) {
							try {
								runnable.close();
							} catch (Exception e) {
								SDFSLogger.getLog().fatal(
										"unable to execute thread", e);
							}
					} else {
						ArrayList<HashLocPair> ar = new ArrayList<HashLocPair>();
						
						if (HashFunctionPool.max_hash_cluster == 1) {
							AbstractHashEngine hc = null;
							try {
								 hc = SparseDedupFile.hashPool
										.borrowObject();
								runnable.startClose();
								
								byte[] hash = null;


									byte[] b = runnable.getFlushedBuffer();
									hash = hc.getHash(b);
									
									HashLocPair p = new HashLocPair();
									p.hash = hash;
									p.pos = 0;
									p.len = b.length;
									p.hashloc = new byte[8];
									p.hash = hash;
									p.data = b;
									ar.add(p);
									runnable.setAR(ar);
								} catch (BufferClosedException e) {
									
								} finally {
									SparseDedupFile.hashPool.returnObject(hc);
								}
						} else {
							VariableHashEngine hc = (VariableHashEngine) SparseDedupFile.hashPool
									.borrowObject();
							try {
							runnable.startClose();
								
								List<Finger> fs = hc.getChunks(runnable
										.getFlushedBuffer());
								int _pos = 0;
								for (Finger f : fs) {
									HashLocPair p = new HashLocPair();
									p.hash = f.hash;
									p.hashloc = f.hl;
									p.len = f.len;
									p.offset = 0;
									p.nlen = f.len;
									p.data = f.chunk;
									p.pos = _pos;
									_pos += f.chunk.length;
									ar.add(p);
								}
								runnable.setAR(ar);
						} catch (BufferClosedException e) {
							
						} finally {
							SparseDedupFile.hashPool.returnObject(hc);
						}
							}
						
						HCServiceProxy.batchHashExists(ar);
						runnable.endClose();
					}
		}catch(Exception e) {
			SDFSLogger.getLog().error("Unable to hash contents", e);
		}
				

			
		}

}
