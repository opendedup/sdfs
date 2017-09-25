package org.opendedup.sdfs.mgmt.cli;

import java.io.ByteArrayInputStream;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.IOUtils;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;


public class ProcessBatchGetBlocks {
	private static BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	private static ThreadPoolExecutor executor = new ThreadPoolExecutor(1, Main.writeThreads, 15, TimeUnit.MINUTES, worksQueue,
			new ThreadPoolExecutor.CallerRunsPolicy());
	public static long runCmd(ArrayList<byte[]> hashes, String server,
			int port, String password, boolean useSSL) throws Exception,
			ClassNotFoundException, HashtableFullException {
		Exception he = null;
		for(int t = 0; t <10;t++) {
		InputStream in = null;
		PostMethod method = null;
		try {
			SDFSLogger.getLog().debug("getting hashes [" + hashes.size() + "]");
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream obj_out = new ObjectOutputStream(bos);
			obj_out.writeObject(hashes);
			String file = com.google.common.io.BaseEncoding.base64Url().encode(
					CompressionUtils.compressSnappy(bos.toByteArray()));
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=batchgetblocks&options=ilovemg",
					"ninja");
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			try{
				//String hmac = MgmtServerConnection.getAuth(password);
			method = MgmtServerConnection.connectAndPost(server, port,
					 sb.toString(),password, "", file, useSSL);
			in = method.getResponseBodyAsStream();
			SDFSLogger.getLog().debug("reading imported blocks");
			IOUtils.copy(in, out);
			formatter.close();
			
			}finally {
				if(method != null)
					try {
				method.releaseConnection();
					}catch(Exception e) {}
			}
			byte[] sh = CompressionUtils.decompressSnappy(out.toByteArray());
			ObjectInputStream obj_in = new ObjectInputStream(
					new ByteArrayInputStream(sh));
			@SuppressWarnings("unchecked")
			List<HashChunk> hck = (List<HashChunk>) obj_in.readObject();
			obj_in.close();
			if (hck.size() != hashes.size())
				throw new IOException("unable to import all blocks requested ["
						+ hashes.size() + "] and received [" + hck.size() + "]");
			AtomicLong imsz = new AtomicLong();
			ArrayList<DataWriter> th = new ArrayList<DataWriter>();
			for (int i = 0; i < hck.size(); i++) {
				DataWriter dw = new DataWriter();
				dw._hc = hck.get(i);
				dw.imsz = imsz;
				th.add(dw);
				executor.execute(dw);
			}
			Exception e1 = null;
			
			for(;;) {
				int nd = 0;
				for(DataWriter dw : th) {
					if(dw.done) {
						if(dw.e1 != null) {
							e1 = dw.e1;
						}
					} else {
						nd++;
					}
				}
				if(nd == 0)
					break;
				Thread.sleep(10);
			}
			if(e1!=null)
				throw e1;
			SDFSLogger.getLog().debug("imported " + hck.size());
			return imsz.get();

		} catch(Exception e) {
			he = e;
			Thread.sleep(1000);
		}finally {	
			if (in != null) {
				try {
					in.close();
				} catch (Exception e) {
				}
			}
			if (method != null)
				try {
					method.releaseConnection();
				} catch (Exception e) {
				}
		}
		}
		throw new IOException(he);

	}
	
	private static class DataWriter implements Runnable {
		HashChunk _hc = null;
		AtomicLong imsz = null;
		Exception e1 = null;
		boolean done = false;
		@Override
		public void run() {
			try {
				HCServiceProxy.writeChunk(_hc.getName(), _hc.getData(),1,null);
				imsz.addAndGet(_hc.getData().length);
			} catch (IOException e) {
				e1 = e;
			} catch (HashtableFullException e) {
				e1 = e;
			} finally {
				done = true;
			}
			
		}
		
	}
}