package org.opendedup.sdfs.filestore.cloud;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractBatchStore;
import org.opendedup.sdfs.filestore.StringResult;

public class MultiDownload implements Runnable {
	AbstractBatchStore cs = null;
	LinkedBlockingQueue<StringResult> sbs = new LinkedBlockingQueue<StringResult>(
			Main.dseIOThreads * 2);
	private boolean done = false;
	Iterator<String> ck = null;
	String prefix;

	public MultiDownload(AbstractBatchStore cs,String prefix) throws IOException {
		this.cs = cs;
		this.prefix = prefix;
		this.ck = cs.getNextObjectList(prefix);
	}

	public void iterationInit(boolean deep, String folder) {
		try {
			Thread th = new Thread(this);
			th.start();
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to initialize", e);
		}
	}

	public StringResult getStringTokenizer() throws Exception {
		while (true) {
			StringResult st = sbs.poll(1, TimeUnit.SECONDS);
			if (st != null) {
				return st;
			} else if (this.done)
				break;
		}
		return null;
	}

	@Override
	public void run() {
		ArrayList<KeyGetter> al = new ArrayList<KeyGetter>();
		for (int i = 0; i < Main.dseIOThreads; i++) {
			KeyGetter kg = new KeyGetter();
			kg.md = this;
			Thread th = new Thread(kg);
			th.start();
			al.add(kg);
		}
		try {
			while (al.size() > 0) {
				Thread.sleep(100);
				ArrayList<KeyGetter> _al = new ArrayList<KeyGetter>();
				for (KeyGetter kd : al) {
					
					if (!kd.done) {
						_al.add(kd);
					}

				}
				if (_al.size() == 0) {
					this.done = true;
				}
				al = _al;
			}
		} catch (Exception e) {

		}
	}

	private String getNextKey() throws IOException {
		synchronized (this) {
		
			try {
				if (ck == null || !ck.hasNext()) {
					ck = cs.getNextObjectList(prefix);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("while doing recovery ", e);
				throw new IOException(e);
			}

			if (ck != null && ck.hasNext()) {
				return ck.next();
			}
		}
		return null;
	}

	private void addStringResult(String key) throws IOException,
			InterruptedException {
		if(cs.objectClaimed(key)) {

			StringResult sr = cs.getStringResult(key);
			//SDFSLogger.getLog().info("adding " + key +  "sr=" + sr.id + "ln=" + sr.st.countTokens());
			this.sbs.put(sr);
		}
	}

	private static final class KeyGetter implements Runnable {
		MultiDownload md = null;
		boolean done = false;

		@Override
		public void run() {
			String st = null;
			try {
				st = md.getNextKey();
				while (st != null) {
					
					//SDFSLogger.getLog().info("Key = " + ost);
					try {
						md.addStringResult(st);
						st = md.getNextKey();
					}catch(Exception e) {
						SDFSLogger.getLog().error("error getting string result for " + st, e);
						st = md.getNextKey();
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("error  getting string result for " + st, e);
			}
			done = true;

		}

	}

}
