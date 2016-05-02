package org.opendedup.sdfs.filestore;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.CommandLineProgressBar;

public class ConsistancyCheck {
	static AtomicLong records = new AtomicLong();
	static AtomicLong recordsRecovered = new AtomicLong();
	static AtomicLong count = new AtomicLong();
	static AtomicLong currentCount = new AtomicLong();
	static AtomicLong corruption = new AtomicLong();

	public static synchronized void runCheck(AbstractHashesMap map,
			AbstractChunkStore store) {
		try {
			store.iterationInit(false);
			System.out
					.println("Running Consistancy Check on DSE, this may take a while");
			SDFSLogger.getLog().warn(
					"Running Consistancy Check on DSE, this may take a while");
			SDFSEvent evt = SDFSEvent.consistancyCheckEvent(
					"Running Consistancy Check on DSE, this may take a while",
					Main.mountEvent);
			CommandLineProgressBar bar = new CommandLineProgressBar(
					"Scanning DSE", map.getSize(), System.out);
			evt.maxCt = map.getSize();
			ArrayList<HashFetcher> al = new ArrayList<HashFetcher>();
			for (int i = 0; i < Main.writeThreads; i++) {
				HashFetcher hf = new HashFetcher();
				hf.map = map;
				hf.store = store;
				Thread th = new Thread(hf);
				th.start();
				hf.running = true;
				al.add(hf);
			}
			while (al.size() > 0) {
				ArrayList<HashFetcher> _al = new ArrayList<HashFetcher>();
				for (HashFetcher hf : al) {
					if (hf.running)
						_al.add(hf);
				}
				al = _al;
				long val = count.get();
				if (val > 100000) {
					count.set(0);
					bar.update(currentCount.get());
				}
				Thread.sleep(1000);

			}
			bar.finish();
			System.out.println("Finished");
			if (corruption.get() > 0) {
				SDFSLogger.getLog().warn(
						"Corruption found for [" + corruption + "] blocks");
				System.out.println("Corruption found for [" + corruption
						+ "] blocks");
			}

			System.out.println("Succesfully Ran Consistance Check for ["
					+ records + "] records, recovered [" + recordsRecovered
					+ "]");

			SDFSLogger
					.getLog()
					.warn("Succesfully Ran Consistance Check for [" + records
							+ "] records, recovered [" + recordsRecovered + "]");
			evt.endEvent("Succesfully Ran Consistance Check for [" + records
					+ "] records, recovered [" + recordsRecovered + "]");
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Unable to recover records because " + e.toString(), e);
		}
	}

	private static class HashFetcher implements Runnable {
		AbstractHashesMap map = null;
		AbstractChunkStore store = null;
		boolean running = false;

		@Override
		public void run() {
			running = true;
			try {
				ChunkData data = store.getNextChunck();
				if (data == null)
					return;

				data.recoverd = true;
				while (data != null) {
					count.incrementAndGet();
					records.incrementAndGet();
					long pos = map.get(data.getHash());
					if (pos == -1) {
						if (map.put(data).getInserted())
							recordsRecovered.incrementAndGet();
					}

					try {
						synchronized (store) {
							data = store.getNextChunck();
						}
						if (data != null) {
							data.recoverd = true;
							currentCount.incrementAndGet();
						}
					} catch (Exception e) {
						corruption.incrementAndGet();
						/*
						SDFSLogger.getLog().warn(
								"Data Corruption found in datastore", e);
								*/
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"Unable to recover records because " + e.toString(), e);
			} finally {
				running = false;
			}

		}

	}

}
