package org.opendedup.sdfs.filestore;

import java.io.IOException;


import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;


import org.opendedup.logging.SDFSLogger;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
public class MAzureServicePool {

	private int poolSize;
	private LinkedBlockingQueue<CloudBlobContainer> passiveObjects = null;
	private ArrayList<CloudBlobContainer> activeObjects = new ArrayList<CloudBlobContainer>();
	private ReentrantLock alock = new ReentrantLock();
	private CloudStorageAccount account;
	private String bucket;

	public MAzureServicePool(CloudStorageAccount account, int size,String bucket)
			throws IOException {
		this.bucket = bucket;
		this.account = account;
		this.poolSize = size;
		passiveObjects = new LinkedBlockingQueue<CloudBlobContainer>(this.poolSize);
		this.populatePool();
	}

	public void populatePool() throws IOException {
		for (int i = 0; i < poolSize; i++) {
			try {
				this.passiveObjects.add(this.makeObject());
			} catch (Exception e) {
				SDFSLogger.getLog().error("Unable to get object out of pool ",
						e);
				throw new IOException(e.toString());

			} finally {
			}
		}
	}

	public CloudBlobContainer borrowObject() throws IOException,
			InterruptedException {
		CloudBlobContainer hc = this.passiveObjects.take();
		this.alock.lock();
		this.activeObjects.add(hc);
		this.alock.unlock();
		return hc;
	}

	public void returnObject(CloudBlobContainer hc) throws IOException {
		alock.lock();
		try {

			this.activeObjects.remove(hc);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to get object out of pool ", e);
			throw new IOException(e.toString());

		} finally {
			alock.unlock();
		}
		try {
			if (this.passiveObjects.size() <= this.poolSize)
				this.passiveObjects.put(hc);
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to get object out of pool ", e);
			throw new IOException(e.toString());

		} finally {
		}
	}

	public CloudBlobContainer makeObject() throws URISyntaxException, StorageException {
		SDFSLogger.getLog().info("pool size is " +this.passiveObjects.size());
		CloudBlobClient serviceClient = account.createCloudBlobClient();
		CloudBlobContainer container = serviceClient.getContainerReference(this.bucket);
		container.createIfNotExists();
		return container;
	}

	public void destroyObject(CloudBlobContainer hc)  {
	}

	public void close() throws IOException,
			InterruptedException {
		int z = 0;
		while (this.activeObjects.size() > 0) {
			Thread.sleep(1);
			z++;
			if (z > 30000)
				throw new IOException(
						"Unable to close s3 pool because close command timed out after 30 seconds");
		}
		alock.lock();
		try {
			
			this.passiveObjects.clear();
		} finally {
			alock.unlock();
		}
	}

}
