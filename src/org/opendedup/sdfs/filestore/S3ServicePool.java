package org.opendedup.sdfs.filestore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.opendedup.logging.SDFSLogger;

public class S3ServicePool {

	private int poolSize;
	private LinkedBlockingQueue<RestS3Service> passiveObjects = null;
	private ArrayList<RestS3Service> activeObjects = new ArrayList<RestS3Service>();
	private ReentrantLock alock = new ReentrantLock();
	private AWSCredentials awsCredentials;

	public S3ServicePool(AWSCredentials awsCredentials, int size)
			throws IOException {
		this.awsCredentials = awsCredentials;
		this.poolSize = size;
		passiveObjects = new LinkedBlockingQueue<RestS3Service>(this.poolSize);
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

	public RestS3Service borrowObject() throws IOException,
			InterruptedException {
		RestS3Service hc = this.passiveObjects.take();
		this.alock.lock();
		this.activeObjects.add(hc);
		this.alock.unlock();
		return hc;
	}

	public void returnObject(RestS3Service hc) throws IOException {
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
			else
				hc.shutdown();
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to get object out of pool ", e);
			throw new IOException(e.toString());

		} finally {
		}
	}

	public RestS3Service makeObject() throws S3ServiceException {
		Jets3tProperties jProps = Jets3tProperties
				.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);
		jProps.setProperty("httpclient.max-connections",
				Integer.toString(this.poolSize+5));

		RestS3Service s3Service = new RestS3Service(awsCredentials, null, null,
				jProps);
		return s3Service;
	}

	public void destroyObject(RestS3Service hc) throws ServiceException {
		hc.shutdown();
	}

	public void close() throws ServiceException, IOException,
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
			for (RestS3Service s : this.passiveObjects) {
				s.shutdown();
			}
			this.passiveObjects.clear();
		} finally {
			alock.unlock();
		}
	}

}
