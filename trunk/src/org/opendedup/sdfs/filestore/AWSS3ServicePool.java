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
import org.opendedup.logging.SDFSLogger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

public class AWSS3ServicePool {

	private int poolSize;
	private LinkedBlockingQueue<AmazonS3Client> passiveObjects = null;
	private ArrayList<AmazonS3Client> activeObjects = new ArrayList<AmazonS3Client>();
	private ReentrantLock alock = new ReentrantLock();
	private BasicAWSCredentials awsCredentials;

	public AWSS3ServicePool(BasicAWSCredentials awsCredentials, int size)
			throws IOException {
		this.awsCredentials = awsCredentials;
		this.poolSize = size;
		passiveObjects = new LinkedBlockingQueue<AmazonS3Client>(this.poolSize);
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

	public AmazonS3Client borrowObject() throws IOException,
			InterruptedException {
		AmazonS3Client hc = this.passiveObjects.take();
		this.alock.lock();
		this.activeObjects.add(hc);
		this.alock.unlock();
		return hc;
	}

	public void returnObject(AmazonS3Client hc) throws IOException {
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

	public AmazonS3Client makeObject() throws S3ServiceException {
		Jets3tProperties jProps = Jets3tProperties
				.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);
		jProps.setProperty("httpclient.max-connections",
				Integer.toString(this.poolSize));

		AmazonS3Client s3Service = new AmazonS3Client(awsCredentials);
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
			for (AmazonS3Client s : this.passiveObjects) {
				s.shutdown();
			}
			this.passiveObjects.clear();
		} finally {
			alock.unlock();
		}
	}

}
