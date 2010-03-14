package org.opendedup.sdfs.filestore;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.opendedup.util.HashFunctions;
import org.opendedup.util.StringUtils;


/**
 * 
 * @author Sam Silverberg
 * The S3 chunk store implements the AbstractChunkStore and is used to store deduped chunks to AWS S3 data storage.
 * It is used if the aws tag is used within the chunk store config file. It is important to make the chunk size very large
 * on the client when using this chunk store since S3 charges per http request.
 *
 */
public class S3ChunkStore implements AbstractChunkStore {
	private static String awsAccessKey = null;
	private static String awsSecretKey = null;
	private static AWSCredentials awsCredentials = 
        new AWSCredentials(awsAccessKey, awsSecretKey);
	private String name;
	private S3Bucket s3Bucket = null;
	S3Service s3Service;
	private transient static Logger log = Logger.getLogger("sdfs");
	//private static ReentrantLock lock = new ReentrantLock();
	
	
	public S3ChunkStore(String name) throws IOException {
		this.name = name;
		try {
			s3Service = new RestS3Service(awsCredentials);
			this.s3Bucket = s3Service.getBucket(awsAccessKey);
			if(this.s3Bucket == null) {
				this.s3Bucket = s3Service.createBucket(awsAccessKey);
				log.info("created new store " + awsAccessKey);
			}
		} catch (S3ServiceException e) {
			e.printStackTrace();
			throw new IOException(e.toString());
		}
	}

	
	public long bytesRead() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public long bytesWritten() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public void closeStore() {
		
	}

	
	public void expandFile(long length) throws IOException {
		// TODO Auto-generated method stub
		
	}

	
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		String hashString = StringUtils.getHexString(hash);
		try {
			S3Object obj = s3Service.getObject(s3Bucket, hashString);
			byte [] data = new byte[(int)obj.getContentLength()];
			DataInputStream in = new DataInputStream(obj.getDataInputStream());
			in.readFully(data);
			obj.closeDataInputStream();
			return data;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException("unable to read " + hashString);
		}

	}

	
	public String getName() {
		return this.name;
	}

	
	public long reserveWritePosition(int len) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}


	public void setName(String name) {
		
	}


	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void writeChunk(byte[] hash, byte[] chunk, int len, long start)
			throws IOException {
		
		String hashString = StringUtils.getHexString(hash);
		S3Object s3Object = new S3Object(hashString);
		ByteArrayInputStream s3IS = new ByteArrayInputStream(chunk);
		s3Object.setDataInputStream(s3IS);
		s3Object.setContentType("binary/octet-stream");
		s3Object.setContentLength(s3IS.available());
		try {
			s3Service.putObject(s3Bucket, s3Object);
		} catch (Exception e) {
			// TODO Auto-generated catch block

			log.log(Level.SEVERE, "unable to upload " + hashString,e);
			throw new IOException(e.toString());
		} finally {
			s3IS.close();
		}
		
	}
	
	public static void main(String [] args) throws IOException {
		S3ChunkStore store = new S3ChunkStore("test");
		String test = "This is a test";
		byte [] md5 = HashFunctions.getMD5ByteHash(test.getBytes());
		store.writeChunk(md5, test.getBytes(), 0, 0);
		String outStr = new String(store.getChunk(md5, 0, 0));
		System.out.println(outStr);
	}


	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		String hashString = StringUtils.getHexString(hash);
		try {
			s3Service.deleteObject(s3Bucket, hashString);
		} catch (S3ServiceException e) {
			log.log(Level.WARNING, "Unable to delete object " + hashString, e);
		}
		
	}


	@Override
	public void addChunkStoreListener(AbstractChunkStoreListener listener) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void claimChunk(byte[] hash, long start) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
