package org.opendedup.sdfs.filestore;

import java.io.ByteArrayInputStream;

import java.io.DataInputStream;
import java.io.IOException;

import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.security.GSCredentials;
import org.opendedup.sdfs.Main;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.EncryptUtils;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Element;

/**
 * 
 * @author Sam Silverberg The S3 chunk store implements the AbstractChunkStore
 *         and is used to store deduped chunks to AWS S3 data storage. It is
 *         used if the aws tag is used within the chunk store config file. It is
 *         important to make the chunk size very large on the client when using
 *         this chunk store since S3 charges per http request.
 * 
 */
public class GoogleChunkStore implements AbstractChunkStore {
	private String name;
	private static GoogleStorageService gsService;
	private boolean closed = false;

	// private static ReentrantLock lock = new ReentrantLock();

	static {
		try {
			GSCredentials gsCredentials = new GSCredentials(Main.awsAccessKey,
					Main.awsSecretKey);

			// To communicate with Google Storage use the GoogleStorageService.
			gsService = new GoogleStorageService(gsCredentials);
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to authenticate to AWS", e);
			System.exit(-1);
		}
	}

	public GoogleChunkStore() throws IOException {
	}

	public GoogleChunkStore(String name) throws IOException {
		this.name = name;
		this.init(null);
	}

	public long bytesRead() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long bytesWritten() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void close() {

	}

	public void expandFile(long length) throws IOException {
		// TODO Auto-generated method stub

	}

	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		String hashString = this.getHashName(hash);
		try {
			GSObject obj = gsService.getObject(this.name, hashString);
			byte[] data = new byte[(int) obj.getContentLength()];
			DataInputStream in = new DataInputStream(obj.getDataInputStream());
			in.readFully(data);
			obj.closeDataInputStream();
			if (Main.chunkStoreEncryptionEnabled)
				data = EncryptUtils.decrypt(data);
			if (Main.awsCompress)
				data = CompressionUtils.decompressZLIB(data);
			return data;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			SDFSLogger.getLog()
					.error("unable to fetch block [" + hash + "]", e);
			throw new IOException("unable to read " + hashString);
		}

	}

	public String getName() {
		return this.name;
	}

	public long reserveWritePosition(int len) throws IOException {
		if (this.closed)
			throw new IOException("ChunkStore is closed");

		return 0;
	}

	public void setName(String name) {
	}

	public long size() {
		return 0;
	}

	public void writeChunk(byte[] hash, byte[] chunk, int len, long start)
			throws IOException {

		String hashString = this.getHashName(hash);
		GSObject gsObject = new GSObject(hashString);
		if (Main.awsCompress) {
			chunk = CompressionUtils.compressZLIB(chunk);
			gsObject.addMetadata("compress", "true");
		} else {
			gsObject.addMetadata("compress", "false");
		}
		if (Main.chunkStoreEncryptionEnabled) {
			chunk = EncryptUtils.encrypt(chunk);
			gsObject.addMetadata("encrypt", "true");
		} else {
			gsObject.addMetadata("encrypt", "false");
		}
		ByteArrayInputStream s3IS = new ByteArrayInputStream(chunk);
		gsObject.setDataInputStream(s3IS);
		gsObject.setContentType("binary/octet-stream");
		gsObject.setContentLength(s3IS.available());
		try {
			gsService.putObject(this.name, gsObject);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			SDFSLogger.getLog().fatal(
					"unable to upload " + hashString + " to " + this.name, e);
			throw new IOException(e);
		} finally {
			s3IS.close();
		}
	}

	public void writeBlankChunk(String hash, byte[] chunk) throws IOException {

		String hashString = hash;
		GSObject gsObject = new GSObject(hashString);
		if (Main.awsCompress) {
			chunk = CompressionUtils.compressZLIB(chunk);
			gsObject.addMetadata("compress", "true");
		} else {
			gsObject.addMetadata("compress", "false");
		}
		if (Main.chunkStoreEncryptionEnabled) {
			chunk = EncryptUtils.encrypt(chunk);
			gsObject.addMetadata("encrypt", "true");
		} else {
			gsObject.addMetadata("encrypt", "false");
		}
		ByteArrayInputStream s3IS = new ByteArrayInputStream(chunk);
		gsObject.setDataInputStream(s3IS);
		gsObject.setContentType("binary/octet-stream");
		gsObject.setContentLength(s3IS.available());
		try {
			gsService.putObject(this.getName(), gsObject);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			SDFSLogger.getLog().fatal("unable to upload " + hashString, e);
			throw new IOException(e);
		} finally {
			s3IS.close();
		}
	}

	public static void main(String[] args) throws IOException {

	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		String hashString = this.getHashName(hash);
		try {
			gsService.deleteObject(this.getName(), hashString);
		} catch (ServiceException e) {
			SDFSLogger.getLog()
					.warn("Unable to delete object " + hashString, e);
		}
	}

	public static void deleteBucket(String bucketName, String awsAccessKey,
			String awsSecretKey) {
		try {
			System.out.println("");
			System.out.print("Deleting Bucket [" + bucketName + "]");
			GSCredentials credentials = new GSCredentials(Main.awsAccessKey,
					Main.awsSecretKey);

			// To communicate with Google Storage use the GoogleStorageService.
			GoogleStorageService service = new GoogleStorageService(credentials);
			service.deleteBucket(bucketName);
			SDFSLogger.getLog().info("Bucket [" + bucketName + "] deleted");
			System.out.println("Bucket [" + bucketName + "] deleted");
		} catch (ServiceException e) {
			SDFSLogger.getLog()
					.warn("Unable to delete bucket " + bucketName, e);
		}
	}

	private String getHashName(byte[] hash) throws IOException {
		if (Main.chunkStoreEncryptionEnabled) {
			byte[] encH = EncryptUtils.encrypt(hash);
			return StringUtils.getHexString(encH);
		} else {
			return StringUtils.getHexString(hash);
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

	@Override
	public boolean moveChunk(byte[] hash, long origLoc, long newLoc)
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void init(Element config) throws IOException {
		try {
			this.name = Main.awsBucket;
			gsService.getOrCreateBucket(Main.awsBucket);
		} catch (ServiceException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void setSize(long size) {
		// TODO Auto-generated method stub

	}

	public void clearStore() throws IOException {
		try {
			SDFSLogger.getLog().warn(
					"Deleting all entries from Bucket [" + this.name + "]");
			GSObject[] obj = gsService.listObjects(this.name);
			SDFSLogger.getLog().info("Will delete " + obj.length + " objects");
			for (int i = 0; i < obj.length; i++) {
				gsService.deleteObject(this.name, obj[i].getKey());
			}
			SDFSLogger.getLog().info(
					"All entries in bucket [" + this.getName() + "] deleted");
		} catch (ServiceException e) {
			SDFSLogger.getLog().warn(
					"Unable to delete entries in " + this.getName(), e);
			throw new IOException(e);
		}
	}

	@Override
	public ChunkData getNextChunck() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void iterationInit() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void compact() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
