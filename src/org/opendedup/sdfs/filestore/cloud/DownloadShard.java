package org.opendedup.sdfs.filestore.cloud;

import java.io.DataInputStream;

import org.opendedup.collections.DataArchivedException;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class DownloadShard implements Runnable {

	public byte[] b;
	AbstractDownloadListener l;
	int pos;
	int len;
	GetObjectRequest req = null;
	AmazonS3Client s3Service = null;
	S3Object obj = null;
	DataInputStream in = null;

	public DownloadShard(AbstractDownloadListener l, String name, int pos,
			int len, AmazonS3Client s3Service, String bucket) {
		b = new byte[len];
		this.l = l;
		req = new GetObjectRequest(bucket, name);
		if (len != -1)
			req.setRange(pos, pos + len);
		this.s3Service = s3Service;
	}

	@Override
	public void run() {
		try {
			obj = s3Service.getObject(req);
			in = new DataInputStream(obj.getObjectContent());
			in.readFully(b);
			l.commandResponse(this);

		} catch (AmazonS3Exception e) {
			if (e.getErrorCode().equalsIgnoreCase("InvalidObjectState"))
				l.setDAR(new DataArchivedException());
			l.commandException(e);
		} catch (Exception e) {
			// SDFSLogger.getLog().debug("unable to read " + obj.getKey() +
			// " at pos=" + pos + " byte len=" + b.length + " file size="
			// +obj.getObjectMetadata().getContentLength(),e);
			l.commandException(e);

		} finally {
			try {
				obj.close();
			} catch (Exception e) {

			}
			try {
				in.close();
			} catch (Exception e) {
			}
		}
	}

	public void close() {
		try {
			obj.close();
		} catch (Exception e) {

		}
		try {
			in.close();
		} catch (Exception e) {
		}
	}

}
