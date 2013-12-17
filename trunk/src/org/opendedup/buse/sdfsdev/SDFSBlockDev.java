package org.opendedup.buse.sdfsdev;

import java.io.File;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.buse.driver.*;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;

public class SDFSBlockDev implements BUSE {

	ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	long sz;
	String devicePath;
	String internalFPath;
	DedupFileChannel ch;
	
	public SDFSBlockDev(String blockDev,String devicePath, String internalPath, long sz) throws IOException {
		this.devicePath = devicePath;
		this.sz = sz;
		File f = new File(devicePath);
		if(!f.exists()) {
			Process p = Runtime.getRuntime().exec("modprobe nbd");
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				SDFSLogger.getLog().debug("unable to wait for modprobe",e);
			}
		}
		if(!f.exists())
			throw new IOException("device " + devicePath + " not found.");
		File df = new File(internalPath + "/" + blockDev);
		this.internalFPath = df.getPath();
		if(!df.exists())
			df.getParentFile().mkdirs();
		MetaDataDedupFile mf = MetaFileStore.getMF(df);
		mf.setDev(true);
		this.ch = mf.getDedupFile().getChannel(0);
	}

	@Override
	public int read(ByteBuffer data, int len, long offset) {
		/*
		SDFSLogger.getLog().debug("read request len=" + len + " offset="
				+ (int) offset + " databuflen=" + data.capacity()
				+ " databufpos=" + data.position());
				*/
		try {
			ch.read(data, 0, len, offset);
		} catch (IOException e) {
			SDFSLogger.getLog().error("unable to read file " + this.internalFPath, e);
			return Errno.ENODATA;
		}
		return 0;
	}

	@Override
	public int write(ByteBuffer buff, int len, long offset) {
		/*
		SDFSLogger.getLog().debug("write request len=" + len + " offset="
				+ (int) offset + " databuflen=" + buff.capacity()
				+ " databufpos=" + buff.position());
		*/
		try {
			if (Main.volume.isFull())
				return Errno.ENOSPC;
			try {
				ch.writeFile(buff, len, 0, offset);
			} catch (IOException e) {
				SDFSLogger.getLog().error("unable to write to file" + this.internalFPath, e);
				return Errno.EACCES;
			}
		} finally {
		}
		return 0;
	}

	@Override
	public void disconnect() {
		if(ch != null) {
			try {
				SDFSLogger.getLog().warn("disconnect called for " + this.devicePath);
				ch.getDedupFile().unRegisterChannel(ch, 0);
				ch.getDedupFile().forceClose();
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to close " + this.internalFPath, e);
			}
		}
	}

	@Override
	public int flush() {
		/*
		SDFSLogger.getLog().debug("flush request");
		*/
		try {
			ch.force(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to sync file [" + this.internalFPath + "]", e);
			return Errno.EACCES;
		}
		return 0;
	}

	@Override
	public int trim(long from, int len) {
		/*
		SDFSLogger.getLog().debug("trim request from=" + from + " len=" + len);
		*/
		return 0;
	}

	public void startBlockDev() throws Exception { 
		BUSEMkDev.mkdev(this.devicePath, this.sz, 4096, this, false);
	}

	@Override
	public void close() {
		try {
			BUSEMkDev.closeDev(devicePath);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to close " + this.devicePath, e);
		}
	}

}
