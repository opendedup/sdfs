package org.opendedup.buse.sdfsdev;

import java.io.File;



import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.buse.driver.*;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.BlockDev;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;

import com.google.common.eventbus.EventBus;

public class SDFSBlockDev implements BUSE, Runnable {

	ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	long sz;
	String devicePath;
	String internalFPath;
	public DedupFileChannel ch;
	EventBus eventBus = new EventBus();
	BlockDev dev;
	private boolean closed = true;
	public SDFSBlockDev(BlockDev dev) throws IOException {
		this.dev = dev;
		eventBus.register(dev);
		this.devicePath = dev.getDevPath();
		this.sz = dev.getSize();
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
		MetaDataDedupFile mf = dev.getMF();
		this.ch = mf.getDedupFile().getChannel(0);
	}
	
	

	@Override
	public int read(ByteBuffer data, int len, long offset) {
		/*
		if(len >= Main.CHUNK_LENGTH)
		SDFSLogger.getLog().info("read request len=" + len + " offset="
				+ offset + " databuflen=" + data.capacity()
				+ " databufpos=" + data.position());
		*/
		try {
			ch.read(data, 0, len, offset);
		} catch (Throwable e) {
			SDFSLogger.getLog().error("unable to read file " + this.devicePath, e);
			return Errno.ENODATA;
		}
		return 0;
	}

	@Override
	public int write(ByteBuffer buff, int len, long offset)  {
		/*
		if(len >= Main.CHUNK_LENGTH)
		SDFSLogger.getLog().info("write request len=" + len + " offset="
				+ offset + " databuflen=" + buff.capacity()
				+ " databufpos=" + buff.position());
		*/
		try {
			if (Main.volume.isFull()) {
				SDFSLogger.getLog().error("Volume is full");;
				return Errno.ENOSPC;
				
			}
			try {
				ch.writeFile(buff, len, 0, offset,true);
					
			} catch (Throwable e) {
				SDFSLogger.getLog().error("unable to write to block device" + this.devicePath, e);
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
			} catch (Throwable e) {
				SDFSLogger.getLog().error("unable to close " + this.devicePath, e);
			}
		}
	}

	@Override
	public int flush() {
		/*
		SDFSLogger.getLog().info("flush request");
		*/
		try {
			ch.force(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to sync file [" + this.devicePath + "]", e);
			return Errno.EACCES;
		}
		return 0;
	}

	@Override
	public int trim(long from, int len) {
		/*
		SDFSLogger.getLog().debug("trim request from=" + from + " len=" + len);
		*/
		try {
			ch.trim(from, len);
		} catch (IOException e) {
			SDFSLogger.getLog().error("unable to trim file [" + this.devicePath + "]", e);
			return Errno.EACCES;
		}
		return 0;
	}

	private void startBlockDev() throws Exception { 
		BUSEMkDev.startdev(this.devicePath, this.sz, 4096, this, false);
		
	}

	@Override
	public void close() {
		
		try {
			Process p = Runtime.getRuntime().exec("umount " + this.devicePath);
			p.waitFor();
		}catch(Exception e) {
			SDFSLogger.getLog().error("unable to unmount vols for " + this.devicePath, e);
		}
			eventBus.post(new BlockDeviceBeforeClosedEvent(this.dev));
		try {
			BUSEMkDev.closeDev(devicePath);
			for(int i = 0;i<300;i++) {
				if(this.closed)
					return;
				else
					Thread.sleep(100);
			}
			SDFSLogger.getLog().warn("timed out waiting for close");
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to close " + this.devicePath, e);
		}
	}

	@Override
	public void run() {
		try {
			this.closed = false;
			this.eventBus.post(new BlockDeviceOpenEvent(this.dev));
			this.startBlockDev();
		}catch(Exception e) {
			SDFSLogger.getLog().warn("Block Device Stopping " + this.devicePath,e);
		}
		finally {
			this.closed = true;
			this.eventBus.post(new BlockDeviceClosedEvent(this.dev));
			SDFSLogger.getLog().warn("Block Device Stopped " + this.devicePath);
		}
	}

}
