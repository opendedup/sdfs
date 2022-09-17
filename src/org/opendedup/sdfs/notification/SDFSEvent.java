/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.notification;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.ParserConfigurationException;

import com.google.common.eventbus.EventBus;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.mgmt.GetEvent;
import org.opendedup.util.FileCounts;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.XMLUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsFIFO;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.OptionString;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class SDFSEvent implements java.io.Serializable {
	/**
	 *
	 */
	private static final long serialVersionUID = -7418485011806466368L;
	public Type type = null;
	public Level level;
	public String shortMsg = null;
	public String longMsg = "";
	public String target = null;
	private AtomicLong maxCt = new AtomicLong(1);
	private AtomicLong curCt = new AtomicLong(1);
	public long startTime;
	public long endTime = -1;
	public long actionCount = 0;
	public boolean success = true;
	public String uid = null;
	public String extendedInfo = "";
	private ArrayList<SDFSEvent> children = new ArrayList<SDFSEvent>();
	public String puid;
	private static RocksDB evtdb = null;
	private transient EventBus eventBus = new EventBus();
	public transient static final Type GC = new Type("Garbage Collection");
	public transient static final Type FLUSHALL = new Type("Flush All Buffers");
	public transient static final Type FDISK = new Type("File Check");
	public transient static final Type CRCK = new Type("Cluster Redundancy Check");
	public transient static final Type WAIT = new Type("Waiting to Run Again");
	public transient static final Type CLAIMR = new Type("Claim Records");
	public transient static final Type REMOVER = new Type("Remove Records");
	public transient static final Type AIMPORT = new Type("Replication Meta-Data File Import");
	public transient static final Type IMPORT = new Type("Replication Import");
	public transient static final Type AOUT = new Type("Replication Archive Out");
	public transient static final Type MOUNT = new Type("Mount Volume");
	public transient static final Type COMPACT = new Type("Compaction");
	public transient static final Type UMOUNT = new Type("Unmount Volume");
	public transient static final Type LHASHDB = new Type("Loading Hash Database Task");
	public transient static final Type FSCK = new Type("Consistancy Check");
	public transient static final Type MIMPORT = new Type("Replication Block Data" + " Import");
	public transient static final Type FIXDSE = new Type("Volume Recovery Task");
	public transient static final Type SNAP = new Type("Take Snapshot");
	public transient static final Type EXPANDVOL = new Type("Expand Volume");
	public transient static final Type DELFILE = new Type("Delete File");
	public transient static final Type PERFMON = new Type("Performance Monitor");
	public transient static final Type TEST = new Type("Testing 123");
	public transient static final Type CONVMAP = new Type("CONVMAP");
	public transient static final Type PSWD = new Type("Password Changed");
	public transient static final Type DSKFL = new Type("Disk Full");
	public transient static final Type RDER = new Type("Read Error");
	public transient static final Type CSZ = new Type("Set Cache Size");
	public transient static final Type RSP = new Type("Set Read Speed");
	public transient static final Type WSP = new Type("Set Write Speed");
	public transient static final Type RAE = new Type("Cache File");
	public transient static final Type CF = new Type("Importing Cloud File");
	public transient static final Type ARCHIVERESTORE = new Type("Restore from Glacier");
	public transient static final Type WER = new Type("Write Error");
	public transient static final Type DISCO = new Type("Storage Pool Disconnected");
	public transient static final Type RECO = new Type("Storage Pool Reconnected");
	public transient static final Type DELCV = new Type("Delete Cloud Volume");
	public transient static final Type SYNCVOL = new Type("Sync From Connected Volume");
	public transient static final Level RUNNING = new Level("running");
	public transient static final Level INFO = new Level("info");
	public transient static final Level WARN = new Level("warning");
	public transient static final Level ERROR = new Level("error");
	private static final long MB = 1024 * 1024;

	public static void init() throws RocksDBException {
		File directory = new File(Main.volume.getEvtPath() + File.separator);
		directory.mkdirs();
		RocksDB.loadLibrary();
		CompactionOptionsFIFO fifo = new CompactionOptionsFIFO();
		fifo.setMaxTableFilesSize(500 * MB);
		DBOptions options = new DBOptions();
		options.setCreateIfMissing(true);

		// options.setMinWriteBufferNumberToMerge(2);
		// options.setMaxWriteBufferNumber(6);
		// options.setLevelZeroFileNumCompactionTrigger(2);
		Env env = Env.getDefault();
		options.setEnv(env);

		ColumnFamilyOptions familyOptions = new ColumnFamilyOptions();
		familyOptions.setCompactionOptionsFIFO(fifo);
		ColumnFamilyDescriptor evtArD = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, familyOptions);
		ArrayList<ColumnFamilyDescriptor> descriptors = new ArrayList<ColumnFamilyDescriptor>();
		descriptors.add(evtArD);
		ArrayList<ColumnFamilyHandle> handles = new ArrayList<ColumnFamilyHandle>();
		evtdb = RocksDB.open(options, directory.getPath(), descriptors, handles);
	}

	SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");

	protected SDFSEvent(Type type, String target, String shortMsg, Level level) {
		this.type = type;
		this.target = target;
		this.startTime = System.currentTimeMillis();
		this.shortMsg = shortMsg;
		this.uid = RandomGUID.getGuid();

		this.level = level;
		try {
			synchronized (evtdb) {
				evtdb.put(this.uid.getBytes(), this.toProtoBuf().toByteArray());
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to add message", e);
		}
	}

	public static SDFSEvent GetEvent(org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent evt)  {
		if (evt.getType().equals(MIMPORT.toString())) {
			return new ReplicationImportEvent(evt);
		} else {
			return new SDFSEvent(evt);
		}
	}

	protected SDFSEvent(org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent evt)  {
		this.type = new Type(evt.getType());
		this.target = evt.getTarget();
		this.startTime = evt.getStartTime();
		this.shortMsg = evt.getShortMsg();
		this.uid = evt.getUuid();
		this.maxCt.set(evt.getMaxCount());
		this.curCt.set(evt.getCurrentCount());
		this.puid = evt.getParentUuid();
		this.success = evt.getSuccess();
		this.level = new Level(evt.getLevel());
		try {
			synchronized (evtdb) {
				evtdb.put(this.uid.getBytes(), this.toProtoBuf().toByteArray());
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to add message", e);
		}
	}

	public void registerListener(Object obj) {
		eventBus.register(obj);
	}

	public void unregisterListener(Object obj) {
		try {
			eventBus.unregister(obj);
		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to unregister listener", e);
		}

	}

	public void addCount(long ct) {
		this.curCt.addAndGet(ct);
	}

	public long getCount() {
		return this.curCt.get();
	}

	public long getMaxCount() {
		return this.maxCt.get();
	}

	public void setMaxCount(long ct) {
		this.maxCt.set(ct);
	}

	public void setCurrentCount(long ct) {
		this.curCt.set(ct);
	}

	public void endEvent(String msg, Level level) {
		synchronized (this) {
			this.shortMsg = msg;
			this.level = level;
			if (this.maxCt.get() == 0)
				this.maxCt.incrementAndGet();
			this.curCt = this.maxCt;
			this.endTime = System.currentTimeMillis();
			if (level == INFO) {
				this.success = true;
			}
			eventBus.post(this);
		}
	}

	public void addChild(SDFSEvent evt) throws IOException {
		if (evt.uid.equalsIgnoreCase(this.uid))
			throw new IOException("Cannot add child with same event id");
		evt.puid = this.uid;
		this.children.add(evt);
		eventBus.post(this);
	}

	public ArrayList<SDFSEvent> getChildren() {
		return this.children;
	}

	public boolean isDone() {
		return this.endTime > 0;
	}

	public void endEvent(String msg, Level level, Throwable e) {
		synchronized (this) {
			for (int i = 0; i < this.children.size(); i++) {
				if (this.children.get(i).level.type.equalsIgnoreCase(RUNNING.type))
					this.children.get(i).endEvent(msg, level, e);
			}
			this.shortMsg = msg + " Exception : " + e.toString();
			this.level = level;
			this.endTime = System.currentTimeMillis();
			if (this.maxCt.get() == 0)
				this.maxCt.incrementAndGet();
			this.curCt = this.maxCt;
			this.success = false;
			eventBus.post(this);
		}
	}

	public void endEvent(String msg) {
		synchronized (this) {
			for (int i = 0; i < this.children.size(); i++) {
				if (this.children.get(i).level.type.equalsIgnoreCase(RUNNING.type))
					this.children.get(i).endEvent(msg);
			}
			this.shortMsg = msg;
			this.endTime = System.currentTimeMillis();
			this.level = SDFSEvent.INFO;
			this.curCt = this.maxCt;

			eventBus.post(this);
		}
	}

	public void endEvent() {
		synchronized (this) {
			for (int i = 0; i < this.children.size(); i++) {
				if (this.children.get(i).level.type.equalsIgnoreCase(RUNNING.type))
					this.children.get(i).endEvent();
			}
			this.endTime = System.currentTimeMillis();
			this.level = SDFSEvent.INFO;
			if (this.maxCt.get() == 0)
				this.maxCt.incrementAndGet();
			this.curCt = this.maxCt;
			eventBus.post(this);
		}
	}

	public void endErrorEvent() {
		synchronized (this) {
			for (int i = 0; i < this.children.size(); i++) {
				if (this.children.get(i).level.type.equalsIgnoreCase(RUNNING.type))
					this.children.get(i).endEvent();
			}
			this.endTime = System.currentTimeMillis();
			this.level = SDFSEvent.ERROR;
			if (this.maxCt.get() == 0)
				this.maxCt.incrementAndGet();
			this.curCt = this.maxCt;
			eventBus.post(this);
		}
	}

	public void endWarnEvent() {
		synchronized (this) {
			for (int i = 0; i < this.children.size(); i++) {
				if (this.children.get(i).level.type.equalsIgnoreCase(RUNNING.type))
					this.children.get(i).endEvent();
			}
			this.endTime = System.currentTimeMillis();
			this.level = SDFSEvent.WARN;
			if (this.maxCt.get() == 0)
				this.maxCt.incrementAndGet();
			this.curCt = this.maxCt;
			eventBus.post(this);
		}
	}

	public static SDFSEvent archiveImportEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(AIMPORT, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent readAheadEvent(MetaDataDedupFile f) {
		SDFSEvent event = new SDFSEvent(RAE, getTarget(), "Caching " + f.getPath() + " Locally", RUNNING);

		return event;
	}

	public static SDFSEvent archiveRestoreEvent(MetaDataDedupFile f) {
		SDFSEvent event = new SDFSEvent(ARCHIVERESTORE, getTarget(), "Restoring " + f.getPath(), RUNNING);

		return event;
	}

	public static SDFSEvent cszEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(CSZ, getTarget(), shortMsg, RUNNING);

		return event;
	}

	public static SDFSEvent rspEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(RSP, getTarget(), shortMsg, RUNNING);

		return event;
	}

	public static void discoEvent() {
		SDFSEvent event = new SDFSEvent(DISCO, getTarget(), "Storage Pool Disconnected", RUNNING);
		event.maxCt.set(1);
		event.endWarnEvent();
	}

	public static void recoEvent() {
		SDFSEvent event = new SDFSEvent(RECO, getTarget(), "Storage Pool Reconnected", RUNNING);
		event.maxCt.set(1);
		event.endEvent();
	}

	public static SDFSEvent wspEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(WSP, getTarget(), shortMsg, RUNNING);

		return event;
	}

	public static SDFSEvent cfEvent(String fileName) {
		SDFSEvent event = new SDFSEvent(CF, getTarget(), "Importing [" + fileName + "]", RUNNING);
		return event;
	}

	public static SDFSEvent importEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(IMPORT, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent syncVolEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(SYNCVOL, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent deleteCloudVolumeEvent(long volumeid) {
		SDFSEvent event = new DeleteCloudVolumeEvent(getTarget(), "Deleting Cloud Volume " + volumeid, RUNNING,
				volumeid);
		return event;
	}

	public static SDFSEvent passwdEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(PSWD, "Administrative User", shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent convMapEvent(String shortMsg, String file) {
		SDFSEvent event = new SDFSEvent(CONVMAP, file, shortMsg, RUNNING);
		return event;
	}

	public static void rdErrEvent() {
		SDFSEvent event = new SDFSEvent(RDER, getTarget(), "Read Error Detected", ERROR);
		event.maxCt.set(1);
		event.endErrorEvent();
	}

	public static void wrErrEvent() {
		SDFSEvent event = new SDFSEvent(WER, getTarget(), "Write Error Detected", ERROR);
		event.maxCt.set(1);
		event.endErrorEvent();
	}

	public static SDFSEvent testEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(TEST, "atestvolume", shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent perfMonEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(PERFMON, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent umountEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(UMOUNT, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent archiveOutEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(AOUT, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent compactEvent() {
		SDFSEvent event = new SDFSEvent(COMPACT, getTarget(), "Running Compaction on DSE, this may take a while",
				RUNNING);
		return event;
	}

	public static SDFSEvent mountEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(MOUNT, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static SDFSEvent consistancyCheckEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(FSCK, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent loadHashDBEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(LHASHDB, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent flushAllBuffers() {
		SDFSEvent event = new SDFSEvent(FLUSHALL, getTarget(), "Flushing all buffers", RUNNING);
		return event;
	}

	public static SDFSEvent snapEvent(String shortMsg, File src) throws IOException {

		SDFSEvent event = new SDFSEvent(SNAP, getTarget(), shortMsg, RUNNING);
		if (src.isDirectory())
			event.maxCt.set(FileCounts.getCount(src, true));
		else
			event.maxCt.set(1);
		return event;
	}

	public static BlockImportEvent metaImportEvent(String shortMsg, SDFSEvent evt) {
		BlockImportEvent event = new BlockImportEvent(getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent deleteFileEvent(File f) {
		SDFSEvent event = new SDFSEvent(SDFSEvent.DELFILE, getTarget(), "File " + f.getPath() + " deleted", RUNNING);
		event.endEvent("File " + f.getPath() + " deleted", INFO);
		return event;
	}

	public static SDFSEvent deleteFileFailedEvent(File f) {
		SDFSEvent event = new SDFSEvent(SDFSEvent.DELFILE, getTarget(), "File " + f.getPath() + " delete failed", WARN);
		event.endEvent("File " + f.getPath() + " delete failed", WARN);
		return event;
	}

	public static SDFSEvent claimInfoEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(CLAIMR, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent waitEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(WAIT, getTarget(), shortMsg, RUNNING);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent removeInfoEvent(String shortMsg, SDFSEvent evt) {
		SDFSEvent event = new SDFSEvent(REMOVER, getTarget(), shortMsg, RUNNING);
		event.level = INFO;
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent gcInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(GC, getTarget(), shortMsg, RUNNING);
		return event;
	}

	public static FDiskEvent fdiskInfoEvent(String shortMsg) {
		FDiskEvent event = new FDiskEvent(shortMsg);

		return event;
	}

	public static FDiskEvent fdiskInfoEvent(String shortMsg, SDFSEvent evt) {
		FDiskEvent event = new FDiskEvent(shortMsg);
		try {
			evt.addChild(event);
		} catch (Exception e) {
		}
		return event;
	}

	public static SDFSEvent crckInfoEvent(String shortMsg) {
		SDFSEvent event = new SDFSEvent(FDISK, getTarget(), shortMsg, RUNNING);
		return event;
	}

	@Override
	public String toString() {
		synchronized (this) {
			StringBuffer sb = new StringBuffer();
			sb.append(this.uid);
			sb.append(format.format(new Date(this.startTime)));
			sb.append(",");
			sb.append(this.startTime);
			sb.append(",");
			if (this.endTime > 0)
				sb.append(format.format(new Date(this.endTime)));
			else
				sb.append("");
			sb.append(",");
			sb.append(this.endTime);
			sb.append(",");
			sb.append(this.level);
			sb.append(",");
			sb.append(this.type);
			sb.append(",");
			sb.append(this.target);
			sb.append(",");
			sb.append(this.shortMsg);
			sb.append(",");
			sb.append(this.longMsg);
			sb.append(",");
			if (this.maxCt.get() == 0 || this.curCt.get() == 0)
				sb.append("0");
			else
				sb.append(Double.toString(this.curCt.get() / this.maxCt.get()));
			sb.append(",");
			sb.append(this.curCt);
			sb.append(",");
			sb.append(this.maxCt);
			sb.append(",");
			sb.append(this.extendedInfo);
			sb.append(",");
			sb.append(this.success);
			return sb.toString();
		}

	}

	public Element toXML() throws ParserConfigurationException {
		synchronized (this) {
			Document doc = XMLUtils.getXMLDoc("event");
			/*
			 * if (SDFSLogger.isDebug()) SDFSLogger.getLog().debug(this.toString());
			 */
			Element root = doc.getDocumentElement();
			root.setAttribute("start-date", format.format(new Date(this.startTime)));
			root.setAttribute("start-timestamp", Long.toString(this.startTime));
			if (this.endTime > 0) {
				root.setAttribute("end-date", format.format(new Date(this.endTime)));
			}
			root.setAttribute("end-timestamp", Long.toString(this.endTime));
			root.setAttribute("level", this.level.toString());
			root.setAttribute("type", this.type.toString());
			root.setAttribute("target", this.target);
			root.setAttribute("short-msg", this.shortMsg);
			root.setAttribute("long-msg", this.longMsg);
			try {
				root.setAttribute("percent-complete", Double.toString((this.curCt.get() / this.maxCt.get())));
			} catch (Exception e) {
				root.setAttribute("percent-complete", "0");
			}
			root.setAttribute("max-count", Long.toString(this.maxCt.get()));
			root.setAttribute("current-count", Long.toString(this.curCt.get()));
			root.setAttribute("uuid", this.uid);
			root.setAttribute("parent-uid", this.puid);
			root.setAttribute("extended-info", this.extendedInfo);
			root.setAttribute("success", Boolean.toString(this.success));
			for (int i = 0; i < this.children.size(); i++) {
				Element el = this.children.get(i).toXML();
				doc.adoptNode(el);
				root.appendChild(el);
			}
			return (Element) root.cloneNode(true);
		}
	}

	public org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent toProtoBuf() throws IOException {
		try {
			org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.Builder b = org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent
					.newBuilder();

			if (this.puid != null) {
				b.setParentUuid(this.puid);
			}
			b.setUuid(this.uid).setExtendedInfo(this.extendedInfo).setSuccess(this.success)
					.setStartTime(this.startTime).setEndTime(this.endTime).setLevel(this.level.toString())
					.setType(this.type.toString()).setShortMsg(this.shortMsg).setLongMsg(this.longMsg);
			try {
				b.setPercentComplete(this.curCt.get() / this.maxCt.get());
			} catch (Exception e) {
				b.setPercentComplete(0);
			}
			b.setMaxCount(this.maxCt.get()).setCurrentCount(this.curCt.get());
			for (int i = 0; i < this.children.size(); i++) {
				b.setChildrenUUid(i, this.children.get(i).uid);
			}
			return b.build();
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to run", e);
			throw new IOException(e);
		}
	}

	public static List<org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent> getProtoBufEvents(String start,int length) {
		ArrayList<org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent> al = new ArrayList<org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent>();
		RocksIterator iter = evtdb.newIterator();
		for (iter.seekToFirst(); iter.isValid(); iter.next()) {
			try {
				synchronized(evtdb) {
					byte[] key = iter.key();
					byte [] val = evtdb.get(key);
					org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent evt = getEvent(val);
					if(evt != null) {
						al.add(evt);
					}
					
				}
				
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to list events", e);
				throw new NullPointerException("unable to list events");
			}
		}
		return al;
	}

	public static String getEvents() {
		synchronized (tasks) {
			Iterator<SDFSEvent> iter = SDFSEvent.tasks.values().iterator();
			StringBuffer sb = new StringBuffer();
			while (iter.hasNext()) {
				sb.append(iter.next());
				sb.append("/n");
			}
			return sb.toString();
		}
	}

	public static Element getXMLEvent(String uuid) throws ParserConfigurationException {
		synchronized (tasks) {
			if (tasks.containsKey(uuid))
				return tasks.get(uuid).toXML();
			else
				throw new NullPointerException("[" + uuid + "] could not be found");
		}
	}

	public static org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent getPotoBufEvent(String uuid) {
		synchronized (tasks) {
			if (tasks.containsKey(uuid)) {
				try {
					return tasks.get(uuid).toProtoBuf();
				} catch (Exception e) {
					SDFSLogger.getLog().warn("[" + uuid + "] could not be found", e);
					throw new NullPointerException("[" + uuid + "] could not be found");
				}
			} else
				throw new NullPointerException("[" + uuid + "] could not be found");
		}
	}

	public static SDFSEvent getEvent(String uuid) {
		synchronized (evtdb) {
			byte [] val = evtdb.get(uuid.getBytes());
			if (val != null) {
				org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.Builder b = org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.newBuilder();
				b.mergeFrom(val);
				return GetEvent(b.build())
			}
				
			else
				throw new NullPointerException("[" + uuid + "] could not be found");
		}
	}

	private static org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent getEvent(byte[] evtb) {
		try {
		org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.Builder b = org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.newBuilder();
		return b.mergeFrom(evtb).build();
		}catch(Exception e) {
			SDFSLogger.getLog().error("unable to read event",e);
			return null;
		}
	}

	public static Element getXMLEvents() throws ParserConfigurationException {
		Document doc = XMLUtils.getXMLDoc("events");
		Element root = doc.getDocumentElement();
		synchronized (tasks) {
			Iterator<SDFSEvent> iter = tasks.values().iterator();
			while (iter.hasNext()) {
				Element el = iter.next().toXML();
				doc.adoptNode(el);
				root.appendChild(el);
			}
		}
		return (Element) root.cloneNode(true);
	}

	public static class Level implements java.io.Serializable {
		/**
		 *
		 */
		private static final long serialVersionUID = -2246117933197717794L;
		private String type = "";

		protected Level(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return this.type;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (!(obj instanceof Level)) {
				return false;
			}
			Level c = (Level) obj;
			return c.type.equalsIgnoreCase(this.type);
		}
	}

	public static class Type implements java.io.Serializable {
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		private String type = "";

		protected Type(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return this.type;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (!(obj instanceof Type)) {
				return false;
			}
			Type c = (Type) obj;
			return c.type.equalsIgnoreCase(this.type);
		}
	}

	public static String getTarget() {
		if (Main.standAloneDSE)
			return "Storage node " + Main.DSEID;
		else if (Main.volume != null)
			return Main.volume.getName();
		else
			return "test";
	}

}
