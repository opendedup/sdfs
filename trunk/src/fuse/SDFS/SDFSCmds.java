package fuse.SDFS;

import java.io.File;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.gc.ManualGC;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.RandomGUID;
import org.opendedup.util.VMDKParser;

import fuse.FuseException;
import fuse.XattrLister;

public class SDFSCmds {
	private static final Log log = LogFactory.getLog(SDFSCmds.class);
	public String mountedVolume;
	public String mountPoint;

	static long tbc = 1024 * 1024 * 1024 * 1024;
	static long gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;
	static int kbc = 1024;

	public static final String[] cmds = { "user.cmd.cleanstore","user.cmd.dedupAll",
			"user.cmd.optimize", "user.cmd.snapshot", "user.cmd.vmdk.make",
			"user.cmd.ids.clearstatus", 
			"user.cmd.ids.status", "user.cmd.file.flush", "user.cmd.flush.all",
			"user.sdfs.file.isopen", "user.sdfs.ActualBytesWritten",
			"user.sdfs.VirtualBytesWritten", "user.sdfs.BytesRead",
			"user.sdfs.DuplicateData", 
			"user.sdfs.VMDK", "user.sdfs.fileGUID", "user.sdfs.dfGUID",
			"user.sdfs.dedupAll","user.dse.size","user.dse.maxsize"
	};
	
	public static final String[] cmdDes = {
		"","",
		"", "", "",
		"", 
		"", "", "",
		"", "",
		"", "",
		"", 
		"", "", "",
		"","",""
	};
	
	/*
	public static final String[] cmdDes = {
	"\"Collect all the unused chunks older than <minutes>\" e.g. setfattr -n user.cmd.cleanstore -v 6777:<minutes> ./",
		"\"sets the file to dedup all chunks or not. Set to true if you would like to dedup all chunks <unique-command-id:true or false>\"",
		"\"optimize the file by specifiying a specific length <unique-command-id:length-in-bytes>\"",
		"\"Take a Snapshot of a File or Folder <unique-command-id:snapshotdst>\"",
		"\"Creates an simple flat vmdk in this directory <unique-command-id:vmdkname:size(TB|GB|MB)>. "
				+ "The command must be executed on a directory. e.g."
				+ "setfattr -n user.cmd.vmdk.make -v 5556:bigvserver:500GB /dir",
		"clear all command id status\"",
		RandomGUID.getGuid(),
		"\"get the status if a specific command e.g. to get the status of"
				+ " command id 54333 run getfattr -n user.cmd.ids.status.54333\"",
		"\"Flush write cachefor specificed file <unique-command-id>\"",
		"\"Flush write cache for all files\"",
		"\"checks if the file is open <unique-command-id>\"", "", "", "", "",
		"", "", "", "", "", "","","","",""};
	*/
	public static HashMap<String, String> cmdList = new HashMap<String, String>();
	
	private static LinkedHashMap<String, String> cmdStatus = new LinkedHashMap<String, String>(
			100) {
		// (an anonymous inner class)
		private static final long serialVersionUID = 1;

		@Override
		protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
			return size() > 100;
		}
	};

	static {
		for (int i = 0; i < cmds.length; i++) {
			cmdList.put(cmds[i], cmdDes[i]);
		}
	}

	public SDFSCmds(String mountedvolume, String mountpoint) {
		this.mountedVolume = mountedvolume;
		this.mountPoint = mountpoint;
	}

	public void listAttrs(XattrLister lister) {
		for (int i = 0; i < cmds.length; i++) {
			lister.add(cmds[i]);
		}
	}

	public String getAttr(String command, String path) {
		String internalPath = this.mountedVolume + File.separator + path;
		MetaDataDedupFile mf = MetaFileStore.getMF(internalPath);
		File f = new File(internalPath);
		if (!f.isDirectory()) {
			if (command.equalsIgnoreCase("user.sdfs.dedupAll")) {
				return Boolean.toString(mf.isDedup());
			}
			if (command.equalsIgnoreCase("user.sdfs.file.isopen")) {
				return Boolean.toString(DedupFileStore.fileOpen(mf));
			}
			if (command.equalsIgnoreCase("user.sdfs.ActualBytesWritten")) {
				return Long.toString(mf.getIOMonitor().getActualBytesWritten());
			}
			if (command.equalsIgnoreCase("user.sdfs.VirtualBytesWritten")) {
				return Long
						.toString(mf.getIOMonitor().getVirtualBytesWritten());
			}
			if (command.equalsIgnoreCase("user.sdfs.BytesRead")) {
				return Long.toString(mf.getIOMonitor().getBytesRead());
			}
			if (command.equalsIgnoreCase("user.sdfs.DuplicateData")) {
				return Long.toString(mf.getIOMonitor().getDuplicateBlocks());
			}
			if (command.equalsIgnoreCase("user.sdfs.VMDK")) {
				return Boolean.toString(mf.isVmdk());
			}
			if (command.equalsIgnoreCase("user.sdfs.fileGUID")) {
				return mf.getGUID();
			}
			if (command.equalsIgnoreCase("user.sdfs.dfGUID")) {
				if (mf.getDfGuid() != null)
					return mf.getDfGuid();
			}
		}
		if(command.equalsIgnoreCase("user.dse.size")) {
			return Long.toString(HCServiceProxy.getSize() * Main.CHUNK_LENGTH);
		}
		if(command.equalsIgnoreCase("user.dse.maxsize")) {
			
			return Long.toString(HCServiceProxy.getMaxSize() * Main.CHUNK_LENGTH);
		}
		if (command.equals("user.cmd.ids.status"))
			return cmdList.get(command);
		if (command.startsWith("user.cmd.ids.status")) {
			String[] tokens = command.split("\\.");

			String id = tokens[tokens.length - 1];
			if (cmdStatus.containsKey(id)) {
				String msg = (String) cmdStatus.get(id);
				return msg;
			} else
				return "no status found for id=" + id;
		} else if (command.equalsIgnoreCase("user.cmd.nextid")) {
			return RandomGUID.getGuid();
		} else {
			return cmdList.get(command);
		}

	}

	public void runCMD(String path, String command, String value) throws FuseException {
		if (command.startsWith("user.sdfs")) {
			String name = command;
			String valStr = value;
			File f = new File(mountedVolume + File.separator + path);
			if (!f.isDirectory()) {
				if (name.startsWith("user.sdfs")) {
					MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
					long val = Long.parseLong(valStr);
					if (name.equalsIgnoreCase("user.sdfs.ActualBytesWritten")) {
						mf.getIOMonitor().setActualBytesWritten(val);
					} else if (name
							.equalsIgnoreCase("user.sdfs.VirtualBytesWritten")) {
						mf.getIOMonitor().setVirtualBytesWritten(val);
					} else if (name.equalsIgnoreCase("user.sdfs.BytesRead")) {
						mf.getIOMonitor().setBytesRead(val);
					} else if (name.equalsIgnoreCase("user.sdfs.DuplicateData")) {
						mf.getIOMonitor().setDuplicateBlocks(val);
					}
				}
			}
		} else {
			String[] args = value.split(":");
			String status = "no status";
			if (command.equalsIgnoreCase("user.cmd.dedupAll")) {
				boolean dedup = Boolean.parseBoolean(args[1]);
				status = dedup(path, dedup);
			}
			if(command.equalsIgnoreCase("user.cmd.cleanstore")) {
				int minutes = Integer.parseInt(args[1]);
				status = "command completed successfully";
				try {
					ManualGC.clearChunks(minutes);
				} catch(Exception e) {
					status = "command failed : " +e.getMessage();
				}
			}
			if (command.equalsIgnoreCase("user.cmd.optimize")) {
				long length = Long.parseLong(args[1]);
				status = optimize(path, length);
			}
			if (command.equalsIgnoreCase("user.cmd.snapshot")) {
				if(Main.volume.isFull())
					throw new FuseException("Volume Full")
				.initErrno(FuseException.ENOSPC);
				status = takeSnapshot(path, args[1]);
			}
			if (command.equalsIgnoreCase("user.cmd.vmdk.make")) {
				if(Main.volume.isFull())
					throw new FuseException("Volume Full")
				.initErrno(FuseException.ENOSPC);
				status = this.makeVMDK(path, args[1], args[2]);
			}
			if (command.equalsIgnoreCase("user.cmd.ids.clearstatus")) {
				cmdStatus.clear();
				status = "all status messages cleared";
			}
			if (command.equalsIgnoreCase("user.cmd.file.flush")) {
				status = flushFileCache(path);
			}
			if (command.equalsIgnoreCase("user.cmd.flush.all")) {
				status = flushAllCache();
			}
			cmdStatus.put(args[0], status + " cmd=" + command + " " + value);
		}
	}

	public String flushAllCache() {
		try {
			DedupFileStore.flushAllFiles();
			return "SUCCESS Flush All Files : Write Cache Flushed";
		} catch (Exception e) {
			String errorMsg = "ERROR Flush All Files Failed : ";
			log.error(errorMsg, e);
			return "ERROR Flush All Files Failed : " + errorMsg + " because: "
					+ e.toString();
		}
	}

	public String flushFileCache(String path) {
		String internalPath = this.mountedVolume + File.separator + path;
		String externalPath = this.mountPoint + File.separator + path;
		File parentDir = new File(internalPath);
		if (parentDir.isDirectory())
			return "ERROR Flush File Failed : ["
					+ externalPath
					+ "] is a directory. This command cannot be executed on directories";
		else {
			try {
				MetaFileStore.getMF(internalPath).getDedupFile().writeCache();
				return "SUCCESS Flush File : Write Cache Flushed for "
						+ externalPath;
			} catch (Exception e) {
				String errorMsg = "ERROR Flush File Failed :for "
						+ externalPath;
				log.error(errorMsg, e);
				return errorMsg + " because: " + e.toString();
			}
		}
	}

	private synchronized String makeVMDK(String path, String fileName,
			String size) {
		String internalPath = this.mountedVolume + File.separator + path;
		String externalPath = this.mountPoint + File.separator + path;
		File parentDir = new File(internalPath);
		if (!parentDir.isDirectory())
			return "ERROR VMDK Creation Failed : ["
					+ externalPath
					+ "] is not a directory. This command can only be executed on directories";
		File dst = new File(internalPath + File.separator + fileName);
		if (dst.exists())
			return "ERROR VMDK Creation Failed : [" + externalPath
					+ File.separator + fileName + "] already exists";
		try {
			String units = size.substring(size.length() - 2);
			int sz = Integer.parseInt(size.substring(0, size.length() - 2));
			long fSize = 0;
			if (units.equalsIgnoreCase("TB"))
				fSize = sz * tbc;
			if (units.equalsIgnoreCase("GB"))
				fSize = sz * gbc;
			if (units.equalsIgnoreCase("MB"))
				fSize = sz * mbc;
			VMDKParser.writeFile(internalPath, fileName, fSize);
			return "SUCCESS VMDK Creation Success : VMDK Created in "
					+ externalPath + File.separator + fileName;
		} catch (Exception e) {
			String errorMsg = "ERROR VMDK Creation Failed : for "
					+ externalPath + File.separator + fileName + " " + size;
			log.error(errorMsg, e);
			return errorMsg + " because: " + e.toString();
		}
	}

	private String optimize(String srcPath, long length) {
		File f = new File(this.mountedVolume + File.separator + srcPath);
		try {
			MetaFileStore.getMF(f.getPath()).getDedupFile().optimize(length);
			return "SUCCESS Optimization Success: optimized [" + srcPath
					+ "] of length [" + length + "]";
		} catch (IOException e) {
			log.error("ERROR Optimize Failed: unable to optimize Source ["
					+ srcPath + "] " + "length [" + length + "] because :"
					+ e.toString(), e);
			return "ERROR Optimize Failed: unable to optimize Source ["
					+ srcPath + "] " + "length [" + length + "] because :"
					+ e.toString();
		}
	}

	private String dedup(String srcPath, boolean dedup) {
		File f = new File(this.mountedVolume + File.separator + srcPath);
		try {
			MetaFileStore.getMF(f.getPath()).setDedup(dedup);
			return "SUCCESS Dedup Success: set dedup to [" + srcPath + "]  ["
					+ dedup + "]";
		} catch (Exception e) {
			log.error("ERROR Dedup Failed: unable to set dedup Source ["
					+ srcPath + "] " + "length [" + dedup + "] because :"
					+ e.toString(), e);
			return "ERROR Dedup Failed: unable to set dedup Source [" + srcPath
					+ "] " + "length [" + dedup + "]  because :" + e.toString();
		}
	}

	private synchronized String takeSnapshot(String srcPath, String dstPath) {
		File f = new File(this.mountedVolume + File.separator + srcPath);
		File nf = new File(this.mountedVolume + File.separator + dstPath);

		if (f.getPath().equalsIgnoreCase(nf.getPath()))
			return "ERROR Snapshot Failed: Source [" + srcPath
					+ "] and destination [" + dstPath + "] are the same";
		if (nf.exists())
			return "ERROR Snapshot Failed: destination [" + dstPath
					+ "] already exists";
		try {
			MetaFileStore.snapshot(f.getPath(), nf.getPath(), false);
			return "SUCCESS Snapshot Success: took snapshot Source [" + srcPath
					+ "] " + "Destination [" + dstPath + "]";
		} catch (IOException e) {
			log.error("ERROR Snapshot Failed: unable to take snapshot Source ["
					+ srcPath + "] " + "Destination [" + dstPath
					+ "] because :" + e.toString(), e);
			return "ERROR Snapshot Failed: unable to take snapshot Source ["
					+ srcPath + "] " + "Destination [" + dstPath
					+ "] because :" + e.toString();
		}
	}

}
