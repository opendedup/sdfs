package org.opendedup.mtools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.VMDKData;
import org.opendedup.util.VMDKParser;

public class DiskUtils {
	public static HashMap<String, VMDKMountPoint> mountedVMDKs = new HashMap<String, VMDKMountPoint>();

	public static ArrayList<Partition> getPartitions(String fileName,
			long cylinders) throws IOException {
		ArrayList<Partition> al = new ArrayList<Partition>();
		Process p = Runtime.getRuntime().exec(
				"fdisk -l -u -C " + cylinders + " " + fileName);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line = null;
		boolean initialized = false;
		while ((line = reader.readLine()) != null) {
			if (initialized) {
				String[] tokens = line.split("\\s+");
				Partition par = new Partition();
				par.setDevice(tokens[0].trim());

				if (tokens[1].trim().equals("*")) {
					par.setBoot(true);
					par.setStart(Integer.parseInt(tokens[2]));
					par.setEnd(Long.parseLong(tokens[3]));
					par.setBlocks(Long.parseLong(tokens[4]));
					par.setSystem(Integer.parseInt(tokens[5]));
					par.setType(tokens[6].trim());
				} else {
					par.setBoot(false);
					par.setStart(Integer.parseInt(tokens[1]));
					par.setEnd(Long.parseLong(tokens[2]));
					par.setBlocks(Long.parseLong(tokens[3]));
					par.setSystem(Integer.parseInt(tokens[4]));
					par.setType(tokens[5].trim());
				}
				al.add(par);
			} else
				System.out.println(line);
			if (line.trim().toLowerCase().startsWith("device"))
				initialized = true;

		}
		return al;
	}

	public static synchronized String mountLoopBack(String fileName, long offset)
			throws IOException {
		offset = offset * 512;
		Process p = Runtime.getRuntime().exec(
				"losetup -v -f -o " + offset + " " + fileName);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line = reader.readLine();
		String[] tokens = line.split(" /");
		if (tokens.length != 2)
			throw new IOException("unable to mount loopback for device "
					+ fileName + " output is " + line);
		String dev = "/" + tokens[1];
		SDFSLogger.getLog().info("mounted volume to  " + dev);
		return dev;
	}

	public static synchronized boolean mountVMDK(String fileName,
			String mountPath, long offset) {
		mountPath = new File(mountPath).getPath();
		SDFSLogger.getLog().info(
				"mounting " + fileName + " to " + mountPath + " with offset : "
						+ offset);
		boolean mounted = true;
		String loopBack = null;
		if (!mountedVMDKs.containsKey(mountPath)) {
			try {
				loopBack = mountLoopBack(fileName, offset);
				VMDKMountPoint mp = new VMDKMountPoint(loopBack, mountPath);
				File f = new File(mountPath);
				if (!f.exists())
					f.mkdirs();
				String mountCmd = "mount -t ntfs " + loopBack + " "
						+ mp.getMountPoint();
				SDFSLogger.getLog().info("mounting cmd " + mountCmd);
				Process p = Runtime.getRuntime().exec(mountCmd);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(p.getErrorStream()));
				String line;

				while ((line = reader.readLine()) != null) {
					SDFSLogger.getLog().info(
							" error mounting " + fileName + " mount output "
									+ line);
					mounted = false;
				}

				try {
					p.waitFor();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					mounted = false;
				}
				if (!mounted) {
					f.delete();
					p = Runtime.getRuntime().exec("losetup -d " + loopBack);
					SDFSLogger.getLog().info(" unable to mount filesystem ");
					try {
						p.waitFor();
					} catch (InterruptedException e) {
						SDFSLogger.getLog().fatal(e.toString(), e);
					}
				}
				mountedVMDKs.put(mountPath, mp);
				SDFSLogger.getLog()
						.info("mount of " + fileName + " successful");
			} catch (IOException e) {
				if (loopBack != null) {
					try {
						Process p = Runtime.getRuntime().exec(
								"losetup -d " + loopBack);
						p.waitFor();
					} catch (Exception e1) {
					}
				}

				SDFSLogger.getLog().fatal(
						"unable to mount volume " + fileName + " to "
								+ mountPath, e);
				mounted = false;
			}

			return mounted;
		} else {
			return false;
		}
	}

	public static boolean isMountedVMDK(String mountPath) {
		return mountedVMDKs.containsKey(mountPath);
	}

	public static void unmountVMDK(String mountPath) {
		if (mountedVMDKs.containsKey(mountPath)) {
			VMDKMountPoint mp = mountedVMDKs.get(mountPath);
			try {
				Process p = Runtime.getRuntime().exec(
						"umount -f -l " + mp.getMountPoint());
				p.waitFor();
				p = Runtime.getRuntime().exec("losetup -d " + mp.getLoopBack());
				p.waitFor();
				mountedVMDKs.remove(mountPath);
				File f = new File(mountPath);
				f.delete();
			} catch (Exception e) {
				SDFSLogger.getLog().fatal(
						"unable to unmount volume " + mountPath, e);
			}
		}
	}

	public static String mountVMDK(String vmdkPath, String mountPath)
			throws IOException {
		SDFSLogger.getLog().info("mounting " + vmdkPath + " to " + mountPath);
		File internalFile = new File(vmdkPath);
		VMDKData data = VMDKParser.parseVMDKFile(internalFile.getPath());
		File diskFile = new File(internalFile.getParentFile().getPath()
				+ File.separator + data.getDiskFile());
		long cylanders = data.getCylinders();
		ArrayList<Partition> partitions = getPartitions(diskFile.getPath(),
				cylanders);
		File mountPoint = new File(mountPath);
		if (partitions.size() == 0)
			return null;
		if (partitions.size() == 1) {
			Partition part = partitions.get(0);
			mountVMDK(diskFile.getPath(), mountPoint.getPath(), part.getStart());

		} else {
			for (int i = 0; i < partitions.size(); i++) {
				Partition part = partitions.get(i);
				mountVMDK(diskFile.getPath(), mountPoint.getPath()
						+ File.separator + i, part.getStart());
			}
		}
		return mountPoint.getPath();
	}

	/*
	 * public static File getInternalFile(String fileName) throws IOException {
	 * Volume vol = JLanConfig.getVolume(fileName); File localVolumePath = new
	 * File(vol.getLocalPath()); SDFSLogger.getLog().info("file name is " +
	 * fileName + " volume is " + vol.getName() + " local path is  " +
	 * vol.getLocalPath()); File baseFilePath = new File(fileName); fileName =
	 * baseFilePath.getPath(); File internalFile = new
	 * File(vol.getInternalMountPoint() + File.separator +
	 * fileName.substring(localVolumePath.getPath().length())); return
	 * internalFile; }
	 */

	public static void prepareVMDK(String fileName) throws IOException {
		File internalFile = new File(fileName);
		VMDKData data = VMDKParser.parseVMDKFile(internalFile.getPath());
		File diskFile = new File(internalFile.getParentFile().getPath()
				+ File.separator + data.getDiskFile());
		prepareVMDK(diskFile.getPath(), data.getCylinders());
	}

	public static void prepareVMDK(String fileName, long cylinders)
			throws IOException {
		SDFSLogger.getLog()
				.info("Preparing vmdk " + fileName + " " + cylinders);
		String cmd = Main.scriptsDir + File.separator + "prepdisk.sh" + " "
				+ fileName + " " + cylinders + " " + Main.scriptsDir;
		SDFSLogger.getLog().info("executing " + cmd);
		Process p = Runtime.getRuntime().exec(cmd);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line;
		while ((line = reader.readLine()) != null) {
			SDFSLogger.getLog().info(
					"Making " + fileName + " fdisk output " + line);
		}
		try {
			p.waitFor();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String loopDevice = mountLoopBack(fileName, Main.defaultOffset);
		p = Runtime.getRuntime()
				.exec("mkfs.ntfs  -f -c 32768 -v " + loopDevice);
		reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		while ((line = reader.readLine()) != null) {
			SDFSLogger.getLog().info(
					"NTFS Format " + fileName + " ntfs output " + line);
		}
		p = Runtime.getRuntime().exec("losetup -d " + loopDevice);
		try {
			p.waitFor();
		} catch (InterruptedException e) {
			SDFSLogger.getLog().fatal(e.toString(), e);
		}
	}

	protected static void unmountAll() {
		synchronized (mountedVMDKs) {
			Iterator<VMDKMountPoint> vIter = mountedVMDKs.values().iterator();
			while (vIter.hasNext()) {
				unmountVMDK(vIter.next());
			}
			mountedVMDKs.clear();
			SDFSLogger.getLog().info("Unmounted all VMDKs");
		}
	}

	private static void unmountVMDK(VMDKMountPoint mp) {
		try {
			Process p = Runtime.getRuntime().exec(
					"umount -f -l " + mp.getMountPoint());
			p.waitFor();
			p = Runtime.getRuntime().exec("losetup -d " + mp.getLoopBack());
			p.waitFor();
			File f = new File(mp.getMountPoint());
			f.delete();
			SDFSLogger.getLog().info("unmounted " + mp.getMountPoint());
		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"unable to unmount volume " + mp.getMountPoint(), e);
		}
	}

	public static void close() {
		unmountAll();
	}

	public static void main(String[] args) {
		String cmd = args[0];
		if (cmd.equalsIgnoreCase("format")) {
			if (args.length < 2)
				System.out
						.println("Partition and format(ntfs) a virtual volume.");
			System.out
					.println("Note : Only FAT ESX vmdks are supported and the taget \n "
							+ "should be the description file");
			System.out
					.println("e.g. format /mount/vmware/vmfs/win2k8/win2k8.vmdk");
			try {
				DiskUtils.prepareVMDK(args[1]);
				System.out.println("Success");
			} catch (Exception e) {
				System.err.println("format failed");
				e.printStackTrace();
			}
		}

	}

}
