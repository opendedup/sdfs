package org.opendedup.util;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public class ISCSITargetExplorer {

	public static synchronized void export(String iscsiRootPath,
			String fileName, String network, long length, String iqn)
			throws IOException {
		try {
			File f = new File(iscsiRootPath);
			if (!f.exists()) {
				f.mkdirs();
				Set<PosixFilePermission> perms = PosixFilePermissions
						.fromString("rwxrwxrwx");
				try {
					Files.setPosixFilePermissions(f.toPath(), perms);
				} catch (Exception e) {
					System.out.println("Error creating nfs folder"
							+ e.getMessage());
				}
			}
			String tcmProc = "tcm_node --fileio fileio_0/" + fileName + " "
					+ iscsiRootPath + "/" + fileName + " " + length;
			String addLunProc = "lio_node --addlun " + iqn + " 1 0 " + fileName
					+ " " + "fileio_0/" + fileName;
			String addNetProc = "lio_node --addnp " + iqn + " 1 " + network
					+ ":3260";
			String addPermProc = "lio_node --permissive " + iqn + " 1";
			String iqn2 = iqn.replaceAll(":", "\\:");
			String addRWProc = "/sys/kernel/config/target/iscsi/" + iqn2
					+ "/tpgt_1/attrib/demo_mode_write_protect";
			String disAuthProc = "lio_node --disableauth=" + iqn + " 1";
			String enProc = "lio_node  --enabletpg " + iqn + " 1";
			Process p = Runtime.getRuntime().exec(tcmProc);
			if (p.waitFor() != 0) {
				throw new IOException("unable to execute \"" + tcmProc + "\"");
			}
			p = Runtime.getRuntime().exec(addLunProc);
			if (p.waitFor() != 0) {
				throw new IOException("unable to execute \"" + addLunProc
						+ "\"");
			}
			p = Runtime.getRuntime().exec(addNetProc);
			if (p.waitFor() != 0) {
				throw new IOException("unable to execute \"" + addNetProc
						+ "\"");
			}
			p = Runtime.getRuntime().exec(addPermProc);
			if (p.waitFor() != 0) {
				throw new IOException("unable to execute \"" + addPermProc
						+ "\"");
			}
			FileOutputStream out = new FileOutputStream(addRWProc);
			DataOutputStream dout = new DataOutputStream(out);
			dout.writeInt(0);
			out.close();
			out.flush();
			p = Runtime.getRuntime().exec(disAuthProc);
			if (p.waitFor() != 0) {
				throw new IOException("unable to execute \"" + disAuthProc
						+ "\"");
			}
			p = Runtime.getRuntime().exec(enProc);
			if (p.waitFor() != 0) {
				throw new IOException("unable to execute \"" + enProc + "\"");
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static synchronized void unexport(String iscsiRootPath,
			String fileName, String network, long length, String iqn)
			throws IOException {
		try {
			File f = new File(iscsiRootPath);
			if (!f.exists()) {
				f.mkdirs();
				Set<PosixFilePermission> perms = PosixFilePermissions
						.fromString("rwxrwxrwx");
				try {
					Files.setPosixFilePermissions(f.toPath(), perms);
				} catch (Exception e) {
					System.out.println("Error creating nfs folder"
							+ e.getMessage());
				}
			}
			String delIqn = "lio_node --deliqn " + iqn;
			String delTcm = "tcm_node --freedev " + "fileio_0/" + fileName;
			String disProc = "lio_node  --deltpg " + iqn + " 1";
			Process p = Runtime.getRuntime().exec(disProc);
			if (p.waitFor() != 0) {
				throw new IOException("unable to execute \"" + disProc + "\"");
			}
			p = Runtime.getRuntime().exec(delIqn);
			if (p.waitFor() != 0) {
				throw new IOException("unable to execute \"" + delIqn + "\"");
			}
			p = Runtime.getRuntime().exec(delTcm);
			if (p.waitFor() != 0) {
				throw new IOException("unable to execute \"" + delTcm + "\"");
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static synchronized boolean available(String iqn) {
		return new File("/sys/kernel/config/target/iscsi/" + iqn).exists();
	}

	public static void main(String[] args) throws NumberFormatException,
			IOException {
		if (args[0].equalsIgnoreCase("up"))
			export(args[1], args[2], args[3], Long.parseLong(args[4]), args[5]);
		else if (args[0].equalsIgnoreCase("down"))
			unexport(args[1], args[2], args[3], Long.parseLong(args[4]),
					args[5]);
		else if (args[0].equalsIgnoreCase("avail"))
			System.out.println(available(args[1]));
		else {
			System.out.println("invalid arguement " + args[0]);
		}
	}

}
