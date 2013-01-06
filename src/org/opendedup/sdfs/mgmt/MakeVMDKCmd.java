package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.VMDKParser;

public class MakeVMDKCmd implements XtendedCmd {

	static long tbc = 1099511627776L;
	static long gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;
	static int kbc = 1024;

	@Override
	public String getResult(String cmd, String file) throws IOException {
		String[] args = cmd.split(":");
		String destination = args[0];
		String size = args[1];
		File f = new File(Main.volume.getPath() + File.separator + file);
		if (!f.isDirectory())
			return "ERROR VMDK Creation Failed : ["
					+ file
					+ "] is not a directory. This command can only be executed on directories";
		File dst = new File(Main.volume.getPath() + File.separator
				+ destination);
		if (dst.exists())
			throw new IOException("ERROR VMDK Creation Failed : ["
					+ destination + "] already exists");
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
			VMDKParser.writeFile(f.getPath(), destination, fSize);
			return "SUCCESS VMDK Creation Success : VMDK Created in " + file
					+ File.separator + destination + " for size " + size;
		} catch (Exception e) {
			String errorMsg = "ERROR VMDK Creation Failed : for " + file
					+ File.separator + destination + " of size " + size;
			SDFSLogger.getLog().error(errorMsg, e);
			throw new IOException(errorMsg + " because: " + e.toString());
		}
	}

}
