package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.sdfs.Main;

public class MakeFolderCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		String internalPath = Main.volume.getPath() + File.separator + file;
		File f = new File(internalPath);
		if (f.exists())
			throw new IOException("requeste folder " + file + " already exists");
		else {
			f.mkdirs();
			return "created folder " + file;
		}
	}

}
