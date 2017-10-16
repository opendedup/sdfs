package org.opendedup.sdfs.mgmt;

import java.io.File;



import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;

public class SetFileAttribute {

	public static void getResult(String file,String name,String value) throws IOException  {
		String internalPath = Main.volume.getPath() + File.separator + file;
		File f = new File(internalPath);
		if (!f.exists() || f.isDirectory())
			throw new IOException("requeste file " + file + " does not exist");
		else {
			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(internalPath);
				if(mf != null) {
				if(value != null)
					mf.addXAttribute(name, value);
				else
					mf.removeXAttribute(name);
				}else 
					SDFSLogger.getLog().warn("file " + file + " could not be found.");
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to fulfill request on file " + file, e);
				throw new IOException(
						"request to fetch attributes failed because "
								+ e.toString());
			}
		}
	}

}
