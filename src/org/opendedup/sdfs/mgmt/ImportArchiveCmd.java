package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.sdfs.replication.ArchiveImporter;
import org.opendedup.util.SDFSLogger;


public class ImportArchiveCmd implements XtendedCmd {

	@Override
	public String getResult(String archive, String dest) throws IOException {
		return importArchive(archive,dest);
	}

	private String importArchive(String archive,String dest)
			throws IOException {
		try {
			ArchiveImporter.importArchive(archive, dest);
			String st ="import archive ["
									+ archive + "] " + "Destination [" + dest
									+ "]";
			return st;
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Unable to import archive ["
							+ archive + "] " + "Destination [" + dest
							+ "] because :" + e.toString(), e);
			throw new IOException(
					"Unable to import archive ["
							+ archive + "] " + "Destination [" + dest
							+ "] because :" + e.toString());
		}
	}

}
