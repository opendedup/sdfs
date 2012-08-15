package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.sdfs.replication.ArchiveImporter;
import org.opendedup.util.SDFSLogger;


public class ImportArchiveCmd {


	public String getResult(String archive, String dest,String server,String password,int port,int maxSz) throws IOException {
		return importArchive(archive,dest,server,password,port,maxSz);
	}

	private String importArchive(String archive,String dest,String server,String password,int port,int maxSz)
			throws IOException {
		String sc = " not successful";
		try {
			String st =ArchiveImporter.importArchive(archive, dest,server,password,port,maxSz);
			sc = "successful";
			SDFSLogger.getLog().info("Exited Replication task [" +sc +"]");
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
		finally {
			SDFSLogger.getLog().info("Exited Replication task [" +sc +"]");
		}
	}

}
