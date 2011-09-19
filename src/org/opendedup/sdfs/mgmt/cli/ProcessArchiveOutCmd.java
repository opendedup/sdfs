package org.opendedup.sdfs.mgmt.cli;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.util.SDFSLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessArchiveOutCmd {
	public static void runCmd(String file) throws IOException {
			SDFSLogger.getLog().debug("archive a copy of ["+file+"]");
			file = URLEncoder.encode(file, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=archiveout&options=iloveanne", file);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			SDFSLogger.getLog().debug("getting " + msg);
			InputStream in = MgmtServerConnection.connectAndGet("", msg);
			FileOutputStream out = new FileOutputStream(msg);
			byte[] buf = new byte[32768];
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}
			in.close();
			out.close();
			SDFSLogger.getLog().debug("Copy Out ["+status+"] returned ["+msg+"]");
			SDFSLogger.getLog().info(msg);
			if(status.equalsIgnoreCase("failed"))
				throw new IOException("archive failed because " +msg);

	}

}
