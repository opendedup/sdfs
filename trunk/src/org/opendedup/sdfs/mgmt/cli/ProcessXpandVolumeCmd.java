package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessXpandVolumeCmd {
	public static void runCmd(String size) {
		try {
			System.out.printf("expanding volume to [%s]\n", size);
			size = URLEncoder.encode(size, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);

			formatter.format("file=%s&cmd=expandvolume&size=%s", "", size);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			formatter.close();
			System.out.printf("Expand [%s] returned [%s]\n", status, msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
