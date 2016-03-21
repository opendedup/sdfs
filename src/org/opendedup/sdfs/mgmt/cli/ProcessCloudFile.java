package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessCloudFile {
	public static void runCmd(String file, String dstfile) {
		try {
			System.out.printf("Checking out a read only copy of [%s] \n", file);
			file = URLEncoder.encode(file, "UTF-8");
			file = URLEncoder.encode(file, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			if(dstfile != null)
				formatter.format("file=%s&cmd=cloudfile&dstfile=%s", file,dstfile);
			else
				formatter.format("file=%s&cmd=cloudfile", file);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			System.out.printf(
					"Checking out a read only copy of [%s] returned [%s]\n",
					status, msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
