package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;


import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessCopyExtents {
	public static void runCmd(String srcfile,String dstfile,long sstart, long len,long dstart) {
		try {

			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=copyextents&srcfile=%s&dstfile=%s&sstart=%d&len=%s&dstart=%d", 
					"null",URLEncoder.encode(srcfile,"UTF-8"),URLEncoder.encode(dstfile,"UTF-8"),sstart,len,dstart);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				Element cpx = (Element) root.getElementsByTagName("copy-extent")
						.item(0);
				System.out.printf("Copied Source : %s\n",
						cpx.getAttribute("srcfile"));
				System.out.printf("Copied Destination : %s\n",
						cpx.getAttribute("dstfile"));
				System.out.printf("Source Start : %s\n",
						cpx.getAttribute("requested-source-start"));
				System.out.printf("Destination Start : %s\n",
						cpx.getAttribute("requested-dest-start"));
				System.out.printf("Requested Bytes Copied : %s\n",
						cpx.getAttribute("lenth"));
				System.out.printf("Actual Bytes Copied : %s\n",
						cpx.getAttribute("written"));
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


}
