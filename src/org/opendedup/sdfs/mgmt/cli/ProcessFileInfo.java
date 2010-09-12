package org.opendedup.sdfs.mgmt.cli;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;


public class ProcessFileInfo {
	public static void runCmd(String file) {
		try {
			file = URLEncoder.encode(file,"UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=info", file);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			Element files = (Element)root.getElementsByTagName("files").item(0);
			for(int i = 0; i < files.getElementsByTagName("file-info").getLength();i++){
				Element fileEl = (Element)files.getElementsByTagName("file-info").item(i);
				System.out.println("#");
				System.out.printf("file name : %s\n",fileEl.getAttribute("file-name") );
				System.out.printf("sdfs path : %s\n",fileEl.getAttribute("sdfs-path") );
				System.out.printf("file type : %s\n",fileEl.getAttribute("file-type") );
				if(fileEl.getAttribute("file-type").equalsIgnoreCase("file")) {
				System.out.printf("dedup file : %s\n",fileEl.getAttribute("dedup") );
				System.out.printf("map file guid : %s\n",fileEl.getAttribute("dedup-map-guid") );
				System.out.printf("file type : %s\n",fileEl.getAttribute("file-type") );
				System.out.printf("file type : %s\n",fileEl.getAttribute("file-type") );
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
