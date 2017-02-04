package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessSetFileAttribute {
	public static void runCmd(String file,String name,String value) {
		try {
			file = URLEncoder.encode(file,"UTF-8");
			name = URLEncoder.encode(name,"UTF-8");
			if(value !=null)
				value = URLEncoder.encode(value, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			if(value != null)
				formatter.format("file=%s&cmd=setattribute&name=%s&value=%s", file, name,value);
			else
				formatter.format("file=%s&cmd=setattribute&name=%s", file, name);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				if (root.getAttribute("status").equals("failed")) {
					System.out.println(root.getAttribute("msg"));
					System.exit(-1);
				}
				String status = root.getAttribute("status");
				String msg = root.getAttribute("msg");
				System.out.printf("Set Read Speed [%s] returned [%s]\n",
						status, msg);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
