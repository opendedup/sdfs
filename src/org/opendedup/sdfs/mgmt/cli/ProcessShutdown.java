package org.opendedup.sdfs.mgmt.cli;

import java.io.IOException;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessShutdown {
	public static void runCmd() {
		try {

			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=shutdown", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			System.out.println(root.getAttribute("msg"));
			formatter.close();

		} catch(IOException e) {
			if(e.getCause() != null && e.getCause().getMessage().equalsIgnoreCase("Connection reset"))
				System.out.println("Volume Shut Down");
			else
				e.printStackTrace();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
