package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;

import java.util.Formatter;

import org.opendedup.util.CommandLineProgressBar;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessFdisk {
	public static void runCmd(String path) {
		try {
			String file = URLEncoder.encode(path, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			System.out
					.printf("running fdisk\n");
			System.out.flush();
			formatter.format("file=%s&cmd=%s&options=%s", file, "fdisk",
					"1");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			Element sevt = (Element) root.getElementsByTagName("event").item(0);
			String uuid = sevt.getAttribute("uuid");
			boolean closed = false;
			CommandLineProgressBar bar = null;
			while (!closed) {

				sb = new StringBuilder();
				formatter = new Formatter(sb);
				formatter.format("file=%s&cmd=%s&options=%s&uuid=%s", file,
						"event", Integer.toString(1),
						URLEncoder.encode(uuid, "UTF-8"));
				doc = MgmtServerConnection.getResponse(sb.toString());
				root = doc.getDocumentElement();
				sevt = (Element) root.getElementsByTagName("event").item(0);
				
					if (bar == null) {
						long maxct = Long.parseLong(sevt
								.getAttribute("max-count"));
						bar = new CommandLineProgressBar(
								sevt.getAttribute("type"), maxct, System.out);
					}
					try {

						long pc = Long.parseLong(sevt
								.getAttribute("current-count"));
						bar.update(pc);
						if (!sevt.getAttribute("end-timestamp").equals("-1")) {
							System.out.println(sevt.getAttribute("type")
									+ " : " + sevt.getAttribute("short-msg"));
							closed = true;
						}

					} catch (Exception e) {
						e.printStackTrace();
						closed = true;
					}
				}
				
				// System.out.println(evt.getAttribute("level"));
				if (!closed)
					Thread.sleep(1000);

			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
