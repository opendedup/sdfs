package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.util.CommandLineProgressBar;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessClusterRedundancyCheck {
	public static void runCmd() {
		try {
			String file = URLEncoder.encode("null", "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			System.out.printf("Running cluster redundancy check");
			System.out.flush();
			formatter.format("file=%s&cmd=%s&options=%s", file, "redundancyck",
					"z");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			formatter.close();
			Element root = doc.getDocumentElement();
			Element evt = (Element) root.getElementsByTagName("event").item(0);
			String uuid = evt.getAttribute("uuid");
			boolean closed = false;
			CommandLineProgressBar bar = null;
			while (!closed) {
				sb = new StringBuilder();
				formatter = new Formatter(sb);
				formatter.format("file=%s&cmd=%s&options=%s&uuid=%s", file,
						"event", "z", URLEncoder.encode(uuid, "UTF-8"));
				doc = MgmtServerConnection.getResponse(sb.toString());
				root = doc.getDocumentElement();
				evt = (Element) root.getElementsByTagName("event").item(0);
				if (bar == null) {

					long maxct = Long.parseLong(evt.getAttribute("max-count"));
					bar = new CommandLineProgressBar(evt.getAttribute("type"),
							maxct, System.out);
				}
				try {

					long pc = Long.parseLong(evt.getAttribute("current-count"));
					bar.update(pc);

				} catch (Exception e) {
					e.printStackTrace();
				}
				if (!evt.getAttribute("end-timestamp").equals("-1")) {
					System.out.println(evt.getAttribute("type")
							+ " Task Completed : "
							+ evt.getAttribute("short-msg"));
					closed = true;
				}
				Thread.sleep(1000);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
