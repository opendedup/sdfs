package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.CommandLineProgressBar;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessCleanStore {
	public static void runCmd(int minutes) {
		try {
			String file = URLEncoder.encode("null", "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			System.out
					.printf("Cleaning store of data older that [%d] minutes\n",
							minutes);
			System.out.flush();
			formatter.format("file=%s&cmd=%s&options=%s", file, "cleanstore",
					Integer.toString(minutes));
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			Element evt = (Element) root.getElementsByTagName("event").item(0);
			String uuid = evt.getAttribute("uuid");
			boolean closed = false;
			int le = 0;
			int curevt = 0;
			CommandLineProgressBar bar = null;
			while (!closed) {

				sb = new StringBuilder();
				formatter = new Formatter(sb);
				formatter.format("file=%s&cmd=%s&options=%s&uuid=%s", file,
						"event", Integer.toString(minutes),
						URLEncoder.encode(uuid, "UTF-8"));
				doc = MgmtServerConnection.getResponse(sb.toString());
				root = doc.getDocumentElement();
				evt = (Element) root.getElementsByTagName("event").item(0);
				if (le != evt.getElementsByTagName("event").getLength())
					System.out.println();
				le = evt.getElementsByTagName("event").getLength();
				if (le > 0) {
					Element sevt = (Element) evt.getElementsByTagName("event")
							.item(curevt);
					if (bar == null) {
						sevt = (Element) evt.getElementsByTagName("event")
								.item(curevt);
						long maxct = Long.parseLong(sevt
								.getAttribute("max-count"));
						bar = new CommandLineProgressBar(
								sevt.getAttribute("type"), maxct, System.out);
					}
					try {

						long pc = Long.parseLong(sevt
								.getAttribute("current-count"));
						bar.update(pc);
						if (!sevt.getAttribute("end-timestamp").equals("-1")
								&& evt.getElementsByTagName("event")
										.getLength() > curevt) {
							System.out.println(sevt.getAttribute("type")
									+ " : " + sevt.getAttribute("short-msg"));
							curevt++;
							try {
								sevt = (Element) evt.getElementsByTagName(
										"event").item(curevt);
								long maxct = Long.parseLong(sevt
										.getAttribute("max-count"));
								bar = new CommandLineProgressBar(
										sevt.getAttribute("type"), maxct,
										System.out);
							} catch (NullPointerException e) {
							}
						}

					} catch (Exception e) {
						// e.printStackTrace();
					}
				}
				if (!evt.getAttribute("end-timestamp").equals("-1")) {
					if (evt.getAttribute("level").equals(
							SDFSEvent.INFO.toString()))
						System.out.println(evt.getAttribute("type")
								+ " Task Completed Successfully : "
								+ evt.getAttribute("short-msg"));
					else
						System.out.println(evt.getAttribute("type")
								+ " Task Failed : "
								+ evt.getAttribute("short-msg"));
					closed = true;
				}
				// System.out.println(evt.getAttribute("level"));
				if (!closed)
					Thread.sleep(1000);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
