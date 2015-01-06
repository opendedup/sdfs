package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;

import java.util.Formatter;

import org.opendedup.util.CommandLineProgressBar;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessRestoreArchiveCmd {
	public static void runCmd(String file) {
		try {
			System.out.printf("restoring[%s] from archive. This can take 4 or more hours.\n",
					file);
			file = URLEncoder.encode(file, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);

			formatter.format("file=%s&cmd=restorearchive&options=s", file);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			Element evt = (Element) root.getElementsByTagName("event").item(0);
			String uuid = evt.getAttribute("uuid");
			long maxcount = Long.parseLong(evt.getAttribute("max-count"));
			CommandLineProgressBar bar = null;
			bar = new CommandLineProgressBar(evt.getAttribute("type"),
					maxcount, System.out);
			boolean closed = false;
			int le = 0;
			int curevt = 0;
			while (!closed) {
				sb = new StringBuilder();
				formatter = new Formatter(sb);
				formatter.format("file=%s&cmd=%s&options=%s&uuid=%s", file,
						"event", Integer.toString(0),
						URLEncoder.encode(uuid, "UTF-8"));
				doc = MgmtServerConnection.getResponse(sb.toString());
				root = doc.getDocumentElement();
				evt = (Element) root.getElementsByTagName("event").item(0);
				if(Long.parseLong(evt.getAttribute("max-count")) == maxcount && bar==null) {
					bar = new CommandLineProgressBar(evt.getAttribute("type"),
							maxcount, System.out);
				}else {
					maxcount = Long.parseLong(evt.getAttribute("max-count"));
				}
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
							System.out.println();
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
						e.printStackTrace();
					}
				} else {
					long pc = Long.parseLong(evt.getAttribute("current-count"));
					bar.update(pc);
				}
				if (!evt.getAttribute("end-timestamp").equals("-1")) {
					if (evt.getAttribute("level").equalsIgnoreCase("info")
							|| evt.getAttribute("level").equalsIgnoreCase(
									"running")) {
						System.out.println();
						System.out.println(evt.getAttribute("type")
								+ " Task Completed : "
								+ evt.getAttribute("short-msg"));
						System.exit(0);
					}
					else {
						System.err.println(evt.getAttribute("type")
								+ " Task Failed : "
								+ evt.getAttribute("short-msg"));
						System.exit(-1);
					}
					closed = true;
				}
				Thread.sleep(15000);

			}
			formatter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
