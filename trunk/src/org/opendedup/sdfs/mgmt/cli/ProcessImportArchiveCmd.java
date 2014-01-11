package org.opendedup.sdfs.mgmt.cli;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Formatter;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.util.CommandLineProgressBar;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessImportArchiveCmd {
	public static void runCmd(String archive, String path, String server,
			String password, int port, boolean quiet, int maxSz)
			throws IOException {
		SDFSLogger.getBasicLog().debug(
				"importing [" + archive + "] destination is [" + path + "]"
						+ " server is [" + server + "] server password is ["
						+ password + "] server port is [" + port + "]");
		archive = URLEncoder.encode(archive, "UTF-8");
		path = URLEncoder.encode(path, "UTF-8");
		server = URLEncoder.encode(server, "UTF-8");
		password = URLEncoder.encode(password, "UTF-8");
		StringBuilder sb = new StringBuilder();
		Formatter formatter = new Formatter(sb);
		formatter
				.format("file=%s&cmd=importarchive&options=%s&server=%s&spasswd=%s&port=%s&maxsz=%s",
						archive, path, server, password,
						Integer.toString(port), Integer.toString(maxSz));
		Document doc = MgmtServerConnection.getResponse(sb.toString());
		Element root = doc.getDocumentElement();
		Element evt = (Element) root.getElementsByTagName("event").item(0);
		formatter.close();
		String uuid = evt.getAttribute("uuid");
		long maxcount = Long.parseLong(evt.getAttribute("max-count"));
		long smaxcount = 0;
		if (root.getAttribute("status").equals("failed"))
			SDFSLogger.getBasicLog().error("msg");
		CommandLineProgressBar bar = null;
		bar = new CommandLineProgressBar(evt.getAttribute("type"), maxcount,
				System.out);
		boolean closed = false;
		String currentEvent = evt.getAttribute("uuid");
		int le = 0;
		int curevt = 0;
		while (!closed) {
			sb = new StringBuilder();
			formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=%s&options=%s&uuid=%s", archive,
					"event", Integer.toString(0),
					URLEncoder.encode(uuid, "UTF-8"));
			doc = MgmtServerConnection.getResponse(sb.toString());
			formatter.close();
			root = doc.getDocumentElement();
			evt = (Element) root.getElementsByTagName("event").item(0);
			long nmc = Long.parseLong(evt.getAttribute("max-count"));
			if (nmc != maxcount) {
				maxcount = nmc;
				bar = new CommandLineProgressBar(evt.getAttribute("type"),
						maxcount, System.out);
			}

			if (le != evt.getElementsByTagName("event").getLength())
				System.out.println();
			le = evt.getElementsByTagName("event").getLength();
			if (le > 0) {
				curevt = evt.getElementsByTagName("event").getLength() - 1;
				Element sevt = (Element) evt.getElementsByTagName("event")
						.item(curevt);
				try {
					if (!sevt.getAttribute("uuid").equalsIgnoreCase(
							currentEvent)) {
						sevt = (Element) evt.getElementsByTagName("event")
								.item(curevt);
						currentEvent = sevt.getAttribute("uuid");
						smaxcount = Long.parseLong(sevt
								.getAttribute("max-count"));
						bar = new CommandLineProgressBar(
								sevt.getAttribute("type"), smaxcount,
								System.out);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					try {
						long _nmc = Long.parseLong(sevt
								.getAttribute("max-count"));
						if (_nmc != smaxcount) {
							smaxcount = _nmc;
							bar = new CommandLineProgressBar(
									sevt.getAttribute("type"), smaxcount,
									System.out);
						}
						long pc = Long.parseLong(sevt
								.getAttribute("current-count"));
						bar.update(pc);
					} catch (Exception e) {
						System.out.println(XMLUtils.toXMLString(doc));
					}
					if (!sevt.getAttribute("end-timestamp").equals("-1")
							&& evt.getElementsByTagName("event").getLength() > curevt) {
						System.out.println(sevt.getAttribute("type") + " : "
								+ sevt.getAttribute("short-msg"));
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
						|| evt.getAttribute("level")
								.equalsIgnoreCase("running"))
					System.out.println(evt.getAttribute("type")
							+ " Task Completed : "
							+ evt.getAttribute("short-msg"));
				else {
					System.err
							.println(evt.getAttribute("type")
									+ " Task Failed : "
									+ evt.getAttribute("short-msg"));
					System.exit(-1);
				}
				closed = true;
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

}
