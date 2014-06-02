package org.opendedup.sdfs.mgmt.cli;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.Formatter;

import org.apache.commons.httpclient.methods.GetMethod;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.util.CommandLineProgressBar;
import org.opendedup.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessArchiveOutCmd {
	public static String runCmd(String file, String dir) throws IOException {
		SDFSLogger.getLog().debug("archive a copy of [" + file + "]");
		file = URLEncoder.encode(file, "UTF-8");
		StringBuilder sb = new StringBuilder();
		Formatter formatter = new Formatter(sb);
		formatter.format("file=%s&cmd=archiveout&options=ilovemg", file);
		formatter.close();
		Document doc = MgmtServerConnection.getResponse(sb.toString());
		Element root = doc.getDocumentElement();
		Element evt = (Element) root.getElementsByTagName("event").item(0);
		File f = new File(evt.getAttribute("extended-info"));
		String uuid = evt.getAttribute("uuid");
		long maxcount = Long.parseLong(evt.getAttribute("max-count"));
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
			formatter.format("file=%s&cmd=%s&options=%s&uuid=%s", file,
					"event", Integer.toString(0),
					URLEncoder.encode(uuid, "UTF-8"));
			formatter.close();
			doc = MgmtServerConnection.getResponse(sb.toString());
			root = doc.getDocumentElement();
			evt = (Element) root.getElementsByTagName("event").item(0);

			if (le != evt.getElementsByTagName("event").getLength())
				System.out.println();
			le = evt.getElementsByTagName("event").getLength();
			if (le > 0) {
				Element sevt = (Element) evt.getElementsByTagName("event")
						.item(curevt);
				try {
					if (!sevt.getAttribute("uuid").equalsIgnoreCase(
							currentEvent)) {
						sevt = (Element) evt.getElementsByTagName("event")
								.item(curevt);
						currentEvent = sevt.getAttribute("uuid");
						long maxct = Long.parseLong(sevt
								.getAttribute("max-count"));
						bar = new CommandLineProgressBar(
								sevt.getAttribute("type"), maxct, System.out);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					try {
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
						curevt++;
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
		GetMethod m = null;
		InputStream in = null;
		try {
			m = MgmtServerConnection.connectAndGet("", f.getName());
			in = m.getResponseBodyAsStream();
			File nf = new File(dir + File.separator + f.getName());
			if (!nf.getParentFile().exists())
				nf.getParentFile().mkdirs();
			FileOutputStream out = new FileOutputStream(nf.getPath());
			byte[] buf = new byte[32768];
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}
			in.close();
			out.close();
			m.releaseConnection();
			return nf.getPath();
		} finally {
			if (in != null)
				in.close();
			if (m != null)
				m.releaseConnection();
		}

	}

}
