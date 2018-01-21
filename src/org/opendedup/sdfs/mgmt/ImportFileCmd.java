package org.opendedup.sdfs.mgmt;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.IOUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.mgmt.cli.MgmtServerConnection;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.replication.MetaFileImport;
import org.w3c.dom.Element;

public class ImportFileCmd implements Runnable {
	String srcFile;
	String destFile;
	String server;
	String password;
	int port;
	int maxSz = 0;
	boolean useSSL = false;
	boolean hmac = false;
	SDFSEvent evt;

	public Element getResult(String srcFile, String destFile, String serverURL, int maxSz)
			throws IOException, URISyntaxException {
		URL url = new URL(serverURL);
		server = url.getHost();
		port = url.getPort();
		if (url.getProtocol().equalsIgnoreCase("https"))
			this.useSSL = true;
		this.srcFile = srcFile;
		this.destFile = destFile;
		this.maxSz = maxSz;
		SDFSLogger.getLog().info("connecting to " +serverURL + " srcfile=" + srcFile + " destFile=" + destFile);
		@SuppressWarnings("deprecation")
		List<NameValuePair> params = URLEncodedUtils.parse(new URI(serverURL), "UTF-8");
		for (NameValuePair param : params) {
			if (param.getName().equalsIgnoreCase("hmac")) {
				this.password = param.getValue();
				this.hmac = true;
			}
			if (param.getName().equalsIgnoreCase("password")) {
				this.password = MgmtServerConnection.initAuth(param.getValue(),server,port,this.useSSL);
				this.hmac = true;
			}
		}

		return importArchive();
	}

	private Element importArchive() throws IOException {
		evt = SDFSEvent.importEvent("Importing " + srcFile + " from " + server + ":" + port + " to " + destFile);
		evt.curCt = 0;
		evt.maxCt = 3;
		Thread th = new Thread(this);
		th.start();
		try {
			return evt.toXML();
		} catch (ParserConfigurationException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void run() {
		String sc = " not successful";
		MetaDataDedupFile mf = null;
		try {
			evt.shortMsg = "Importing " + this.srcFile + " to " + this.destFile;
			Map<String,String> axa = null;
			if(new File(this.destFile).exists()) {
				MetaDataDedupFile _mf = MetaFileStore.getMF(this.destFile);
				Map<String,String> xa = _mf.getExtendedAttributes();
				if(xa != null && xa.containsKey("netbackup.data")) {
					axa = new HashMap<String,String>();
					axa.putAll(xa);
				}
			}
			MetaFileStore.removeMetaFile(this.destFile);
			mf = downloadMetaFile();
			if(axa != null)
				mf.getExtendedAttributes().putAll(axa);
			evt.curCt++;
			evt.shortMsg = "Importing map for " + this.destFile;
			String ng = downloadDDB(mf.getDfGuid());
			mf.setDfGuid(ng);
			mf.sync();
			MetaFileStore.removedCachedMF(mf.getPath());
			MetaFileStore.addToCache(mf);
			evt.curCt++;
			MetaFileImport mi = new MetaFileImport(mf.getPath(), server, password, port, maxSz, evt, useSSL);
			mi.runImport();
			evt.endEvent("import of " + this.destFile + " was successful");
			sc = "successful";
		} catch (Throwable e) {
			if (mf != null) {
				mf.clearRetentionLock();
				MetaFileStore.removeMetaFile(mf.getPath());
				mf = null;
			}
			evt.endEvent("Unable to import archive [" + srcFile + "] " + "Destination [" + destFile + "]",
					SDFSEvent.ERROR, e);
			SDFSLogger.getLog().error("Unable to import archive [" + srcFile + "] " + "Destination [" + destFile
					+ "] because :" + e.toString(), e);

		} finally {
			SDFSLogger.getLog().info("Exited Replication task [" + sc + "]");
		}

	}

	private MetaDataDedupFile downloadMetaFile() throws Exception {
		String fp = MgmtWebServer.METADATA_PATH + URLEncoder.encode(this.srcFile, "UTF-8");
		GetMethod mtd = null;
		try {
			if (hmac) {
				String url = MgmtServerConnection.createAuthUrl("", password);
				mtd = MgmtServerConnection.connectAndGetHMAC(server, port, url, fp, useSSL);
			}
			else
				mtd = MgmtServerConnection.connectAndGet(server, port, password, "", fp, useSSL);
			BufferedInputStream bis = new BufferedInputStream(mtd.getResponseBodyAsStream());
			String pt = Main.volume.getPath() + File.separator + this.destFile;
			File _f = new File(pt);
			/*
			 * if(_f.exists()) { MetaDataDedupFile _mf = MetaFileStore.getMF(_f);
			 * _mf.clearRetentionLock(); MetaFileStore.removeMetaFile(pt); }
			 */
			BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(_f));
			MetaFileStore.removedCachedMF(_f.getPath());
			IOUtils.copy(bis, bout);
			IOUtils.closeQuietly(bis);
			IOUtils.closeQuietly(bout);
			MetaFileStore.removedCachedMF(_f.getPath());
			return MetaFileStore.getMF(_f);
		} finally {
			if (mtd != null) {
				try {
					mtd.releaseConnection();
				} catch (Exception e) {
				}
			}
		}

	}

	private String downloadDDB(String guid) throws Exception {
		SDFSLogger.getLog().info("getting " + guid);
		String fp = MgmtWebServer.MAPDATA_PATH + guid;
		GetMethod mtd = null;
		try {
			if (hmac) {
					String url = MgmtServerConnection.createAuthUrl("", password);
					mtd = MgmtServerConnection.connectAndGetHMAC(server, port, url, fp, useSSL);
			}
				
			else
				mtd = MgmtServerConnection.connectAndGet(server, port, password, "", fp, useSSL);
			BufferedInputStream bis = new BufferedInputStream(mtd.getResponseBodyAsStream());
			String ng = UUID.randomUUID().toString();
			String path = Main.dedupDBStore + File.separator + ng.substring(0, 2) + File.separator + ng
					+ File.separator;
			File gd = new File(path);
			gd.mkdirs();
			boolean comp = Boolean.parseBoolean(mtd.getResponseHeader("metadatacomp").getValue());
			String pt = path + ng + ".map";
			if (comp)
				pt = pt + ".lz4";
			File _f = new File(pt);
			BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(_f));
			IOUtils.copy(bis, bout);
			IOUtils.closeQuietly(bis);
			IOUtils.closeQuietly(bout);
			SDFSLogger.getLog().info("downloaded " + _f.getPath() + " size=" + _f.length());
			return ng;
		} finally {
			if (mtd != null) {
				try {
					mtd.releaseConnection();
				} catch (Exception e) {
				}
			}
		}

	}

}
