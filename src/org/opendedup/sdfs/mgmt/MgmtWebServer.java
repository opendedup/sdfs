package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.rabin.utils.StringUtils;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.mgmt.websocket.DDBUpdate;
import org.opendedup.sdfs.mgmt.websocket.MetaDataUpdate;
import org.opendedup.sdfs.mgmt.websocket.MetaDataUpload;
import org.opendedup.sdfs.mgmt.websocket.PingService;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FindOpenPort;
import org.opendedup.util.KeyGenerator;
import org.opendedup.util.XMLUtils;
import org.simpleframework.common.buffer.FileAllocator;
import org.simpleframework.http.Path;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.simpleframework.http.core.ContainerSocketProcessor;
import org.simpleframework.http.socket.service.PathRouter;
import org.simpleframework.http.socket.service.Router;
import org.simpleframework.http.socket.service.RouterContainer;
import org.simpleframework.http.socket.service.Service;
import org.simpleframework.transport.SocketProcessor;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class MgmtWebServer implements Container {
	private static Connection connection = null;
	private static String archivePath = new File(Main.volume.getPath()).getParent() + File.separator + "archives";
	public static final String METADATA_PATH = "/metadata/";
	public static final String IO_PATH = "/io/";
	public static final String METADATA_INFO_PATH = "/metadatainfo/";
	public static final String MAPDATA_PATH = "/mapdata/";
	public static final String BLOCK_PATH = "/blockdata/";
	public static final String BATCH_BLOCK_PATH = "/batchblockdata/";
	public static final String BATCH_BLOCK_POINTER = "/batchblockpointer/";
	public static final String SESSION = "/session/";
	public static Io io = null;

	public static Map<String, String> splitQuery(String query) {

		Map<String, String> query_pairs = new LinkedHashMap<String, String>();
		if (query != null) {
			try {
				String[] pairs = query.split("&");
				for (String pair : pairs) {
					int idx = pair.indexOf("=");
					query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"),
							URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to parse " + query, e);
			}
		}
		return query_pairs;
	}

	private transient static LinkedHashMap<String, String> sessions = new LinkedHashMap<String, String>(Main.maxOpenFiles*2, .075F,
			false) {
		private static final long serialVersionUID = -1L;

		@Override
		protected boolean removeEldestEntry(Map.Entry<String, String> entry) {
			return size() > Main.maxOpenFiles*2;
		}
	};

	@Override
	public void handle(Request request, Response response) {
		try {
			//SDFSLogger.getLog().info(request.getTarget());
			Path reqPath = request.getPath();
			String[] parts = request.getTarget().split("\\?");
			Map<String, String> qry = null;
			if (parts.length > 1) {
				qry = splitQuery(parts[1]);
			} else {
				qry = splitQuery(null);
			}
			boolean cmdReq = reqPath.getPath().trim().equalsIgnoreCase("/");

			String file = null;

			if (qry.containsKey("file"))
				file = qry.get("file");
			String cmd = qry.get("cmd");
			if (cmd != null)
				cmd = cmd.toLowerCase();

			String cmdOptions = null;
			if (qry.containsKey("options"))
				cmdOptions = qry.get("options");
			SDFSLogger.getLog().debug("cmd=" + cmd + " file=" + file + " options=" + cmdOptions);

			boolean auth = false;
			if (request.getTarget().startsWith(SESSION)) {
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder builder;
				builder = factory.newDocumentBuilder();
				DOMImplementation impl = builder.getDOMImplementation();
				// Document.
				Document doc = impl.createDocument(null, "result", null);
				// Root element.
				String sessionid = HashFunctions.getRandomString(8);
				sessions.put(sessionid, request.getClientAddress().getHostString());
				Element result = doc.getDocumentElement();
				result.setAttribute("status", "success");
				result.setAttribute("session-id", sessionid);
				result.setAttribute("salt", Main.sdfsPasswordSalt);
				String rsString = XMLUtils.toXMLString(doc);

				// SDFSLogger.getLog().debug(rsString);
				response.setContentType("text/xml");
				byte[] rb = rsString.getBytes();
				response.setContentLength(rb.length);
				response.getOutputStream().write(rb);
				response.getOutputStream().flush();
				response.close();
				return;
			}
			if (Main.sdfsCliRequireAuth) {
				String password = qry.get("password");
				String hmac = qry.get("hmac");
				if (hmac != null) {
					String[] tks = URLDecoder.decode(hmac, "UTF-8").split(":");
					String hsh = tks[0];
					String session = tks[1];
					if (!sessions.containsKey(session))
						auth = false;
					else {
						String im = HashFunctions.getHmacSHA256(session, StringUtils.getHexBytes(Main.sdfsPassword));
						if (im.equalsIgnoreCase(hsh))
							auth = true;
					}
				} else if (password != null) {
					String hash = HashFunctions.getSHAHash(password.trim().getBytes(),
							Main.sdfsPasswordSalt.getBytes());
					if (hash.equals(Main.sdfsPassword))
						auth = true;
				} else {
					SDFSLogger.getLog().warn("could not authenticate user");
				}
			} else {
				auth = true;
			}
			if (cmdReq) {
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder builder;
				builder = factory.newDocumentBuilder();
				DOMImplementation impl = builder.getDOMImplementation();
				// Document.
				Document doc = impl.createDocument(null, "result", null);
				// Root element.
				Element result = doc.getDocumentElement();
				result.setAttribute("status", "failed");
				result.setAttribute("msg", "could not authenticate user");
				if (auth) {
					switch (cmd) {
					case "shutdown":
						new Shutdown().getResult();
						result.setAttribute("status", "success");
						result.setAttribute("msg", "shutting down volume manager");
						break;
					case "info":
						try {
							boolean shortList = false;
							if (qry.containsKey("short")) {
								shortList = Boolean.parseBoolean(qry.get("short"));
							}
							Element msg = new GetAttributes().getResult(cmdOptions, file, shortList);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("info", e);
						}
						break;
					case "setattribute":
						try {
							String name = qry.get("name");
							String value = null;
							if (qry.containsKey("value"))
								value = qry.get("value");
							SetFileAttribute.getResult(file, name, value);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("info", e);
						}
						break;
					case "ostevtgetseqnum":
						try {
							Element msg = OSTEventStore.getCurrentSeqNum();
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("info", e);
						}
						break;
					case "ostevtresseqnum":
						try {
							Element msg = OSTEventStore.reserverSeqNum();
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("info", e);
						}
						break;
					case "ostevtset":
						try {
							long seqnum = Long.parseLong(qry.get("num"));
							String data = qry.get("event");
							String payload = null;
							if (qry.containsKey("payload"))
								payload = qry.get("payload");
							OSTEventStore.AddOSTEvent(seqnum, data, payload);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");

						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("info", e);
						}
						break;
					case "ostevtsetpayload":
						try {
							long seqnum = Long.parseLong(qry.get("num"));
							String payload = qry.get("payload");
							OSTEventStore.SetOSTEventPayload(seqnum, payload);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");

						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("info", e);
						}
						break;
					case "ostevtget":
						try {
							long seqnum = Long.parseLong(qry.get("num"));
							Element msg = OSTEventStore.getOSTEvent(seqnum);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("info", e);
						}
						break;
					case "ostevtdelete":
						try {
							long seqnum = Long.parseLong(qry.get("num"));
							OSTEventStore.DeleteOSTEvent(seqnum);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");

						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("info", e);
						}
						break;
					case "ostevtgetall":
						try {
							Element msg = OSTEventStore.getOSTEvents();
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("info", e);
						}
						break;
					case "connectedvolumes":
						try {
							Element msg = new GetConnectedVolumes().getResult();
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("connectedvolumes", e);
						}
						break;
					case "syncvolume":
						try {
							new SyncFromConnectedVolume().getResult(Long.parseLong(qry.get("id")));
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("syncvolume", e);
						}
						break;
					case "deletevolume":
						try {
							new DeleteConnectedVolume().getResult(Long.parseLong(qry.get("id")));
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("deletevolume", e);
						}
						break;
					case "deletefile":
						try {
							String changeid = null;
							if (qry.containsKey("changeid")) {
								changeid = qry.get("changeid");
							}
							boolean rmlock = false;
							if (qry.containsKey("retentionlock"))
								rmlock = Boolean.parseBoolean(qry.get("retentionlock"));
							String msg = new DeleteFileCmd().getResult(cmdOptions, file, changeid, rmlock);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.createTextNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("deletefile", e);
						}
						break;
					case "cloudfile":
						try {

							String dstfile = null;
							if (qry.containsKey("dstfile")) {
								dstfile = qry.get("dstfile");
							}
							boolean overwrite = false;
							if (qry.containsKey("overwrite")) {
								overwrite = Boolean.parseBoolean(qry.get("overwrite"));
							}
							Element msg = new GetCloudFile().getResult(file, dstfile, overwrite);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("cloudfile", e);
						}
						break;
					case "cloudmfile":
						try {

							String dstfile = null;
							if (qry.containsKey("dstfile")) {
								dstfile = qry.get("dstfile");
							}
							String changeid = qry.get("changeid");
							Element msg = new GetCloudMetaFile().getResult(file, dstfile, changeid);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("cloudmfile", e);
						}
						break;/*
					case "clouddbfile":
						try {
							String changeid = qry.get("changeid");
							Element msg = new GetCloudDBFile().getResult(file, changeid);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("clouddbfile", e);
						}
						break;*/
					case "setcachesz":
						try {
							Element msg = new SetCacheSize().getResult(qry.get("sz"));
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "setreadspeed":
						try {
							Element msg = new SetReadSpeed().getResult(qry.get("sp"));
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("setcachesz", e);
						}
						break;
					case "syncfiles":

						try {
							Element msg = new SyncFSCmd().getResult();
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("syncfiles", e);
						}
						break;
					case "setwritespeed":
						try {
							Element msg = new SetWriteSpeed().getResult(qry.get("sp"));
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("setwritespeed", e);
						}
						break;
					case "deletearchive":

						try {
							String msg = new DeleteArchiveCmd().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.createTextNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("deletearchive", e);
						}
						break;
					case "makefolder":
						try {

							String msg = new MakeFolderCmd().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.createTextNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("makefolder", e);
						}
						break;
					case "copyextents":
						try {
							String srcfile = qry.get("srcfile");
							String dstfile = qry.get("dstfile");
							long sstart = Long.parseLong(qry.get("sstart"));
							long len = Long.parseLong(qry.get("len"));
							long dstart = Long.parseLong(qry.get("dstart"));

							Element msg = new CopyExtents().getResult(srcfile, dstfile, sstart, len, dstart);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("copyextents", e);
						}
						break;

					case "filteredinfo":
						try {
							boolean includeFiles = Boolean.parseBoolean(qry.get("includefiles"));
							boolean includeFolders = Boolean.parseBoolean(qry.get("includefolders"));
							int level = Integer.parseInt(qry.get("level"));
							Element msg = new GetFilteredFileAttributes().getResult(cmdOptions, file, includeFiles,
									includeFolders, level);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("filteredinfo", e);
						}
					case "dse-info":
						try {
							Element msg = new GetDSE().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("dse-info", e);
						}
						break;
					case "cluster-dse-info":
						try {
							Element msg = new GetClusterDSE().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("cluster-dse-info", e);
						}
						break;
					case "cluster-volumes":
						try {
							Element msg = new GetRemoteVolumes().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("cluster-volumes", e);
						}
						break;
					case "cluster-volume-remove":
						try {
							new RemoveRemoteVolume().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("cluster-volume-remove", e);
						}
						break;
					case "cluster-volume-add":
						try {
							new AddRemoteVolume().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("cluster-volume-add", e);
						}
						break;
					case "blockdev-add":
						try {
							Element el = new BlockDeviceAdd().getResult(qry.get("devname"), qry.get("size"),
									qry.get("start"));

							result.setAttribute("status", "success");
							result.setAttribute("msg", "successfully added block device [" + qry.get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("blockdev-add", e);
						}
						break;
					case "blockdev-rm":
						try {
							Element el = new BlockDeviceRm().getResult(qry.get("devname"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully removed block device [" + qry.get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("blockdev-rm", e);
						}
						break;
					case "blockdev-start":
						try {
							Element el = new BlockDeviceStart().getResult(qry.get("devname"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully started block device [" + qry.get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("blockdev-start", e);
						}
						break;
					case "blockdev-stop":
						try {
							Element el = new BlockDeviceStop().getResult(qry.get("devname"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully stopped block device [" + qry.get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("blockdev-stop", e);
						}
						break;
					case "blockdev-list":
						try {
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							List<Element> els = new BlockDeviceList().getResult();
							for (Element el : els) {
								result.appendChild(doc.adoptNode(el));
							}
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("blockdev-list", e);
						}
						break;
					case "blockdev-update":
						try {
							Element el = new BlockDeviceUpdate().getResult(qry.get("devname"), qry.get("param"),
									qry.get("value"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully updated block device [" + qry.get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("blockdev-update", e);
						}
						break;
					case "close-file":
						long fd = -1;
						if (qry.containsKey("fd"))
							fd = Long.parseLong(qry.get("fd"));
						boolean written = false;
						if (qry.containsKey("written"))
							written = Boolean.parseBoolean(qry.get("written"));
						new CloseFile().getResult(cmdOptions, written, file, fd);
						result.setAttribute("status", "success");
						result.setAttribute("msg", "command completed successfully");
						break;
					case "set-gc-schedule":
						try {
							new SetGCSchedule().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("set-gc-schedule", e);
						}
						break;
					case "get-gc-schedule":
						try {
							Element msg = new GetGCSchedule().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));

						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("get-gc-schedule", e);
						}
						break;
					case "get-gc-master":
						try {
							Element msg = new GetGCMaster().getResult();
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("get-gc-master", e);
						}
						break;

					case "cluster-promote-gc":
						try {
							new PromoteToGCMaster().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("cluster-promote-gc", e);
						}
						break;
					case "open-files":
						try {
							Element msg = new GetOpenFiles().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("open-files", e);
						}
						break;
					case "debug-info":
						try {
							Element msg = new GetDebug().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("debug-info", e);
						}
						break; /*
								 * else if (cmd.equalsIgnoreCase("events")) { try { String msg = new
								 * GetEvents().getResult(cmdOptions, file); result =
								 * "<result status=\"success\" msg=\"command completed successfully\">" ; result
								 * = result + msg; result = result + "</result>"; } catch (IOException e) {
								 * result.setAttribute("status", "failed"); result.setAttribute("msg",
								 * e.toString()); SDFSLogger.getLog().warn(e); } }
								 */
					case "volume-info":
						try {
							Element msg = new GetVolume().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("volume-info", e);
						}
						break;
					case "changepassword":
						try {
							String msg = new SetPasswordCmd().getResult("", qry.get("newpassword"));
							result.setAttribute("status", "success");
							result.setAttribute("msg", msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("changepassword", e);
						}
						break;
					case "snapshot":
						try {
							Element msg = new SnapshotCmd().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "snapshot finished successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("snapshot", e);
						}
						break;
					case "restorearchive":
						try {
							Element msg = new RestoreArchiveCmd().getResult(file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "replication finished successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("error", e);
						}
						break;
					case "importarchive":
						try {
							String server = qry.get("server");
							String password = qry.get("spasswd");
							int port = Integer.parseInt(qry.get("port"));
							boolean lz4 = false;
							int maxSz = 30;
							boolean useSSL = false;
							if (qry.containsKey("maxsz"))
								maxSz = Integer.parseInt(qry.get("maxsz"));
							if (qry.containsKey("useSSL"))
								useSSL = Boolean.parseBoolean(qry.get("useSSL"));
							if (qry.containsKey("uselz4"))
								lz4 = Boolean.parseBoolean(qry.get("uselz4"));
							Element msg = new ImportArchiveCmd().getResult(file, cmdOptions, server, password, port,
									maxSz, useSSL, lz4);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "replication started successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("importarchive", e);
						}
						break;
					case "importfile":
						try {
							String srcFile = qry.get("srcfile");
							String destFile = qry.get("dstfile");
							String server = qry.get("server");
							int maxsz = Integer.parseInt(qry.get("maxsz"));
							Element msg = new ImportFileCmd().getResult(srcFile, destFile, server, maxsz);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "replication started successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("importarchive", e);
						}
						break;
					case "batchgetblocks":
						byte[] rb = com.google.common.io.BaseEncoding.base64Url().decode(request.getParameter("data"));
						byte[] rslt = new BatchGetBlocksCmd().getResult(rb);
						long time = System.currentTimeMillis();
						response.setContentType("application/octet-stream");
						response.setValue("Server", "SDFS Management Server");
						response.setDate("Date", time);
						response.setDate("Last-Modified", time);
						response.getOutputStream().write(rslt);
						response.getOutputStream().flush();
						try {
							response.getOutputStream().close();
						} catch (Exception e) {
						}
						try {
							response.close();
						} catch (Exception e) {
						}
						break;
					case "cancelimport":
						try {
							String uuid = qry.get("uuid");
							result.setAttribute("status", "success");
							result.setAttribute("msg", new CancelImportArchiveCmd().getResult(uuid));
						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("cancelimport", e);
						}
						break;
					case "archiveout":
						try {
							boolean uselz4 = false;
							if (qry.containsKey("uselz4"))
								uselz4 = Boolean.parseBoolean(qry.get("uselz4"));
							Element msg = new ArchiveOutCmd().getResult(cmdOptions, file, uselz4);
							result.setAttribute("status", "success");
							result.setAttribute("msg", "archive out started successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("archiveout", e);
						}
						break;

					case "msnapshot":
						try {
							int snaps = Integer.parseInt(qry.get("snaps"));
							String msg = new MultiSnapshotCmd(snaps).getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("msnapshot", e);
						}
						break;
					case "flush":
						try {
							String msg = new FlushBuffersCmd().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "expandvolume":
						try {
							String size = qry.get("size");
							String msg = new ExpandVolumeCmd().getResult(cmdOptions, size);
							result.setAttribute("status", "success");
							result.setAttribute("msg", msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("expandvolume", e);
						}
						break;
					case "volumeconfigpath":
						try {
							result.setAttribute("status", "success");
							result.setAttribute("msg", Main.volume.getConfigPath());
						} catch (java.lang.NullPointerException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("volumeconfigpath", e);
						}
						break;
					case "dedup":
						try {
							String msg = new SetDedupAllCmd().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "perfmon":
						String msg = new SetEnablePerfMonCmd().getResult(cmdOptions, file);
						result.setAttribute("status", "success");
						result.setAttribute("msg", msg);
						break;
					case "cleanstore":
						try {
							Element emsg = new CleanStoreCmd().getResult();
							result.setAttribute("status", "success");
							doc.adoptNode(emsg);
							result.appendChild(emsg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("cleanstore", e);
						}
						break;
					case "fdisk":
						try {
							Element emsg = new FDISKCmd().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							doc.adoptNode(emsg);
							result.appendChild(emsg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("fdisk", e);
						}
						break;
					case "redundancyck":
						try {
							Element emsg = new ClusterRedundancyCmd().getResult(cmdOptions, null);
							result.setAttribute("status", "success");
							doc.adoptNode(emsg);
							result.appendChild(emsg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "event":
						try {
							String uuid = qry.get("uuid");
							Element emsg = new GetEvent().getResult(uuid);
							result.setAttribute("status", "success");
							doc.adoptNode(emsg);
							result.appendChild(emsg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("event", e);
						}
						break;
					case "events":
						try {
							Element emsg = new GetEvents().getResult();
							result.setAttribute("status", "success");
							doc.adoptNode(emsg);
							result.appendChild(emsg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn("events", e);
						}
						break;
					default:
						result.setAttribute("status", "failed");
						result.setAttribute("msg", "no command specified");
					}
					
				} else {
					result.setAttribute("status", "failed");
					result.setAttribute("msg", "authentication failed");
					response.setCode(403);
				}
				if (!cmd.equalsIgnoreCase("batchgetblocks")) {
					String rsString = XMLUtils.toXMLString(doc);

					// SDFSLogger.getLog().debug(rsString);
					response.setContentType("text/xml");
					byte[] rb = rsString.getBytes();
					response.setContentLength(rb.length);
					response.getOutputStream().write(rb);
					response.getOutputStream().flush();
					response.close();
				}
			} else {
				if (!auth) {
					PrintStream body = response.getPrintStream();
					response.setCode(403);
					body.println("authentication required");

				} else if (reqPath.getPath().contains("..")
						|| URLDecoder.decode(reqPath.getPath(), "UTF-8").contains("..")) {
					response.setCode(404);
					PrintStream body = response.getPrintStream();
					body.println("invalid path " + reqPath.getPath());
					SDFSLogger.getLog().error("invalid path " + reqPath.getPath());
					body.close();
				} else if (request.getTarget().startsWith(IO_PATH)) {
					String pth = request.getTarget().substring(IO_PATH.length()).split("\\?")[0];
					// SDFSLogger.getLog().info("io path=" + pth);
					io.processIo(request, response, pth);
				} else if (Main.matcher != null && request.getTarget().startsWith(Main.matcher.getWPath())) {
					long time = System.currentTimeMillis();
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					String guid = request.getTarget().substring(Main.matcher.getWPath().length());
					// SDFSLogger.getLog().info("path=" + request.getTarget());
					// SDFSLogger.getLog().info("guid=" + guid);
					guid = guid.split("\\?")[0];
					// SDFSLogger.getLog().info("guid=" + guid);
					String pth = guid.split("/")[0];
					// SDFSLogger.getLog().info("pth=" + pth);
					long start = Long.parseLong(guid.split("/")[1]);
					pth = URLDecoder.decode(pth, "UTF-8");
					Main.matcher.getResult(pth, start, response);
				} else if (request.getTarget().startsWith(METADATA_PATH)) {
					long time = System.currentTimeMillis();
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					String path = Main.volume.getPath() + File.separator
							+ request.getTarget().substring(METADATA_PATH.length());
					path = path.split("\\?")[0];
					path = URLDecoder.decode(path, "UTF-8");
					File f = new File(path);
					if (!f.exists()) {
						response.setCode(404);
						PrintStream body = response.getPrintStream();
						body.println("could not find path " + f.getPath());
						SDFSLogger.getLog().error("could not find path " + f.getPath());
						body.close();
					} else {
						MetaFileStore.getMF(path).sync();
						response.setContentLength(f.length());
						this.downloadFile(f, request, response);
					}

				} else if (request.getTarget().startsWith(METADATA_INFO_PATH)) {
					String path = Main.volume.getPath() + File.separator
							+ request.getTarget().substring(METADATA_INFO_PATH.length());

					path = path.split("\\?")[0];
					path = URLDecoder.decode(path, "UTF-8");
					File f = new File(path);
					if (!f.exists()) {
						response.setCode(404);
						PrintStream body = response.getPrintStream();
						body.println("could not find path " + f.getPath());
						SDFSLogger.getLog().error("could not find path " + f.getPath());
						body.close();
					} else {
						long time = System.currentTimeMillis();
						response.setContentType("application/json");
						response.setValue("Server", "SDFS Management Server");
						response.setDate("Date", time);
						response.setDate("Last-Modified", time);
						PrintStream body = response.getPrintStream();
						body.println(GetJSONAttributes.getResult(path));
						body.close();
					}
				} else if (request.getTarget().startsWith(MAPDATA_PATH)) {
					long time = System.currentTimeMillis();
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);

					String guid = request.getTarget().substring(MAPDATA_PATH.length());
					// SDFSLogger.getLog().info("path=" + request.getTarget());
					// SDFSLogger.getLog().info("guid=" + guid);
					guid = guid.split("\\?")[0];
					guid = URLDecoder.decode(guid, "UTF-8");
					String path = Main.dedupDBStore + File.separator + guid.substring(0, 2) + File.separator + guid
							+ File.separator + guid + ".map";
					// SDFSLogger.getLog().info("Downloading " + path);
					File f = new File(path);
					if (!f.exists()) {
						path = Main.dedupDBStore + File.separator + guid.substring(0, 2) + File.separator + guid
								+ File.separator + guid + ".map.lz4";

						response.setValue("metadatacomp", "true");
					} else {
						response.setValue("metadatacomp", "false");
					}
					f = new File(path);
					// SDFSLogger.getLog().info("Downloading " + path + " size="
					// + f.length());
					response.setContentLength(f.length());
					this.downloadFile(f, request, response);
				} else if (request.getTarget().startsWith(BLOCK_PATH)) {
					byte[] hash = com.google.common.io.BaseEncoding.base64Url()
							.decode(request.getTarget().substring(BLOCK_PATH.length()));
					byte[] data = HCServiceProxy.fetchHashChunk(hash).getData();
					long time = System.currentTimeMillis();
					response.setContentType("application/octet-stream");
					response.setValue("Server", "SDFS Management Server");
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					response.setContentLength(data.length);
					response.getByteChannel().write(ByteBuffer.wrap(data));
					response.getByteChannel().close();
				} else if (request.getTarget().startsWith(BATCH_BLOCK_PATH)) {
					byte[] rb = com.google.common.io.BaseEncoding.base64Url().decode(request.getParameter("data"));
					byte[] rslt = new BatchGetBlocksCmd().getResult(rb);
					long time = System.currentTimeMillis();
					response.setContentType("application/octet-stream");
					response.setValue("Server", "SDFS Management Server");
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					response.setContentLength(rslt.length);
					response.getByteChannel().write(ByteBuffer.wrap(rslt));
					response.getByteChannel().close();
				} else if (request.getTarget().startsWith(BATCH_BLOCK_POINTER)) {
					byte[] rb = com.google.common.io.BaseEncoding.base64Url().decode(request.getParameter("data"));
					byte[] rslt = new BatchGetPointerCmd().getResult(rb);
					long time = System.currentTimeMillis();
					response.setContentType("application/octet-stream");
					response.setValue("Server", "SDFS Management Server");
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					response.setContentLength(rslt.length);
					response.getByteChannel().write(ByteBuffer.wrap(rslt));
					response.getByteChannel().close();
				} else {

					File f = new File(archivePath + File.separator + reqPath.getPath());
					try {
						if (f.exists()) {
							long time = System.currentTimeMillis();
							response.setContentType("application/x-gtar");
							response.addValue("Server", "SDFS Management Server");
							response.setDate("Date", time);
							response.setDate("Last-Modified", time);
							response.setContentLength(f.length());
							InputStream in = new FileInputStream(f);
							OutputStream out = response.getOutputStream();
							byte[] buf = new byte[32768];
							int len;
							while ((len = in.read(buf)) > 0) {
								out.write(buf, 0, len);
							}
							in.close();
							out.close();
						} else {
							response.setCode(404);
							PrintStream body = response.getPrintStream();
							body.println("could not find " + reqPath);
							body.close();
						}
					} finally {
						f.delete();
					}
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to satify request ", e);
			response.setCode(500);
			try {
				PrintStream body = response.getPrintStream();
				body.println(e.toString());
				body.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				SDFSLogger.getLog().error("unable to satify request ", e1);
			}
			SDFSLogger.getLog().error("unable to satify request ", e);
		} finally {
			try {
				response.close();
			} catch (IOException e) {
				SDFSLogger.getLog().debug("error when closing response", e);
			}
		}
	}

	private void downloadFile(File f, Request request, Response response) throws IOException {
		if (f.exists()) {
			response.setContentType("application/octet-stream");
			response.addValue("Server", "SDFS Management Server");
			response.setContentLength(f.length());
			InputStream in = new FileInputStream(f);
			OutputStream out = response.getOutputStream();
			byte[] buf = new byte[32768];
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}
			out.flush();
			in.close();
			out.close();

		} else {
			response.setCode(404);
			PrintStream body = response.getPrintStream();
			body.println("could not find " + f.getPath());
			body.close();
			SDFSLogger.getLog().warn("unable to find " + f.getPath());
		}
	}

	public static void start(boolean useSSL) throws InvalidKeyException, KeyStoreException, NoSuchAlgorithmException,
			CertificateException, NoSuchProviderException, SignatureException, IOException, UnrecoverableKeyException,
			KeyManagementException {
		SSLContext sslContext = null;
		if (Main.sdfsCliEnabled) {
			if (useSSL) {
				String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
				String key = keydir + File.separator + "volume.keystore";
				if (!new File(key).exists()) {
					KeyGenerator.generateKey(new File(key));
				}
				FileInputStream keyFile = new FileInputStream(key);
				KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
				keyStore.load(keyFile, "sdfs".toCharArray());
				// init KeyManagerFactory
				KeyManagerFactory keyManagerFactory = KeyManagerFactory
						.getInstance(KeyManagerFactory.getDefaultAlgorithm());
				keyManagerFactory.init(keyStore, "sdfs".toCharArray());
				// init KeyManager
				sslContext = SSLContext.getInstance("TLSv1.2");
				// sslContext.init(keyManagerFactory.getKeyManagers(), new
				// TrustManager[]{new NaiveX509TrustManager()}, null);
				sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
			}
			Map<String, Service> routes = new HashMap<String, Service>();
			routes.put("/metadatasocket", new MetaDataUpdate());
			routes.put("/ddbsocket", new DDBUpdate());
			if (Main.matcher != null) {
				Main.matcher.start();
				routes.put(Main.matcher.getWSPath(), Main.matcher);
			}
			routes.put("/uploadsocket", new MetaDataUpload());
			routes.put("/ping", new PingService());
			Router negotiator = new PathRouter(routes, new PingService());
			io = new Io(Main.volume.getPath(), Main.volumeMountPoint);
			Container container = new MgmtWebServer();
			RouterContainer rn = new RouterContainer(container, negotiator, Main.writeThreads);
			SocketProcessor server = new ContainerSocketProcessor(rn, new FileAllocator(1024 * 1024 * 8),Main.writeThreads,4);
			connection = new SocketConnection(server);
			Main.sdfsCliPort = FindOpenPort.pickFreePort(Main.sdfsCliPort);
			SocketAddress address = new InetSocketAddress(Main.sdfsCliListenAddr, Main.sdfsCliPort);
			connection = new SocketConnection(server);
			if (sslContext != null)
				connection.connect(address, sslContext);
			else
				connection.connect(address);
			SDFSLogger.getLog().info("###################### SDFSCLI SSL Management WebServer Started at "
					+ address.toString() + " #########################");

		}
	}

	public static void stop() {
		try {
			connection.close();
		} catch (Throwable e) {
			SDFSLogger.getLog().error("unable to stop Web Management Server", e);
		}
	}

}
