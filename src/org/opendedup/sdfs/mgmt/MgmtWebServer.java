package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
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
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.mgmt.websocket.DDBUpdate;
import org.opendedup.sdfs.mgmt.websocket.MetaDataUpdate;
import org.opendedup.sdfs.mgmt.websocket.MetaDataUpload;
import org.opendedup.sdfs.mgmt.websocket.PingService;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FindOpenPort;
import org.opendedup.util.KeyGenerator;
import org.opendedup.util.XMLUtils;
import org.simpleframework.http.Path;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.simpleframework.http.core.ContainerSocketProcessor;
import org.simpleframework.transport.SocketProcessor;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.simpleframework.http.socket.service.Router;
import org.simpleframework.http.socket.service.RouterContainer;
import org.simpleframework.http.socket.service.Service;
import org.simpleframework.http.socket.service.PathRouter;

public class MgmtWebServer implements Container {
	private static Connection connection = null;
	private static String archivePath = new File(Main.volume.getPath())
			.getParent() + File.separator + "archives";
	private static final String METADATA_PATH = "/metadata/";
	private static final String METADATA_INFO_PATH = "/metadatainfo/";
	private static final String MAPDATA_PATH = "/mapdata/";
	private static final String BLOCK_PATH = "/blockdata/";
	private static final String BATCH_BLOCK_PATH = "/batchblockdata/";
	private static final String BATCH_BLOCK_POINTER = "/batchblockpointer/";

	@Override
	public void handle(Request request, Response response) {
		try {
			Path reqPath = request.getPath();

			boolean cmdReq = reqPath.getPath().trim().equalsIgnoreCase("/");
			String file = request.getQuery().get("file");
			String cmd = request.getQuery().get("cmd").toLowerCase();
			String cmdOptions = request.getQuery().get("options");
			SDFSLogger.getLog().debug(
					"cmd=" + cmd + " file=" + file + " options=" + cmdOptions);
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder;
			builder = factory.newDocumentBuilder();
			DOMImplementation impl = builder.getDOMImplementation();
			// Document.
			Document doc = impl.createDocument(null, "result", null);
			// Root element.
			Element result = doc.getDocumentElement();
			result.setAttribute("status", "failed");
			result.setAttribute("msg", "could not authenticate user");
			boolean auth = false;
			if (Main.sdfsCliRequireAuth) {
				String password = request.getQuery().get("password");
				if (password != null) {
					String hash = HashFunctions.getSHAHash(password.trim()
							.getBytes(), Main.sdfsPasswordSalt.getBytes());
					if (hash.equals(Main.sdfsPassword))
						auth = true;
				} else {
					SDFSLogger.getLog().warn(
							"could not authenticate user to cli");
				}
			} else {
				auth = true;
			}
			if (cmdReq) {
				if (auth) {
					switch (cmd) {
					case "shutdown":
						new Shutdown().getResult();
						result.setAttribute("status", "success");
						result.setAttribute("msg",
								"shutting down volume manager");
						break;
					case "info":
						try {
							Element msg = new GetAttributes().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "connectedvolumes":
						try {
							Element msg = new GetConnectedVolumes().getResult();
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "syncvolume":
						try {
							new SyncFromConnectedVolume().getResult(Long
									.parseLong(request.getQuery().get("id")));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "deletefile":
						try {
							String changeid = null;
							if (request.getQuery().containsKey("changeid")) {
								changeid = request.getQuery().get("changeid");
							}
							String msg = new DeleteFileCmd().getResult(
									cmdOptions, file, changeid);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.createTextNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "cloudfile":
						try {

							String dstfile = null;
							if (request.getQuery().containsKey("dstfile")) {
								dstfile = request.getQuery().get("dstfile");
							}

							Element msg = new GetCloudFile().getResult(file,
									dstfile);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "cloudmfile":
						try {

							String dstfile = null;
							if (request.getQuery().containsKey("dstfile")) {
								dstfile = request.getQuery().get("dstfile");
							}
							String changeid = request.getQuery()
									.get("changeid");
							Element msg = new GetCloudMetaFile().getResult(
									file, dstfile, changeid);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "clouddbfile":
						try {
							String changeid = request.getQuery()
									.get("changeid");
							Element msg = new GetCloudDBFile().getResult(file,
									changeid);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "setcachesz":
						try {
							Element msg = new SetCacheSize().getResult(request
									.getQuery().get("sz"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "setreadspeed":
						try {
							Element msg = new SetReadSpeed().getResult(request
									.getQuery().get("sp"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "syncfiles":

						try {
							Element msg = new SyncFSCmd().getResult();
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "setwritespeed":
						try {
							Element msg = new SetWriteSpeed().getResult(request
									.getQuery().get("sp"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "deletearchive":

						try {
							String msg = new DeleteArchiveCmd().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.createTextNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "makefolder":
						try {

							String msg = new MakeFolderCmd().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.createTextNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "copyextents":
						try {
							String srcfile = request.getQuery().get("srcfile");
							String dstfile = request.getQuery().get("dstfile");
							long sstart = Long.parseLong(request.getQuery()
									.get("sstart"));
							long len = Long.parseLong(request.getQuery().get(
									"len"));
							long dstart = Long.parseLong(request.getQuery()
									.get("dstart"));

							Element msg = new CopyExtents().getResult(srcfile,
									dstfile, sstart, len, dstart);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;

					case "filteredinfo":
						try {
							boolean includeFiles = Boolean.parseBoolean(request
									.getQuery().get("includefiles"));
							boolean includeFolders = Boolean
									.parseBoolean(request.getQuery().get(
											"includefolders"));
							int level = Integer.parseInt(request.getQuery()
									.get("level"));
							Element msg = new GetFilteredFileAttributes()
									.getResult(cmdOptions, file, includeFiles,
											includeFolders, level);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					case "dse-info":
						try {
							Element msg = new GetDSE().getResult(cmdOptions,
									file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "cluster-dse-info":
						try {
							Element msg = new GetClusterDSE().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "cluster-volumes":
						try {
							Element msg = new GetRemoteVolumes().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "cluster-volume-remove":
						try {
							new RemoveRemoteVolume()
									.getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "cluster-volume-add":
						try {
							new AddRemoteVolume().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "blockdev-add":
						try {
							Element el = new BlockDeviceAdd().getResult(request
									.getQuery().get("devname"), request
									.getQuery().get("size"), request.getQuery()
									.get("start"));

							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully added block device ["
											+ request.getQuery().get("devname")
											+ "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "blockdev-rm":
						try {
							Element el = new BlockDeviceRm().getResult(request
									.getQuery().get("devname"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully removed block device ["
											+ request.getQuery().get("devname")
											+ "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "blockdev-start":
						try {
							Element el = new BlockDeviceStart()
									.getResult(request.getQuery()
											.get("devname"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully started block device ["
											+ request.getQuery().get("devname")
											+ "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "blockdev-stop":
						try {
							Element el = new BlockDeviceStop()
									.getResult(request.getQuery()
											.get("devname"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully stopped block device ["
											+ request.getQuery().get("devname")
											+ "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "blockdev-list":
						try {
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							List<Element> els = new BlockDeviceList()
									.getResult();
							for (Element el : els) {
								result.appendChild(doc.adoptNode(el));
							}
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "blockdev-update":
						try {
							Element el = new BlockDeviceUpdate().getResult(
									request.getQuery().get("devname"), request
											.getQuery().get("param"), request
											.getQuery().get("value"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully updated block device ["
											+ request.getQuery().get("devname")
											+ "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "close-file":
						long fd = -1;
						if(request.getQuery().containsKey("fd"))
							fd = Long.parseLong(request.getQuery().get("fd"));
						new CloseFile().getResult(cmdOptions, file,fd);
						result.setAttribute("status", "success");
						result.setAttribute("msg",
								"command completed successfully");
						break;

					case "set-gc-schedule":
						try {
							new SetGCSchedule().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "get-gc-schedule":
						try {
							Element msg = new GetGCSchedule().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));

						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "get-gc-master":
						try {
							Element msg = new GetGCMaster().getResult();
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;

					case "cluster-promote-gc":
						try {
							new PromoteToGCMaster().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "open-files":
						try {
							Element msg = new GetOpenFiles().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "debug-info":
						try {
							Element msg = new GetDebug().getResult(cmdOptions,
									file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break; /*
								 * else if (cmd.equalsIgnoreCase("events")) {
								 * try { String msg = new
								 * GetEvents().getResult(cmdOptions, file);
								 * result =
								 * "<result status=\"success\" msg=\"command completed successfully\">"
								 * ; result = result + msg; result = result +
								 * "</result>"; } catch (IOException e) {
								 * result.setAttribute("status", "failed");
								 * result.setAttribute("msg", e.toString());
								 * SDFSLogger.getLog().warn(e); } }
								 */
					case "volume-info":
						try {
							Element msg = new GetVolume().getResult(cmdOptions,
									file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "changepassword":
						try {
							String msg = new SetPasswordCmd().getResult("",
									request.getQuery().get("newpassword"));
							result.setAttribute("status", "success");
							result.setAttribute("msg", msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "snapshot":
						try {
							Element msg = new SnapshotCmd().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"snapshot finished successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "restorearchive":
						try {
							Element msg = new RestoreArchiveCmd()
									.getResult(file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"replication finished successfully");
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
							String server = request.getQuery().get("server");
							String password = request.getQuery().get("spasswd");
							int port = Integer.parseInt(request.getQuery().get(
									"port"));
							boolean lz4 = false;
							int maxSz = 30;
							boolean useSSL = false;
							if (request.getQuery().containsKey("maxsz"))
								maxSz = Integer.parseInt(request.getQuery()
										.get("maxsz"));
							if (request.getQuery().containsKey("useSSL"))
								useSSL = Boolean.parseBoolean(request
										.getQuery().get("useSSL"));
							if (request.getQuery().containsKey("uselz4"))
								lz4 = Boolean.parseBoolean(request.getQuery()
										.get("uselz4"));
							Element msg = new ImportArchiveCmd().getResult(
									file, cmdOptions, server, password, port,
									maxSz, useSSL, lz4);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"replication started successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "batchgetblocks":
						byte[] rb = com.google.common.io.BaseEncoding
								.base64Url().decode(
										request.getParameter("data"));
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
							String uuid = request.getQuery().get("uuid");
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									new CancelImportArchiveCmd()
											.getResult(uuid));
						} catch (Exception e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "archiveout":
						try {
							boolean uselz4 = false;
							if (request.getQuery().containsKey("uselz4"))
								uselz4 = Boolean.parseBoolean(request
										.getQuery().get("uselz4"));
							Element msg = new ArchiveOutCmd().getResult(
									cmdOptions, file, uselz4);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"archive out started successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;

					case "msnapshot":
						try {
							int snaps = request.getQuery().getInteger("snaps");
							String msg = new MultiSnapshotCmd(snaps).getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "flush":
						try {
							String msg = new FlushBuffersCmd().getResult(
									cmdOptions, file);
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
							String size = request.getQuery().get("size");
							String msg = new ExpandVolumeCmd().getResult(
									cmdOptions, size);
							result.setAttribute("status", "success");
							result.setAttribute("msg", msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "volumeconfigpath":
						try {
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									Main.volume.getConfigPath());
						} catch (java.lang.NullPointerException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "dedup":
						try {
							String msg = new SetDedupAllCmd().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg", msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "perfmon":
						String msg = new SetEnablePerfMonCmd().getResult(
								cmdOptions, file);
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
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "fdisk":
						try {
							Element emsg = new FDISKCmd().getResult(cmdOptions,
									file);
							result.setAttribute("status", "success");
							doc.adoptNode(emsg);
							result.appendChild(emsg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
						break;
					case "redundancyck":
						try {
							Element emsg = new ClusterRedundancyCmd()
									.getResult(cmdOptions, null);
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
							String uuid = request.getQuery().get("uuid");
							Element emsg = new GetEvent().getResult(uuid);
							result.setAttribute("status", "success");
							doc.adoptNode(emsg);
							result.appendChild(emsg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
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
							SDFSLogger.getLog().warn(e);
						}
						break;
					default:
						result.setAttribute("status", "failed");
						result.setAttribute("msg", "no command specified");
					}
					if (!cmd.equalsIgnoreCase("batchgetblocks")) {
						String rsString = XMLUtils.toXMLString(doc);
						PrintStream body = response.getPrintStream();
						long time = System.currentTimeMillis();

						// SDFSLogger.getLog().debug(rsString);
						response.setContentType("text/xml");
						response.addValue("Server", "SDFS Management Server");
						response.setDate("Date", time);
						response.setDate("Last-Modified", time);
						body.println(rsString);
						body.close();
					}
				}
			} else {
				if (!auth) {
					PrintStream body = response.getPrintStream();
					response.setCode(403);
					body.println("authentication required");

				} else if (reqPath.getPath().contains("..")) {
					response.setCode(404);
					PrintStream body = response.getPrintStream();
					body.println("could not find " + reqPath);
					body.close();
				} else if (request.getTarget().startsWith(METADATA_PATH)) {
					String path = Main.volume.getPath()
							+ File.separator
							+ request.getTarget().substring(
									METADATA_PATH.length());
					File f = new File(path);
					this.downloadFile(f, request, response);

				} else if (request.getTarget().startsWith(METADATA_INFO_PATH)) {
					String path = Main.volume.getPath()
							+ File.separator
							+ request.getTarget().substring(
									METADATA_INFO_PATH.length());
					long time = System.currentTimeMillis();
					response.setContentType("application/json");
					response.setValue("Server", "SDFS Management Server");
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					PrintStream body = response.getPrintStream();
					body.println(GetJSONAttributes.getResult(path));
					body.close();
				} else if (request.getTarget().startsWith(MAPDATA_PATH)) {
					String guid = request.getTarget().substring(
							MAPDATA_PATH.length());
					String path = Main.dedupDBStore + File.separator
							+ guid.substring(0, 2) + File.separator + guid;
					File f = new File(path);
					this.downloadFile(f, request, response);
				} else if (request.getTarget().startsWith(BLOCK_PATH)) {
					byte[] hash = com.google.common.io.BaseEncoding.base64Url()
							.decode(request.getTarget().substring(
									BLOCK_PATH.length()));
					byte[] data = HCServiceProxy.fetchHashChunk(hash).getData();
					long time = System.currentTimeMillis();
					response.setContentType("application/octet-stream");
					response.setValue("Server", "SDFS Management Server");
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					response.getByteChannel().write(ByteBuffer.wrap(data));
					response.getByteChannel().close();
				} else if (request.getTarget().startsWith(BATCH_BLOCK_PATH)) {
					byte[] rb = com.google.common.io.BaseEncoding.base64Url()
							.decode(request.getParameter("data"));
					byte[] rslt = new BatchGetBlocksCmd().getResult(rb);
					long time = System.currentTimeMillis();
					response.setContentType("application/octet-stream");
					response.setValue("Server", "SDFS Management Server");
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					response.getByteChannel().write(ByteBuffer.wrap(rslt));
					response.getByteChannel().close();
				} else if (request.getTarget().startsWith(BATCH_BLOCK_POINTER)) {
					byte[] rb = com.google.common.io.BaseEncoding.base64Url()
							.decode(request.getParameter("data"));
					byte[] rslt = new BatchGetPointerCmd().getResult(rb);
					long time = System.currentTimeMillis();
					response.setContentType("application/octet-stream");
					response.setValue("Server", "SDFS Management Server");
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					response.getByteChannel().write(ByteBuffer.wrap(rslt));
					response.getByteChannel().close();
				} else {

					File f = new File(archivePath + File.separator
							+ reqPath.getPath());
					try {
						if (f.exists()) {
							long time = System.currentTimeMillis();
							response.setContentType("application/x-gtar");
							response.addValue("Server",
									"SDFS Management Server");
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

	private void downloadFile(File f, Request request, Response response)
			throws IOException {
		if (f.exists()) {
			long time = System.currentTimeMillis();
			response.setContentType("application/octet-stream");
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
			body.println("could not find " + f.getPath());
			body.close();
		}
	}

	public static void start(boolean useSSL) throws InvalidKeyException,
			KeyStoreException, NoSuchAlgorithmException, CertificateException,
			NoSuchProviderException, SignatureException, IOException,
			UnrecoverableKeyException, KeyManagementException {
		SSLContext sslContext = null;
		if (Main.sdfsCliEnabled) {
			if (useSSL) {
				String keydir = new File(Main.volume.getPath()).getParent()
						+ File.separator + "keys";
				String key = keydir + File.separator + "volume.keystore";
				if (!new File(key).exists()) {
					KeyGenerator.generateKey(new File(key));
				}
				FileInputStream keyFile = new FileInputStream(key);
				KeyStore keyStore = KeyStore.getInstance(KeyStore
						.getDefaultType());
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
			routes.put("/uploadsocket", new MetaDataUpload());
			routes.put("/ping", new PingService());
			Router negotiator = new PathRouter(routes, new PingService());
			Container container = new MgmtWebServer();
			RouterContainer rn = new RouterContainer(container, negotiator, 10);
			SocketProcessor server = new ContainerSocketProcessor(rn, 24, 3);
			connection = new SocketConnection(server);
			Main.sdfsCliPort = FindOpenPort.pickFreePort(Main.sdfsCliPort);
			SocketAddress address = new InetSocketAddress(
					Main.sdfsCliListenAddr, Main.sdfsCliPort);
			connection = new SocketConnection(server);
			if (sslContext != null)
				connection.connect(address, sslContext);
			else
				connection.connect(address);
			SDFSLogger
					.getLog()
					.info("###################### SDFSCLI SSL Management WebServer Started at "
							+ address.toString() + " #########################");

		}
	}

	public static void stop() {
		try {
			connection.close();
		} catch (Throwable e) {
			SDFSLogger.getLog()
					.error("unable to stop Web Management Server", e);
		}
	}

}
