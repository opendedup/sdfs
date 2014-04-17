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
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.FindOpenPort;
import org.opendedup.util.KeyGenerator;
import org.opendedup.util.XMLUtils;
import org.simpleframework.http.Path;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class MgmtWebServer implements Container {
	private static Connection connection = null;
	private static String archivePath = new File(Main.volume.getPath())
			.getParent() + File.separator + "archives";

	@Override
	public void handle(Request request, Response response) {
		try {
			Path reqPath = request.getPath();

			boolean cmdReq = reqPath.getPath().trim().equalsIgnoreCase("/");

			String file = request.getQuery().get("file");
			String cmd = request.getQuery().get("cmd");

			String cmdOptions = request.getQuery().get("options");
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
							"could not authenticate user  to cli");
				}
			} else {
				auth = true;
			}
			if (cmdReq) {
				if (auth) {
					if (cmd == null) {
						result.setAttribute("status", "failed");
						result.setAttribute("msg", "no command specified");
					}else if(cmd.equalsIgnoreCase("shutdown")) {
						new Shutdown().getResult();
						result.setAttribute("status", "success");
						result.setAttribute("msg", "shutting down volume manager");
					}
					else if (cmd.equalsIgnoreCase("info")) {
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
					} else if (cmd.equalsIgnoreCase("deletefile")) {
						try {
							String msg = new DeleteFileCmd().getResult(
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
					} else if (cmd.equalsIgnoreCase("deletearchive")) {
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
					} else if (cmd.equalsIgnoreCase("makefolder")) {
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
					}

					else if (cmd.equalsIgnoreCase("filteredinfo")) {
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
					} else if (cmd.equalsIgnoreCase("dse-info")) {
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
					} else if (cmd.equalsIgnoreCase("cluster-dse-info")) {
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
					} else if (cmd.equalsIgnoreCase("cluster-volumes")) {
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
					} else if (cmd.equalsIgnoreCase("cluster-volume-remove")) {
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
					} else if (cmd.equalsIgnoreCase("cluster-volume-add")) {
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
					}else if(cmd.equalsIgnoreCase("blockdev-add")) {
						try {
							Element el = new BlockDeviceAdd().getResult(request.getQuery().get("devname"),request.getQuery().get("size"),request.getQuery().get("start"));
							
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully added block device [" + request.getQuery().get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					}
					else if(cmd.equalsIgnoreCase("blockdev-rm")) {
						try {
							Element el = new BlockDeviceRm().getResult(request.getQuery().get("devname"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully removed block device [" + request.getQuery().get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					}
					else if(cmd.equalsIgnoreCase("blockdev-start")) {
						try {
							Element el = new BlockDeviceStart().getResult(request.getQuery().get("devname"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully started block device [" + request.getQuery().get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					}
					else if(cmd.equalsIgnoreCase("blockdev-stop")) {
						try {
							Element el = new BlockDeviceStop().getResult(request.getQuery().get("devname"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully stopped block device [" + request.getQuery().get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					}
					else if(cmd.equalsIgnoreCase("blockdev-list")) {
						try {
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							List<Element> els = new BlockDeviceList().getResult();
							for(Element el : els) {
								result.appendChild(doc.adoptNode(el));
							}
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					}
					else if(cmd.equalsIgnoreCase("blockdev-update")) {
						try {
							Element el = new BlockDeviceUpdate().getResult(request.getQuery().get("devname"),request.getQuery().get("param"),request.getQuery().get("value"));
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"successfully updated block device [" + request.getQuery().get("devname") + "]");
							result.appendChild(doc.adoptNode(el));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					}
					
					else if (cmd.equalsIgnoreCase("set-gc-schedule")) {
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
					} 
					else if (cmd.equalsIgnoreCase("get-gc-schedule")) {
						try {
							Element msg =new GetGCSchedule().getResult(cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
							
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					}
					else if (cmd.equalsIgnoreCase("get-gc-master")) {
						try {
							Element msg =new GetGCMaster().getResult();
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"command completed successfully");
							result.appendChild(doc.adoptNode(msg));
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					}
					
					else if (cmd.equalsIgnoreCase("cluster-promote-gc")) {
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
					} else if (cmd.equalsIgnoreCase("open-files")) {
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
					} else if (cmd.equalsIgnoreCase("debug-info")) {
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
					}/*
					 * else if (cmd.equalsIgnoreCase("events")) { try { String
					 * msg = new GetEvents().getResult(cmdOptions, file); result
					 * =
					 * "<result status=\"success\" msg=\"command completed successfully\">"
					 * ; result = result + msg; result = result + "</result>"; }
					 * catch (IOException e) { result.setAttribute("status",
					 * "failed"); result.setAttribute("msg", e.toString());
					 * SDFSLogger.getLog().warn(e); } }
					 */
					else if (cmd.equalsIgnoreCase("volume-info")) {
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
					} else if (cmd.equalsIgnoreCase("changepassword")) {
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
					} else if (cmd.equalsIgnoreCase("snapshot")) {
						try {
							Element msg = new SnapshotCmd().getResult(
									cmdOptions, file);
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									"replication finished successfully");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					} else if (cmd.equalsIgnoreCase("importarchive")) {
						try {
							String server = request.getQuery().get("server");
							String password = request.getQuery().get("spasswd");
							int port = Integer.parseInt(request.getQuery().get(
									"port"));
							int maxSz = 30;
							boolean useSSL = false;
							if (request.getQuery().containsKey("maxsz"))
								maxSz = Integer.parseInt(request.getQuery()
										.get("maxsz"));
							if (request.getQuery().containsKey("useSSL"))
								useSSL = Boolean.parseBoolean(request
										.getQuery().get("useSSL"));
							Element msg = new ImportArchiveCmd().getResult(
									file, cmdOptions, server, password, port,
									maxSz, useSSL);

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
					} else if (cmd.equalsIgnoreCase("batchgetblocks")) {
						byte[] rb = com.google.common.io.BaseEncoding
								.base64Url().decode(request.getParameter("data"));
						byte[] rslt = new BatchGetBlocksCmd().getResult(rb);
						long time = System.currentTimeMillis();
						response.set("Content-Type", "application/octet-stream");
						response.set("Server", "SDFS Management Server");
						response.setDate("Date", time);
						response.setDate("Last-Modified", time);
						response.getByteChannel().write(ByteBuffer.wrap(rslt));
						response.getByteChannel().close();
					} else if (cmd.equalsIgnoreCase("cancelimport")) {
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
					} else if (cmd.equalsIgnoreCase("archiveout")) {
						try {
							Element msg = new ArchiveOutCmd().getResult(
									cmdOptions, file);
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
					}

					else if (cmd.equalsIgnoreCase("msnapshot")) {
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
					} else if (cmd.equalsIgnoreCase("flush")) {
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
					} else if (cmd.equalsIgnoreCase("expandvolume")) {
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
					} else if (cmd.equalsIgnoreCase("volumeconfigpath")) {
						try {
							result.setAttribute("status", "success");
							result.setAttribute("msg",
									Main.volume.getConfigPath());
						} catch (java.lang.NullPointerException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					} else if (cmd.equalsIgnoreCase("dedup")) {
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
					} else if (cmd.equalsIgnoreCase("perfmon")) {
						String msg = new SetEnablePerfMonCmd().getResult(
								cmdOptions, file);
						result.setAttribute("status", "success");
						result.setAttribute("msg", msg);
					} else if (cmd.equalsIgnoreCase("cleanstore")) {
						try {
							Element msg = new CleanStoreCmd().getResult(
									cmdOptions, null);
							result.setAttribute("status", "success");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					} else if (cmd.equalsIgnoreCase("redundancyck")) {
						try {
							Element msg = new ClusterRedundancyCmd().getResult(
									cmdOptions, null);
							result.setAttribute("status", "success");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					} else if (cmd.equalsIgnoreCase("event")) {
						try {
							String uuid = request.getQuery().get("uuid");
							Element msg = new GetEvent().getResult(uuid);
							result.setAttribute("status", "success");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					} else if (cmd.equalsIgnoreCase("events")) {
						try {
							Element msg = new GetEvents().getResult();
							result.setAttribute("status", "success");
							doc.adoptNode(msg);
							result.appendChild(msg);
						} catch (IOException e) {
							result.setAttribute("status", "failed");
							result.setAttribute("msg", e.toString());
							SDFSLogger.getLog().warn(e);
						}
					}
				}
				if (!cmd.equalsIgnoreCase("batchgetblocks")) {
					String rsString = XMLUtils.toXMLString(doc);
					PrintStream body = response.getPrintStream();
					long time = System.currentTimeMillis();

					// SDFSLogger.getLog().debug(rsString);
					response.set("Content-Type", "text/xml");
					response.set("Server", "SDFS Management Server");
					response.setDate("Date", time);
					response.setDate("Last-Modified", time);
					body.println(rsString);
					body.close();
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
				} else {

					File f = new File(archivePath + File.separator
							+ reqPath.getPath());
					if (f.exists()) {
						long time = System.currentTimeMillis();
						response.set("Content-Type", "application/x-gtar");
						response.set("Server", "SDFS Management Server");
						response.setDate("Date", time);
						response.setDate("Last-Modified", time);
						response.set("Content-Length",
								Long.toString(f.length()));
						InputStream in = new FileInputStream(f);
						OutputStream out = response.getOutputStream();
						byte[] buf = new byte[32768];
						int len;
						while ((len = in.read(buf)) > 0) {
							out.write(buf, 0, len);
						}
						in.close();
						out.close();
						f.delete();
					} else {
						response.setCode(404);
						PrintStream body = response.getPrintStream();
						body.println("could not find " + reqPath);
						body.close();
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
				SDFSLogger.getLog().debug("error when closing response",e);
			}
		}
	}

	public static void start(boolean useSSL) throws InvalidKeyException,
			KeyStoreException, NoSuchAlgorithmException, CertificateException,
			NoSuchProviderException, SignatureException, IOException,
			UnrecoverableKeyException, KeyManagementException {
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
		        SSLContext sslContext = SSLContext.getInstance("SSLv3");
		        // sslContext.init(keyManagerFactory.getKeyManagers(), new TrustManager[]{new NaiveX509TrustManager()}, null);
		        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
				Container container = new MgmtWebServer();
				connection = new SocketConnection(container);
				Main.sdfsCliPort = FindOpenPort.pickFreePort(Main.sdfsCliPort);
				SocketAddress address = new InetSocketAddress(
						Main.sdfsCliListenAddr, Main.sdfsCliPort);
				connection.connect(address, sslContext);
				SDFSLogger.getLog().info(
						"###################### SDFSCLI SSL Management WebServer Started at "
								+ address.toString()
								+ " #########################");
			} else {
				try {
					Container container = new MgmtWebServer();
					connection = new SocketConnection(container);
					Main.sdfsCliPort = FindOpenPort
							.pickFreePort(Main.sdfsCliPort);
					SocketAddress address = new InetSocketAddress(
							Main.sdfsCliListenAddr, Main.sdfsCliPort);
					connection.connect(address);
					SDFSLogger.getLog().info(
							"###################### SDFSCLI Management WebServer Started at "
									+ address.toString()
									+ " #########################");
				} catch (IOException e) {
					SDFSLogger.getLog().error(
							"unable to start Web Management Server", e);
				}
			}
		}
	}

	public static void stop() {
		try {
			connection.close();
		} catch (IOException e) {
			SDFSLogger.getLog()
					.error("unable to stop Web Management Server", e);
		}
	}

}
