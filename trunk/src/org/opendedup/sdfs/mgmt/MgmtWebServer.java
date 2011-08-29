package org.opendedup.sdfs.mgmt;

import org.opendedup.sdfs.Main;


import org.opendedup.util.FindOpenPort;
import org.opendedup.util.HashFunctions;
import org.opendedup.util.SDFSLogger;
import org.simpleframework.http.core.Container;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.simpleframework.http.Response;
import org.simpleframework.http.Request;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.io.IOException;
import java.io.PrintStream;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLServerSocketFactory;

public class MgmtWebServer implements Container {
	private static Connection connection = null;

	public void handle(Request request, Response response) {
		try {
			String file = request.getQuery().get("file");
			String cmd = request.getQuery().get("cmd");
			String cmdOptions = request.getQuery().get("options");
			String result = "<result status=\"failed\" msg=\"could not authenticate user\"/>";
			boolean auth = false;
			if (Main.sdfsCliRequireAuth) {
				String userName = request.getQuery().get("username");
				if (userName != null
						&& userName.equalsIgnoreCase(Main.sdfsCliUserName)) {
					String password = request.getQuery().get("password");
					if (password != null) {
						String hash = HashFunctions.getSHAHash(password.trim()
								.getBytes(), Main.sdfsCliSalt.getBytes());
						if (hash.equals(Main.sdfsCliPassword))
							auth = true;
					} else {
						SDFSLogger.getLog().warn(
								"could not authenticate user " + userName
										+ " to cli");
					}
				}else {
				SDFSLogger.getLog().warn(
						"could find user " + userName
								+ " to authenticate to cli");
				}
			} else
				auth = true;
			if (auth) {
				if (cmd == null)
					result = "<result status=\"failed\" msg=\"no command specified\"/>";
				else if (cmd.equalsIgnoreCase("info")) {
					try {
						String msg = new GetAttributes().getResult(cmdOptions,
								file);
						result = "<result status=\"success\" msg=\"command completed successfully\">";
						result = result + msg;
						result = result + "</result>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} else if (cmd.equalsIgnoreCase("deletefile")) {
					try {
						String msg = new DeleteFileCmd().getResult(cmdOptions,
								file);
						result = "<result status=\"success\" msg=\"command completed successfully\">";
						result = result + msg;
						result = result + "</result>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				}
				else if (cmd.equalsIgnoreCase("makefolder")) {
					try {
						String msg = new MakeFolderCmd().getResult(cmdOptions,
								file);
						result = "<result status=\"success\" msg=\"command completed successfully\">";
						result = result + msg;
						result = result + "</result>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				}
				
				else if (cmd.equalsIgnoreCase("filteredinfo")) {
					try {
						boolean includeFiles = Boolean.parseBoolean(request.getQuery().get("includefiles"));
						boolean includeFolders = Boolean.parseBoolean(request.getQuery().get("includefolders"));
						int level = Integer.parseInt(request.getQuery().get("level"));
						String msg = new GetFilteredFileAttributes().getResult(cmdOptions,
								file,includeFiles,includeFolders,level);
						result = "<result status=\"success\" msg=\"command completed successfully\">";
						result = result + msg;
						result = result + "</result>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} 
				else if (cmd.equalsIgnoreCase("dse-info")) {
					try {
						String msg = new GetDSE().getResult(cmdOptions, file);
						result = "<result status=\"success\" msg=\"command completed successfully\">";
						result = result + msg;
						result = result + "</result>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} else if (cmd.equalsIgnoreCase("debug-info")) {
					try {
						String msg = new GetDebug().getResult(cmdOptions, file);
						result = "<result status=\"success\" msg=\"command completed successfully\">";
						result = result + msg;
						result = result + "</result>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} else if (cmd.equalsIgnoreCase("volume-info")) {
					try {
						String msg = new GetVolume()
								.getResult(cmdOptions, file);
						result = "<result status=\"success\" msg=\"command completed successfully\">";
						result = result + msg;
						result = result + "</result>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} else if (cmd.equalsIgnoreCase("changepassword")) {
					try {
						String msg = new SetPasswordCmd().getResult("",request.getQuery().get("newpassword"));
						result = "<result status=\"success\" msg=\"" + msg
								+ "\"/>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				}else if (cmd.equalsIgnoreCase("snapshot")) {
					try {
						String msg = new SnapshotCmd().getResult(cmdOptions,
								file);
						result = "<result status=\"success\" msg=\"" + msg
								+ "\"/>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} else if (cmd.equalsIgnoreCase("msnapshot")) {
					try {
						int snaps = request.getQuery().getInteger("snaps");
						String msg = new MultiSnapshotCmd(snaps).getResult(
								cmdOptions, file);
						result = "<result status=\"success\" msg=\"" + msg
								+ "\"/>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} else if (cmd.equalsIgnoreCase("flush")) {
					try {
						String msg = new FlushBuffersCmd().getResult(
								cmdOptions, file);
						result = "<result status=\"success\" msg=\"" + msg
								+ "\"/>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} else if (cmd.equalsIgnoreCase("expandvolume")) {
					try {
						String size = request.getQuery().get("size");
						String msg = new ExpandVolumeCmd().getResult(
								cmdOptions, size);
						result = "<result status=\"success\" msg=\"" + msg
								+ "\"/>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} 
				else if (cmd.equalsIgnoreCase("volumeconfigpath")) {
					try {
						
						result = "<result status=\"success\" msg=\"" + Main.wth.getConfigFilePath()
								+ "\"/>";
					} catch (java.lang.NullPointerException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} 
				else if (cmd.equalsIgnoreCase("makevmdk")) {
					try {
						String msg = new MakeVMDKCmd().getResult(cmdOptions,
								file);
						result = "<result status=\"success\" msg=\"" + msg
								+ "\"/>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} else if (cmd.equalsIgnoreCase("dedup")) {
					try {
						String msg = new SetDedupAllCmd().getResult(cmdOptions,
								file);
						result = "<result status=\"success\" msg=\"" + msg
								+ "\"/>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				} else if (cmd.equalsIgnoreCase("cleanstore")) {
					try {
						String msg = new CleanStoreCmd().getResult(cmdOptions,
								null);
						result = "<result status=\"success\" msg=\"" + msg
								+ "\"/>";
					} catch (IOException e) {
						result = "<result status=\"failed\" msg=\""
								+ e.getMessage() + "\"/>";
					}
				}
			}
			PrintStream body = response.getPrintStream();
			long time = System.currentTimeMillis();

			response.set("Content-Type", "text/xml");
			response.set("Server", "SDFS Management Server");
			response.setDate("Date", time);
			response.setDate("Last-Modified", time);

			body.println(result);
			body.close();
		} catch (Exception e) {
			response.setMajor(500);
			e.printStackTrace();
		}
	}

	public static void start() {
		if (Main.sdfsCliEnabled) {
			try {
				Container container = new MgmtWebServer();
				connection = new SocketConnection(container);
				Main.sdfsCliPort = FindOpenPort.pickFreePort(Main.sdfsCliPort);
				SocketAddress address = new InetSocketAddress(Main.sdfsCliListenAddr,
						Main.sdfsCliPort);
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

	public static void stop() {
		try {
			connection.close();
		} catch (IOException e) {
			SDFSLogger.getLog()
					.error("unable to stop Web Management Server", e);
		}
	}

}
