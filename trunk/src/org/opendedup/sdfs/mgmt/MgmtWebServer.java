package org.opendedup.sdfs.mgmt;

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

public class MgmtWebServer implements Container {
	private static Connection connection = null;

	public void handle(Request request, Response response) {
		try {
			String file = request.getQuery().get("file");
			String cmd = request.getQuery().get("cmd");
			String cmdOptions = request.getQuery().get("options");
			String result = "<result status=\"failed\" msg=\"command not found\"/>";
			if(cmd == null)
				result = "<result status=\"failed\" msg=\"no command specified\"/>";
			else if(cmd.equalsIgnoreCase("info")) {
				String msg = new GetAttributes().getResult(cmdOptions, file);
				result = "<result status=\"success\" msg=\"command completed successfully\">";
				result = result + msg;
				result = result + "</result>";
			}
			else if(cmd.equalsIgnoreCase("snapshot")) {
				try {
				String msg = new SnapshotCmd().getResult(cmdOptions,file);
				result = "<result status=\"success\" msg=\""+msg+"\"/>";
				}catch(IOException e) {
					result = "<result status=\"failed\" msg=\""+e.getMessage()+"\"/>";
				}
			}
			else if(cmd.equalsIgnoreCase("flush")) {
				try {
				String msg = new FlushBuffersCmd().getResult(cmdOptions,file);
				result = "<result status=\"success\" msg=\""+msg+"\"/>";
				}catch(IOException e) {
					result = "<result status=\"failed\" msg=\""+e.getMessage()+"\"/>";
				}
			}
			else if(cmd.equalsIgnoreCase("makevmdk")) {
				try {
				String msg = new MakeVMDKCmd().getResult(cmdOptions,file);
				result = "<result status=\"success\" msg=\""+msg+"\"/>";
				}catch(IOException e) {
					result = "<result status=\"failed\" msg=\""+e.getMessage()+"\"/>";
				}
			}
			else if(cmd.equalsIgnoreCase("dedup")) {
				try {
				String msg = new SetDedupAllCmd().getResult(cmdOptions,file);
				result = "<result status=\"success\" msg=\""+msg+"\"/>";
				}catch(IOException e) {
					result = "<result status=\"failed\" msg=\""+e.getMessage()+"\"/>";
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
		try {
			Container container = new MgmtWebServer();
			connection = new SocketConnection(container);
			SocketAddress address = new InetSocketAddress("localhost", 6442);
			connection.connect(address);
			SDFSLogger.getLog().info("Management WebServer Started at " + address.toString());
		} catch (IOException e) {
			SDFSLogger.getLog().error("unable to start Web Management Server",
					e);
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
