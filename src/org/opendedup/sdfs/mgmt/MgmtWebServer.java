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
			String path = request.getPath().getPath();
			String tst = request.getQuery().get("ninja");
			PrintStream body = response.getPrintStream();
			long time = System.currentTimeMillis();

			response.set("Content-Type", "text/plain");
			response.set("Server", "HelloWorld/1.0 (Simple 4.0)");
			response.setDate("Date", time);
			response.setDate("Last-Modified", time);

			body.println("Hello World " + tst + " path=" + path);
			body.close();
		} catch (Exception e) {
			response.setMajor(500);
			e.printStackTrace();
		}
	}

	public static void start() {
		try {
			Container container = new MgmtWebServer();
			Connection connection = new SocketConnection(container);
			SocketAddress address = new InetSocketAddress("localhost", 6442);
			connection.connect(address);
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
