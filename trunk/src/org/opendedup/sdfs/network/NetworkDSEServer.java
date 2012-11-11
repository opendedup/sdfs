package org.opendedup.sdfs.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

public class NetworkDSEServer implements Runnable {
	Socket clientSocket = null;
	ServerSocket serverSocket = null;
	public boolean closed = false;
	private static NioUDPServer udpServer = null;

	@Override
	public void run() {
		try {
			InetSocketAddress addr = new InetSocketAddress(Main.serverHostName,
					Main.serverPort);
			if (Main.useUDP) {
				udpServer = new NioUDPServer();
			}
			serverSocket = new ServerSocket();
			serverSocket.bind(addr);
			SDFSLogger.getLog().info("listening on " + addr.toString());
		} catch (Exception e) {
			System.err.println("unable to open network ports : " + e.getMessage());
			System.err.println("check logs for more details");
			SDFSLogger.getLog().fatal("unable to open network ports", e);
			System.exit(-1);
		}

		// Create a socket object from the ServerSocket to listen and accept
		// connections.
		// Open input and output streams for this socket will be created in
		// client's thread since every client is served by the server in
		// an individual thread

		while (!closed) {
			try {
				clientSocket = serverSocket.accept();
				clientSocket.setKeepAlive(true);
				clientSocket.setTcpNoDelay(true);
				new ClientThread(clientSocket).start();
			} catch (IOException e) {
				if (!serverSocket.isClosed())
					SDFSLogger.getLog().fatal(
							"Unable to open port " + e.toString(), e);
			}
		}

	}
	
	public synchronized void close() {
		this.closed = true;	
		try {
				System.out.println("#### Shutting Down Network Service ####");

				serverSocket.close();
			} catch (Exception e) {
			}
			try {

				udpServer.close();
			} catch (Exception e) {
			}
			
			System.out.println("#### Network Service Shut down completed ####");
	}

}
