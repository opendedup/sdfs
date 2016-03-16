package org.opendedup.sdfs.cluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.network.ClientThread;
import org.opendedup.util.KeyGenerator;

public class NetworkUnicastServer {

	// Declaration section:
	// declare a server socket and a client socket for the server
	// declare an input and an output stream

	static Socket clientSocket = null;
	static ServerSocket serverSocket = null;

	public static void init() throws IOException {
		// Initialization section:
		// Try to open a server socket on port port_number (default 2222)
		// Note that we can't choose a port less than 1023 if we are not
		// privileged users (root)
		try {

			InetSocketAddress addr = new InetSocketAddress(Main.serverHostName,
					Main.serverPort);
			SDFSLogger.getLog().info(
					"############ Will Listen On " + addr.toString()
							+ " ########################");
			if (Main.serverUseSSL) {
				String keydir = Main.hashDBStore + File.separator + "keys";
				String key = keydir + File.separator + "dse_server.keystore";
				if (!new File(key).exists()) {
					KeyGenerator.generateKey(new File(key));
					SDFSLogger.getLog().info(
							"generated certificate for ssl communication at "
									+ key);
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
				KeyManager keyManagers[] = keyManagerFactory.getKeyManagers();
				// init the SSL context
				SSLContext sslContext = SSLContext.getDefault();
				sslContext.init(keyManagers, null, new SecureRandom());
				// get the socket factory
				SSLServerSocketFactory socketFactory = sslContext
						.getServerSocketFactory();

				// and finally, get the socket
				serverSocket = socketFactory.createServerSocket();
				serverSocket.setPerformancePreferences(0, 1, 2);
				serverSocket.bind(addr);
				SDFSLogger.getLog().info(
						"listening on encryted channel " + addr.toString());
			} else {

				serverSocket = new ServerSocket();
				serverSocket.setPerformancePreferences(0, 1, 2);
				serverSocket.setReceiveBufferSize(64 * 1024);
				serverSocket.bind(addr);

			}
		} catch (Exception e) {
			e.printStackTrace();
			SDFSLogger.getLog().fatal("unable to open network ports", e);
			System.exit(-1);
		}

		// Create a socket object from the ServerSocket to listen and accept
		// connections.
		// Open input and output streams for this socket will be created in
		// client's thread since every client is served by the server in
		// an individual thread

		while (true) {
			try {
				clientSocket = serverSocket.accept();
				clientSocket.setKeepAlive(true);
				clientSocket.setTcpNoDelay(true);
				clientSocket.setSendBufferSize(64 * 1024);
				new ClientThread(clientSocket).start();
			} catch (IOException e) {
				if (!serverSocket.isClosed())
					SDFSLogger.getLog().error(
							"Unable to open port " + e.toString(), e);
			}
		}
	}

	public static int getPort() {
		return serverSocket.getLocalPort();
	}

	public static void close() {
		try {
			SDFSLogger.getLog().info(
					"#### Shutting Down TCP Network Service ####");

			serverSocket.close();
		} catch (Exception e) {
		}
		SDFSLogger.getLog().info("#### Shut down completed ####");
	}
}