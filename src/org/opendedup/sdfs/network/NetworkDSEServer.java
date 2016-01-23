package org.opendedup.sdfs.network;

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
import org.opendedup.util.FindOpenPort;
import org.opendedup.util.KeyGenerator;

public class NetworkDSEServer implements Runnable {
	Socket clientSocket = null;
	ServerSocket serverSocket = null;
	public boolean closed = false;

	@Override
	public void run() {
		try {
			Main.serverPort = FindOpenPort.pickFreePort(Main.serverPort);
			InetSocketAddress addr = new InetSocketAddress(Main.serverHostName,
					Main.serverPort);
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
				SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
				sslContext.init(keyManagers, null, new SecureRandom());
				// get the socket factory
				SSLServerSocketFactory socketFactory = sslContext
						.getServerSocketFactory();

				// and finally, get the socket
				serverSocket = socketFactory.createServerSocket();
				serverSocket.bind(addr);
				SDFSLogger.getLog().info(
						"listening on encryted channel " + addr.toString());
			} else {

				serverSocket = new ServerSocket();
				// serverSocket.setReceiveBufferSize(128 * 1024);

				serverSocket.bind(addr);
				SDFSLogger.getLog().info(
						"listening on unencryted channel " + addr.toString());
			}
		} catch (Exception e) {
			System.err.println("unable to open network ports : "
					+ e.getMessage());
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
				clientSocket.setTcpNoDelay(false);
				// clientSocket.setSendBufferSize(128 * 1024);
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

		System.out.println("#### Network Service Shut down completed ####");
	}

}
