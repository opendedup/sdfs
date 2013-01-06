package org.opendedup.sdfs.network;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;


import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HashChunkService;

import sun.security.x509.CertAndKeyGen;
import sun.security.x509.X500Name;

public class NetworkHCServer {

	// Declaration section:
	// declare a server socket and a client socket for the server
	// declare an input and an output stream

	static Socket clientSocket = null;
	static ServerSocket serverSocket = null;

	private static NioUDPServer udpServer = null;

	// This chat server can accept up to 10 clients' connections

	public static void main(String args[]) throws IOException {
		// The default port

		if (args.length < 1) {
			System.out.println("Usage: NetworkHCServer <configFile>");
		} else {
			ShutdownHook shutdownHook = new ShutdownHook();
			Runtime.getRuntime().addShutdownHook(shutdownHook);

			try {
				Config.parseDSEConfigFile(args[0]);
			} catch (IOException e1) {
				SDFSLogger.getLog().fatal(
						"exiting because of an error with the config file");
				System.exit(-1);
			}
			init();

		}

	}

	public static void init() throws IOException {
		HashChunkService.init();
		// Initialization section:
		// Try to open a server socket on port port_number (default 2222)
		// Note that we can't choose a port less than 1023 if we are not
		// privileged users (root)
		try {
			InetSocketAddress addr = new InetSocketAddress(Main.serverHostName,
					Main.serverPort);
			if(Main.serverUseSSL) {
				String keydir = Main.hashDBStore + File.separator + "keys";
				String key = keydir + File.separator + "dse_server.keystore";
				if(!new File(key).exists()) {
					new File(keydir).mkdirs();
				 KeyStore keyStore = KeyStore.getInstance("JKS");
			        keyStore.load(null, null);

			        CertAndKeyGen keypair = new CertAndKeyGen("RSA", "SHA1WithRSA", null);

			        X500Name x500Name = new X500Name(InetAddress.getLocalHost().getCanonicalHostName(), "sdfs_dse", "opendedup", "portland", "or", "US");

			        keypair.generate(1024);
			        PrivateKey privKey = keypair.getPrivateKey();

			        X509Certificate[] chain = new X509Certificate[1];

			        chain[0] = keypair.getSelfCertificate(x500Name, new Date(), (long) 1096 * 24 * 60 * 60);

			        keyStore.setKeyEntry("sdfs", privKey, "sdfs".toCharArray(), chain);

			        keyStore.store(new FileOutputStream(key), "sdfs".toCharArray());
			        SDFSLogger.getLog().info("generated certificate for ssl communication at " + key);
				}
				FileInputStream keyFile = new FileInputStream(key); 
				KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
				keyStore.load(keyFile, "sdfs".toCharArray());
				// init KeyManagerFactory
				KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
				keyManagerFactory.init(keyStore, "sdfs".toCharArray());
				// init KeyManager
				KeyManager keyManagers[] = keyManagerFactory.getKeyManagers();
				// init the SSL context
				SSLContext sslContext = SSLContext.getDefault();
				sslContext.init(keyManagers, null, new SecureRandom());
				// get the socket factory
				SSLServerSocketFactory socketFactory = sslContext.getServerSocketFactory();

				// and finally, get the socket
				serverSocket = socketFactory.createServerSocket();
				serverSocket.bind(addr);
				SDFSLogger.getLog().info("listening on encryted channel " + addr.toString());
			} else {
			if (Main.useUDP) {
				udpServer = new NioUDPServer();
			}
			serverSocket = new ServerSocket();
			serverSocket.bind(addr);
			SDFSLogger.getLog().info("listening on unencryted channel " + addr.toString());
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
				new ClientThread(clientSocket).start();
			} catch (IOException e) {
				if (!serverSocket.isClosed())
					SDFSLogger.getLog().error(
							"Unable to open port " + e.toString(), e);
			}
		}
	}

	public static void close() {
		try {
			System.out.println("#### Shutting Down Network Service ####");

			serverSocket.close();
		} catch (Exception e) {
		}
		try {

			udpServer.close();
		} catch (Exception e) {
		}
		System.out.println("#### Shutting down HashStore ####");
		HashChunkService.close();
		System.out.println("#### Shut down completed ####");
	}
}

class ShutdownHook extends Thread {
	@Override
	public void run() {
		System.out.println("#### Shutting down StorageHub ####");

		NetworkHCServer.close();
	}
}