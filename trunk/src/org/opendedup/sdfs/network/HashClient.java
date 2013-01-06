package org.opendedup.sdfs.network;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;


import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.servers.HCServer;

public class HashClient {

	private Socket clientSocket = null;
	private DataOutputStream os = null;
	private DataInputStream is = null;
	private BufferedReader inReader = null;
	private boolean closed = false;
	private HCServer server;
	private String name = "";
	private String password = "";

	private ReentrantLock lock = new ReentrantLock();

	// private LRUMap existsBuffers = new LRUMap(10);

	public HashClient(HCServer server, String name, String password) {
		this.server = server;
		this.name = name;
		this.openConnection();
	}

	public String getName() {
		return this.name;
	}

	public boolean isClosed() {
		return this.closed;
	}

	public synchronized void openConnection() {
		// System.out.println("Opening Connection to " + server.getHostName());
		// Initialization section:
		// Try to open a socket on a given host and port
		// Try to open input and output streams
		try {
			SDFSLogger.getLog().debug(
					"Connecting to server " + server.getHostName()
							+ " on port " + server.getPort());
			if (server.isSSL()) {
				TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
					@Override
					public void checkClientTrusted(
							final X509Certificate[] chain, final String authType) {
					}

					@Override
					public void checkServerTrusted(
							final X509Certificate[] chain, final String authType) {
					}

					@Override
					public X509Certificate[] getAcceptedIssuers() {
						return null;
					}
				} };
				SSLContext sslContext = SSLContext.getInstance("TLS");
				sslContext.init(null, trustAllCerts,
						new java.security.SecureRandom());
				// Create an ssl socket factory with our all-trusting manager
				SSLSocketFactory sslSocketFactory = sslContext
						.getSocketFactory();
				clientSocket = sslSocketFactory.createSocket(
						server.getHostName(), server.getPort());
			} else {
				clientSocket = new Socket(server.getHostName(),
						server.getPort());
			}
			clientSocket.setKeepAlive(true);
			clientSocket.setTcpNoDelay(true);
			os = new DataOutputStream(new BufferedOutputStream(
					clientSocket.getOutputStream(), Main.CHUNK_LENGTH + 34));
			is = new DataInputStream(new BufferedInputStream(
					clientSocket.getInputStream(), Main.CHUNK_LENGTH + 34));
			inReader = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()));
			// Read the Header Line
			inReader.readLine();
			String passwdMessage = password + "\r\n";
			os.write(passwdMessage.getBytes());
			os.flush();
			int auth = is.readInt();
			if (auth == 0)
				throw new IOException(
						"unable to authenticate chech upstream password");
			this.closed = false;
			SDFSLogger.getLog().debug(
					"hashclient connection established "
							+ clientSocket.toString());
		} catch (UnknownHostException e) {
			SDFSLogger.getLog().fatal("Don't know about host " + server);
			this.closed = true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"Couldn't get I/O for the connection to the host", e);
			this.closed = true;
		}
	}

	public void executeCmd(IOCmd cmd) throws IOException {
		if (this.closed)
			this.openConnection();
		lock.lock();
		try {

			cmd.executeCmd(is, os);
		} catch (Exception e) {
			this.closed = true;
			try {
				this.openConnection();
				cmd.executeCmd(is, os);
			} catch (Exception e1) {
				SDFSLogger.getLog().fatal("unable to execute command", e);
				throw new IOException("unable to execute command");
			}
		} finally {
			lock.unlock();
		}
	}

	private void executeUDPCmd(IOCmd cmd) throws SocketTimeoutException,
			IOException {
		DatagramSocket socket = new DatagramSocket();
		socket.setSoTimeout(Main.UDPClientTimeOut);
		byte[] b = new byte[33];
		ByteBuffer buf = ByteBuffer.wrap(b);
		if (cmd.getCmdID() == NetworkCMDS.HASH_EXISTS_CMD) {
			HashExistsCmd hcmd = (HashExistsCmd) cmd;
			buf.put(NetworkCMDS.HASH_EXISTS_CMD);
			buf.put(hcmd.getHash());
			InetSocketAddress addr = new InetSocketAddress(
					server.getHostName(), server.getPort());
			DatagramPacket packet = new DatagramPacket(buf.array(), b.length,
					addr);
			socket.send(packet);
			packet = new DatagramPacket(new byte[2], 2, addr);
			socket.receive(packet);
			buf = ByteBuffer.wrap(packet.getData());
			short exists = buf.getShort();
			if (exists == 0)
				hcmd.exists = false;
			else
				hcmd.exists = true;
		}
		b = null;
	}

	public void close() {
		try {
			os.write(NetworkCMDS.QUIT_CMD);
			os.flush();
		} catch (Exception e) {

		} finally {
			try {
				inReader.close();
			} catch (IOException e) {
			}
			try {
				os.close();
			} catch (IOException e) {
			}
			try {
				is.close();
			} catch (IOException e) {
			}
			try {
				clientSocket.close();
			} catch (IOException e) {
			}
			os = null;
			is = null;
			inReader = null;
			this.closed = true;
		}
	}

	public boolean writeChunk(byte[] hash, byte[] aContents, int position,
			int len) throws IOException {
		WriteHashCmd cmd = new WriteHashCmd(hash, aContents, len,
				server.isCompress());

		this.executeCmd(cmd);

		return cmd.wasWritten();
	}

	public byte[] fetchChunk(byte[] hash) throws IOException {
		FetchChunkCmd cmd = new FetchChunkCmd(hash, server.isCompress());
		this.executeCmd(cmd);
		return cmd.getChunk();
	}

	public ArrayList<HashChunk> fetchChunks(ArrayList<String> al)
			throws IOException {
		BulkFetchChunkCmd cmd = new BulkFetchChunkCmd(al);
		this.executeCmd(cmd);
		return cmd.getChunks();
	}

	public boolean hashExists(byte[] hash, short hops) throws IOException {
		HashExistsCmd cmd = new HashExistsCmd(hash, hops);
		if (server.isUseUDP()) {
			try {
				this.executeUDPCmd(cmd);
			} catch (IOException e) {
				System.out.println("trying tcp");
				this.executeCmd(cmd);
			}
		} else {
			this.executeCmd(cmd);
		}
		return cmd.exists();
	}

	public void ping() throws IOException {
		PingCmd cmd = new PingCmd();
		this.executeCmd(cmd);
	}

}
