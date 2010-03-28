package org.opendedup.sdfs.network;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections.map.LRUMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HCServer;

public class HashClient {

	private Socket clientSocket = null;
	private DataOutputStream os = null;
	private DataInputStream is = null;
	private BufferedReader inReader = null;
	private boolean closed = false;
	private HCServer server;
	private String name = "";
	private static Logger log = Logger.getLogger("sdfs");
	private ReentrantLock lock = new ReentrantLock();

	// private LRUMap existsBuffers = new LRUMap(10);

	public HashClient(HCServer server, String name) {
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
			log.info("Connecting to server " + server.getHostName() + " on port " + server.getPort());
			clientSocket = new Socket(server.getHostName(), server.getPort());
			clientSocket.setKeepAlive(true);
			clientSocket.setTcpNoDelay(true);
			os = new DataOutputStream(new BufferedOutputStream(clientSocket
					.getOutputStream(), Main.CHUNK_LENGTH + 34));
			is = new DataInputStream(new BufferedInputStream(clientSocket
					.getInputStream(), Main.CHUNK_LENGTH + 34));
			inReader = new BufferedReader(new InputStreamReader(clientSocket
					.getInputStream()));
			// Read the Header Line
			inReader.readLine();
			this.closed = false;
		} catch (UnknownHostException e) {
			log.severe("Don't know about host " + server);
			this.closed = true;
		} catch (Exception e) {
			log.log(Level.SEVERE,"Couldn't get I/O for the connection to the host",e);
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
				log.log(Level.SEVERE, "unable to execute command", e);
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
		WriteHashCmd cmd = new WriteHashCmd(hash, aContents, len, server
				.isCompress());

		this.executeCmd(cmd);

		return cmd.wasWritten();
	}

	public byte[] fetchChunk(byte[] hash) throws IOException {
		FetchChunkCmd cmd = new FetchChunkCmd(hash, server.isCompress());
		this.executeCmd(cmd);
		return cmd.getChunk();
	}

	public boolean hashExists(byte[] hash) throws IOException {
		HashExistsCmd cmd = new HashExistsCmd(hash);
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
