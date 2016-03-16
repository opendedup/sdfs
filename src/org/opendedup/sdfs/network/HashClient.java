package org.opendedup.sdfs.network;

import java.io.BufferedInputStream;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.servers.HCServer;

public class HashClient implements Runnable {
	private Socket clientSocket = null;
	private DataOutputStream os = null;
	private DataInputStream is = null;
	private BufferedReader inReader = null;
	private boolean closed = true;
	private HCServer server;
	private String name = "";
	private String password = "";
	private IOCmd ncmd = null;
	private Object result = null;
	private AsyncCmdListener listener;
	private byte id;
	private boolean suspect = false;

	// private LRUMap existsBuffers = new LRUMap(10);

	public HashClient(HCServer server, String name, String password, byte id)
			throws IOException {
		this.server = server;
		this.name = name;
		this.id = id;
		this.password = password;
		try {
			this.openConnection();
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to open connection", e);
			throw new IOException("unable to open connection");
		}
	}

	public boolean isSuspect() {
		return this.suspect;
	}

	public String getName() {
		return this.name;
	}

	public boolean isClosed() {
		synchronized (this) {
			return this.closed;
		}
	}

	public synchronized void openConnection() throws IOException {
		// System.out.println("Opening Connection to " + server.getHostName());
		// Initialization section:
		// Try to open a socket on a given host and port
		// Try to open input and output streams
		synchronized (this) {
			if (this.closed) {
				try {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug(
								"Connecting to server " + server.getHostName()
										+ " on port " + server.getPort());
					if (server.isSSL()) {
						TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
							@Override
							public void checkClientTrusted(
									final X509Certificate[] chain,
									final String authType) {
							}

							@Override
							public void checkServerTrusted(
									final X509Certificate[] chain,
									final String authType) {
							}

							@Override
							public X509Certificate[] getAcceptedIssuers() {
								return null;
							}
						} };
						SSLContext sslContext = SSLContext
								.getInstance("TLSv1.2");
						sslContext.init(null, trustAllCerts,
								new java.security.SecureRandom());
						// Create an ssl socket factory with our all-trusting
						// manager
						SSLSocketFactory sslSocketFactory = sslContext
								.getSocketFactory();
						clientSocket = sslSocketFactory.createSocket();
					} else {
						clientSocket = new Socket();
					}
					clientSocket.setKeepAlive(true);
					clientSocket.setTcpNoDelay(false);
					// clientSocket.setReceiveBufferSize(128 * 1024);
					// clientSocket.setSendBufferSize(128 * 1024);
					clientSocket.setPerformancePreferences(0, 1, 2);

					clientSocket.connect(new InetSocketAddress(server
							.getHostName(), server.getPort()));

					clientSocket.setSoTimeout(3000);
					os = new DataOutputStream(new BufferedOutputStream(
							clientSocket.getOutputStream(), 32768));
					is = new DataInputStream(new BufferedInputStream(
							clientSocket.getInputStream(), 32768));
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
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug(
								"hashclient connection established "
										+ clientSocket.toString());
				} catch (UnknownHostException e) {
					SDFSLogger.getLog().fatal(
							"Don't know about host " + server.getHostName()
									+ server.getPort());
					this.closed = true;
					throw e;
				} catch (Exception e) {
					SDFSLogger.getLog().fatal(
							"Couldn't get I/O for the connection to the host",
							e);
					this.closed = true;
					throw new IOException(
							"Couldn't get I/O for the connection to the host "
									+ server.getHostName() + ":"
									+ server.getPort());
				}
			}
		}
	}

	public void executeCmd(IOCmd cmd) throws IOException {
		if (this.closed) {
			try {
				this.openConnection();
			} catch (Exception e) {
				SDFSLogger.getLog().fatal(
						"unable to open connection to "
								+ clientSocket.toString(), e);
				throw new IOException(e);
			}
		}
		try {
			cmd.executeCmd(is, os);
		} catch (Exception e) {
			if (e instanceof java.net.SocketTimeoutException) {
				this.suspect = true;
				try {
					this.close();
				} catch (Exception e1) {
				}
				throw new IOException(
						"unable to execute command because connection timed out");
			} else {
				this.closed = true;
				SDFSLogger.getLog().error("error executing command", e);
				try {
					this.close();
				} catch (Exception e1) {
				}
				throw new IOException("unable to execute command");
			}
		}
	}

	public void close() {
		synchronized (this) {
			try {
				os.write(NetworkCMDS.QUIT_CMD);
				os.flush();
			} catch (Exception e) {

			} finally {
				try {
					inReader.close();
				} catch (Exception e) {
				}
				try {
					os.close();
				} catch (Exception e) {
				}
				try {
					is.close();
				} catch (Exception e) {
				}
				try {
					clientSocket.close();
				} catch (Exception e) {
				}
				os = null;
				is = null;
				inReader = null;
				this.closed = true;
			}
		}
	}

	public boolean writeChunk(byte[] hash, byte[] aContents, int position,
			int len) throws IOException {
		WriteHashCmd cmd = new WriteHashCmd(hash, aContents, len,
				server.isCompress());

		this.executeCmd(cmd);

		return cmd.wasWritten();
	}

	public void writeChunkAsync(byte[] hash, byte[] aContents, int position,
			int len, AsyncCmdListener l) throws IOException {
		this.listener = l;
		this.ncmd = new WriteHashCmd(hash, aContents, len, server.isCompress());
	}

	public byte[] fetchChunk(byte[] hash) throws IOException {
		synchronized (this) {
			FetchChunkCmd cmd = new FetchChunkCmd(hash, server.isCompress());
			this.executeCmd(cmd);
			return cmd.getResult();
		}
	}

	public ArrayList<HashChunk> fetchChunks(ArrayList<String> al)
			throws IOException {
		synchronized (this) {
			BulkFetchChunkCmd cmd = new BulkFetchChunkCmd(al);
			this.executeCmd(cmd);
			return cmd.getChunks();
		}
	}

	public boolean hashExists(byte[] hash) throws IOException {
		synchronized (this) {
			HashExistsCmd cmd = new HashExistsCmd(hash);

			this.executeCmd(cmd);
			return cmd.exists();
		}
	}

	public void ping() throws IOException {
		synchronized (this) {
			PingCmd cmd = new PingCmd();
			this.executeCmd(cmd);
		}
	}

	@Override
	public void run() {
		try {
			this.executeCmd(ncmd);
			this.result = ncmd.getResult();
			this.listener.commandResponse(this.result, this);
			// SDFSLogger.getLog().debug("thread ran result is " +
			// (Boolean)this.result);

		} catch (Exception e) {
			this.listener.commandException(e);
		} finally {

		}
	}

	public Object getResult() {
		return this.result;
	}

	public byte getId() {
		return id;
	}

}
