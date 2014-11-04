package org.opendedup.sdfs.network;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.QuickList;
import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.StringUtils;

/**
 * @author Sam Silverberg This is the network class that is used within the
 *         Chunk store to service all client requests and responses. It is
 *         threaded and is spawned by @see
 *         com.annesam.sdfs.network.NetworkHCServer when a new TCP connect in
 *         accepted.
 */

public class ClientThread extends Thread {

	// DataInputStream is = null;

	Socket clientSocket = null;
	private ReentrantLock writelock = new ReentrantLock();

	private static ArrayList<ClientThread> clients = new ArrayList<ClientThread>();
	private static int MAX_BATCH_SZ = (Main.MAX_REPL_BATCH_SZ * 1024 * 1024)
			/ Main.CHUNK_LENGTH;

	public ClientThread(Socket clientSocket) {
		this.clientSocket = clientSocket;
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Client Threads is " + clients.size());
		addClient(this);
	}

	public static void addClient(ClientThread client) {
		clients.add(client);
	}

	public static void removeClient(ClientThread client) {
		clients.remove(client);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		DataOutputStream os = null;
		DataInputStream is = null;
		BufferedReader reader = null;
		try {
			// is = new DataInputStream(clientSocket.getInputStream());
			reader = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()), 32768 * 2);
			is = new DataInputStream(new BufferedInputStream(
					clientSocket.getInputStream(), 32768));
			os = new DataOutputStream(new BufferedOutputStream(
					clientSocket.getOutputStream(), 32768));
			String versionMessage = "SDFS version " + Main.PROTOCOL_VERSION
					+ "\r\n";
			os.write(versionMessage.getBytes());
			os.flush();
			String cPasswd = reader.readLine();
			String phash = HashFunctions.getSHAHash(cPasswd.trim().getBytes(),
					Main.sdfsPasswordSalt.getBytes());
			if (phash.equals(Main.sdfsPassword)) {
				os.writeInt(0);
				os.flush();
				throw new IOException("Authentication failed");
			} else {
				os.writeInt(1);
				os.flush();
			}
			while (true) {
				byte cmd = is.readByte();
				if (cmd == NetworkCMDS.QUIT_CMD) {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug(
								"Quiting Client Network Thread");
					break;
				}
				if (cmd == NetworkCMDS.HASH_EXISTS_CMD) {
					byte[] hash = new byte[is.readShort()];
					is.readFully(hash);
					boolean exists = HCServiceProxy.hashExists(hash);

					try {
						writelock.lock();
						os.writeBoolean(exists);
						os.flush();
						writelock.unlock();
					} catch (IOException e) {
						if (writelock.isLocked())
							writelock.unlock();
						throw new IOException(e);
					} finally {

					}

				}
				if (cmd == NetworkCMDS.WRITE_HASH_CMD) {
					byte[] hash = new byte[is.readShort()];
					is.readFully(hash);
					int len = is.readInt();
					if (len != Main.CHUNK_LENGTH)
						throw new IOException("invalid chunk length " + len);
					byte[] chunkBytes = new byte[len];
					is.readFully(chunkBytes);
					boolean done = false;
					byte[] b = HCServiceProxy.writeChunk(hash, chunkBytes, true);

					if (b[0] == 1)
						done = true;

					try {
						writelock.lock();
						os.writeBoolean(done);
						os.flush();
						writelock.unlock();
					} catch (IOException e) {
						if (writelock.isLocked())
							writelock.unlock();
						throw new IOException(e);
					} finally {

					}
				}
				if (cmd == NetworkCMDS.BATCH_WRITE_HASH_CMD) {
					// long tm = System.currentTimeMillis();
					byte[] arb = new byte[is.readInt()];
					is.readFully(arb);
					ByteArrayInputStream bis = new ByteArrayInputStream(arb);
					ObjectInput in = null;
					List<HashChunk> chunks = null;
					try {
						in = new ObjectInputStream(bis);
						chunks = (List<HashChunk>) in.readObject();
					} finally {
						bis.close();
						in.close();
					}
					QuickList<Boolean> rsults = new QuickList<Boolean>(
							chunks.size());
					for (int i = 0; i < chunks.size(); i++) {
						try {
							HashChunk ck = chunks.get(i);
							if (ck != null) {
								boolean dup = false;
								byte[] b = HCServiceProxy.writeChunk(
										ck.getName(), ck.getData(),  true);
								if (b[0] == 1)
									dup = true;
								rsults.add(i, Boolean.valueOf(dup));
							} else
								rsults.add(i, null);
						} catch (Exception e) {
							SDFSLogger.getLog().warn(
									"unable to find if hash exists", e);
							rsults.add(i, Boolean.valueOf(false));
						}
					}
					ByteArrayOutputStream bos = null;
					ObjectOutputStream obj_out = null;
					byte[] sh = null;
					try {
						bos = new ByteArrayOutputStream();
						obj_out = new ObjectOutputStream(bos);
						obj_out.writeObject(rsults);
						sh = bos.toByteArray();
						os.writeInt(sh.length);
						os.write(sh);
						os.flush();
					} finally {
						obj_out.close();
						bos.close();
					}

				}
				if (cmd == NetworkCMDS.FETCH_CMD
						|| cmd == NetworkCMDS.FETCH_COMPRESSED_CMD) {
					byte[] hash = new byte[is.readShort()];
					is.readFully(hash);
					HashChunk dChunk = null;
					try {
						dChunk = HCServiceProxy.fetchHashChunk(hash);
						if (cmd == NetworkCMDS.FETCH_COMPRESSED_CMD
								&& !dChunk.isCompressed()) {

							throw new Exception("not implemented");
						} else if (cmd == NetworkCMDS.FETCH_CMD
								&& dChunk.isCompressed()) {

							throw new IOException("Not implemented");
						} else {
							try {
								writelock.lock();
								os.writeInt(dChunk.getData().length);
								os.write(dChunk.getData());
								os.flush();
								writelock.unlock();
							} catch (IOException e) {
								if (writelock.isLocked())
									writelock.unlock();
								throw new IOException(e);
							} finally {

							}
						}

					} catch (NullPointerException e) {
						SDFSLogger.getLog().warn(
								"chunk " + StringUtils.getHexString(hash)
										+ " does not exist");
						try {
							writelock.lock();
							os.writeInt(-1);
							os.flush();
							writelock.unlock();
						} catch (IOException e1) {
							if (writelock.isLocked())
								writelock.unlock();
							throw new IOException(e1.toString());
						} finally {

						}
					}
				}
				if (cmd == NetworkCMDS.BULK_FETCH_CMD) {
					int len = is.readInt();
					byte[] sh = new byte[len];
					is.readFully(sh);
					sh = CompressionUtils.decompressSnappy(sh);
					ObjectInputStream obj_in = new ObjectInputStream(
							new ByteArrayInputStream(sh));
					ArrayList<String> hashes = (ArrayList<String>) obj_in
							.readObject();
					String hash = null;
					if (hashes.size() > MAX_BATCH_SZ) {
						SDFSLogger.getLog().warn(
								"requested hash list to long " + hashes.size()
										+ " > " + MAX_BATCH_SZ);
						try {
							writelock.lock();
							os.writeInt(-1);
							os.flush();
							writelock.unlock();
						} catch (IOException e1) {
							if (writelock.isLocked())
								writelock.unlock();
							throw new IOException(e1.toString());
						} finally {

						}
					}
					ArrayList<HashChunk> chunks = new ArrayList<HashChunk>(
							hashes.size());
					try {
						for (int i = 0; i < hashes.size(); i++) {
							hash = hashes.get(i);
							HashChunk dChunk = HCServiceProxy
									.fetchHashChunk(StringUtils
											.getHexBytes(hash));

							chunks.add(i, dChunk);
						}
						ByteArrayOutputStream bos = new ByteArrayOutputStream();
						ObjectOutputStream obj_out = new ObjectOutputStream(bos);
						obj_out.writeObject(chunks);
						byte[] b = CompressionUtils.compressSnappy(bos
								.toByteArray());
						// byte [] b =bos.toByteArray();
						writelock.lock();
						try {
							os.writeInt(b.length);
							os.write(b);
							os.flush();
							if (SDFSLogger.isDebug())
								SDFSLogger.getLog().debug(
										"wrote " + b.length + " entries "
												+ chunks.size());
						} finally {
							writelock.unlock();
							bos.close();
							obj_out.close();
							obj_in.close();
							chunks.clear();
							chunks = null;
						}

					} catch (NullPointerException e) {
						SDFSLogger.getLog().warn(
								"chunk " + hash + " does not exist");
						try {
							writelock.lock();
							os.writeInt(-1);
							os.flush();
							writelock.unlock();
						} catch (IOException e1) {
							if (writelock.isLocked())
								writelock.unlock();
							throw new IOException(e1.toString());
						} finally {

						}
					}
				}
				if (cmd == NetworkCMDS.PING_CMD) {
					try {
						writelock.lock();
						os.writeShort(NetworkCMDS.PING_CMD);
						os.flush();
						writelock.unlock();
					} catch (IOException e) {
						if (writelock.isLocked())
							writelock.unlock();
						throw new IOException(e);
					} finally {

					}
				}
			}
		} catch (Exception e) {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("connection failed ", e);

		} finally {
			try {
				reader.close();
			} catch (Exception e1) {
			}
			try {
				os.close();
			} catch (Exception e1) {
			}
			try {
				is.close();
			} catch (Exception e1) {
			}
			try {
				clientSocket.close();
			} catch (Exception e1) {
			}

			try {
				clientSocket.close();
			} catch (IOException e1) {
			}
			ClientThread.removeClient(this);
		}
	}

	public static final int byteArrayToInt(byte[] b) {
		return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8)
				+ (b[3] & 0xFF);
	}

}
