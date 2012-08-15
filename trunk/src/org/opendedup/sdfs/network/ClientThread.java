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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.util.CompressionUtils;
import org.opendedup.util.SDFSLogger;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.StringUtils;

/**
 * @author Sam Silverberg This is the network class that is used within the
 *         Chunk store to service all client requests and responses. It is
 *         threaded and is spawned by @see
 *         com.annesam.sdfs.network.NetworkHCServer when a new TCP connect in
 *         accepted.
 */

class ClientThread extends Thread {

	// DataInputStream is = null;
	
	Socket clientSocket = null;
	private ReentrantLock writelock = new ReentrantLock();

	private static ArrayList<ClientThread> clients = new ArrayList<ClientThread>();
	private static int MAX_BATCH_SZ = (Main.MAX_REPL_BATCH_SZ*1024*1024)/Main.CHUNK_LENGTH;

	public ClientThread(Socket clientSocket) {
		this.clientSocket = clientSocket;
		SDFSLogger.getLog().debug("Client Threads is " + clients.size());
		addClient(this);
	}

	public static void addClient(ClientThread client) {
		clients.add(client);
	}

	public static void removeClient(ClientThread client) {
		clients.remove(client);
	}

	public void run() {
		DataOutputStream os = null;
		DataInputStream is = null;
		BufferedReader reader = null;
		try {
			// is = new DataInputStream(clientSocket.getInputStream());
			reader = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()), 32768 * 2);
			is = new DataInputStream(new BufferedInputStream(
					clientSocket.getInputStream(), 32768 * 2));
			os = new DataOutputStream(new BufferedOutputStream(
					clientSocket.getOutputStream()));
			String versionMessage = "SDFS version " + Main.PROTOCOL_VERSION
					+ "\r\n";
			os.write(versionMessage.getBytes());
			os.flush();
			String cPasswd = reader.readLine();
			if(cPasswd.trim().equals(Main.sdfsCliPassword)) {
				os.writeInt(0);
				os.flush();
				throw new IOException("Authentication failed");
			}else {
				os.writeInt(1);
				os.flush();
			}
			while (true) {
					byte cmd = is.readByte();
					if (cmd == NetworkCMDS.QUIT_CMD) {
						SDFSLogger.getLog().debug(
								"Quiting Client Network Thread");
						break;
					}
					if (cmd == NetworkCMDS.HASH_EXISTS_CMD) {
						short hops = is.readShort();
						byte[] hash = new byte[is.readShort()];
						is.readFully(hash);
						boolean exists = HashChunkService.hashExists(hash,hops);
						
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
					if (cmd == NetworkCMDS.WRITE_HASH_CMD
							|| cmd == NetworkCMDS.WRITE_COMPRESSED_CMD) {
						byte[] hash = new byte[is.readShort()];
						is.readFully(hash);
						int len = is.readInt();
						if(len != Main.CHUNK_LENGTH)
							throw new IOException("invalid chunk length " +len);
						byte[] chunkBytes = new byte[len];
						is.readFully(chunkBytes);
						boolean done = false;
						if (cmd == NetworkCMDS.WRITE_COMPRESSED_CMD) {
							done = HashChunkService.writeChunk(hash,
									chunkBytes, len, len, true);
						} else {
							done = HashChunkService.writeChunk(hash,
									chunkBytes, len, len, false);
						}
						
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
					if (cmd == NetworkCMDS.FETCH_CMD
							|| cmd == NetworkCMDS.FETCH_COMPRESSED_CMD) {
						byte[] hash = new byte[is.readShort()];
						is.readFully(hash);
						HashChunk dChunk = null;
						try {
							dChunk = HashChunkService.fetchChunk(hash);
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
						ObjectInputStream obj_in = new ObjectInputStream(new ByteArrayInputStream(sh));
						@SuppressWarnings("unchecked")
						ArrayList<String> hashes = (ArrayList<String>)obj_in.readObject();
						String hash = null;
						if(hashes.size() > MAX_BATCH_SZ){
							SDFSLogger.getLog().warn(
									"requested hash list to long " + hashes.size() + " > " + MAX_BATCH_SZ);
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
						ArrayList<HashChunk> chunks = new ArrayList<HashChunk>(hashes.size());
						try {
							for(int i = 0;i<hashes.size();i++) {
								hash = hashes.get(i);
								HashChunk dChunk = HashChunkService.fetchChunk(StringUtils.getHexBytes(hash));
								chunks.add(i, dChunk);
							}
							ByteArrayOutputStream bos = new ByteArrayOutputStream();
							ObjectOutputStream obj_out = new ObjectOutputStream(bos);
							obj_out.writeObject(chunks);
							byte [] b = CompressionUtils.compressSnappy(bos.toByteArray());
							//byte [] b =bos.toByteArray();
							writelock.lock();
							try {
								os.writeInt(b.length);
								os.write(b);
								os.flush();
								SDFSLogger.getLog().debug("wrote " + b.length + " entries " + chunks.size());
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
									"chunk " + hash
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
			SDFSLogger.getLog().debug("connection failed ",e);
			
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
