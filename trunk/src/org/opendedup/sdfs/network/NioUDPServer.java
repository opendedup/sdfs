package org.opendedup.sdfs.network;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*; // bug? redundant with previous one??
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HashChunkService;

/**
 * 
 * @author Sam Silverberg
 * 
 *         This is a UDP server class that can be used to serve client requests
 *         within the chunk server. It servers a similar function to @see
 *         com.annesam.sdfs.network.ClientThread . In some cases in may improve
 *         client performance to enable this function on the server. The UDP
 *         server will service :
 * 
 *         - HASH_EXISTS requests - CLAIM_HASH requests
 * 
 *         To enable the UDP server within the chunk store the config option
 *         use-udp="true must be set.
 * 
 * 
 */

public class NioUDPServer implements Runnable {

	int datagramSize = 36;

	private boolean closed = false;
	private transient static Logger log = Logger.getLogger("sdfs");

	NioUDPServer() {
		Thread th = new Thread(this);
		th.start();
	}

	public static void main(String args[]) {
		Main.serverHostName = "localhost";
		Main.serverPort = 2222;
		new NioUDPServer();
	}

	public void close() {
		this.closed = true;
	}

	public void run() {
		try {
			log.info("Starting UDP Server");
			Random theRandom = new Random();
			InetSocketAddress theInetSocketAddress = new InetSocketAddress(
					Main.serverHostName, Main.serverPort);

			// make a DatagramChannel
			DatagramChannel theDatagramChannel = DatagramChannel.open();
			theDatagramChannel.bind(theInetSocketAddress);

			// A channel must first be placed in nonblocking mode
			// before it can be registered with a selector
			theDatagramChannel.configureBlocking(false);
			// instantiate a selector
			Selector theSelector = Selector.open();

			// register the selector on the channel to monitor reading
			// datagrams on the DatagramChannel
			theDatagramChannel.register(theSelector, SelectionKey.OP_READ);

			log.info("UDP Server Started on " + theInetSocketAddress);

			// send and read concurrently, but do not block on read:

			while (!this.closed) {
				int keys = theSelector.select(500);
				// which comes first, next send or a read?
				// in case millisecsUntilSendNextDatagram <= 0 go right to send
				if (keys > 0) {
					try {
						Iterator<SelectionKey> iter = theSelector
								.selectedKeys().iterator();
						ByteBuffer buf = ByteBuffer.allocateDirect(33);
						ByteBuffer resp = ByteBuffer.allocateDirect(2);
						SelectionKey key = null;
						while (iter.hasNext()) {
							try {
								key = iter.next();
								if (key.isReadable()) {
									DatagramChannel ch = (DatagramChannel) key
											.channel();
									InetSocketAddress addr = (InetSocketAddress) ch
											.receive(buf);
									buf.flip();
									byte cmd = buf.get();
									byte[] hash = new byte[16];
									buf.clear();
									boolean exists = false;
									if (cmd == NetworkCMDS.HASH_EXISTS_CMD)
										exists = HashChunkService
												.hashExists(hash);
									// boolean exists = true;
									if (exists)
										resp.putShort((short) 1);
									else
										resp.putShort((short) 0);
									resp.flip();
									ch.send(resp, addr);
									resp.clear();
								}

							} catch (Exception e) {
								log.log(Level.WARNING,
										"unable to process hash request", e);
							} finally {
								iter.remove();
								resp.clear();
								buf.clear();
							}
						}
					} catch (Exception e) {
						log.log(Level.WARNING,
								"unable to process hash request", e);
					}

				}
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "unable to run udp server", e);
			return;
		}

	}
}
