package org.opendedup.sdfs.mgmt.websocket;

import org.simpleframework.http.socket.Session;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.RandomGUID;
import org.simpleframework.http.Request;
import org.simpleframework.http.socket.DataFrame;
import org.simpleframework.http.socket.Frame;
import org.simpleframework.http.socket.FrameType;
import org.simpleframework.http.socket.FrameChannel;
import org.simpleframework.http.socket.FrameListener;
import org.simpleframework.http.socket.Reason;
import org.simpleframework.http.socket.service.Service;

public class MetaDataUpdate implements Service,Runnable {
	private final MetaDataUpdateListener listener;
	private final Map<String, FrameChannel> sockets;
	private final Set<String> users;

	public MetaDataUpdate() {
		sockets = new ConcurrentHashMap<String, FrameChannel>();
		listener = new MetaDataUpdateListener(this);
		this.users = new CopyOnWriteArraySet<String>();
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void connect(Session connection) {
		SDFSLogger.getLog().info("6");
		FrameChannel socket = connection.getChannel();
		Request req = connection.getRequest();
		String password = req.getQuery().get("password");
		String vol = req.getQuery().get("volumeid");
		SDFSLogger.getLog().info("7");
		boolean auth = false;
		SDFSLogger.getLog().info("8");
		if (vol == null) {
			vol = RandomGUID.getGuid();
		}
		if (Main.sdfsCliRequireAuth) {

			
			if (password != null) {
				String hash;
				try {
					hash = HashFunctions.getSHAHash(password.trim().getBytes(), Main.sdfsPasswordSalt.getBytes());
					if (hash.equals(Main.sdfsPassword)) {
						auth = true;
					}

				} catch (NoSuchAlgorithmException | UnsupportedEncodingException | NoSuchProviderException e) {
					SDFSLogger.getLog().error("unable to authenitcate user", e);
				}

			} else {
				SDFSLogger.getLog().warn("could not authenticate user to cli");
			}
		} else {
			auth = true;
		}
		if (auth == true) {
			try {
				SDFSLogger.getLog().info("9");
				socket.register(listener);
				join(vol, socket);
				SDFSLogger.getLog().info("10");
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to connect", e);
			}
		} else
			try {
				SDFSLogger.getLog().info("11");
				connection.getChannel().close();
			} catch (IOException e) {
				SDFSLogger.getLog().error("unable to close connection", e);
			}

	}

	public void join(String user, FrameChannel operation) {
		users.add(user);
		sockets.put(user, operation);
	}

	public void leave(String user, FrameChannel operation) {
		sockets.remove(user);
		users.remove(user);
	}

	public void distribute(Frame frame) {
		try {
			for (String user : users) {
				FrameChannel operation = sockets.get(user);

				try {

					operation.send(frame);
					SDFSLogger.getLog().info("sent " + frame.getText());
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to send message", e);
					sockets.remove(user);
					users.remove(user);
					operation.close();
					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class MetaDataUpdateListener implements FrameListener {
		

		private MetaDataUpdateListener(MetaDataUpdate service) {
			
		}

		@Override
		public void onClose(Session arg0, Reason arg1) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onError(Session arg0, Exception arg1) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onFrame(Session arg0, Frame arg1) {
			// TODO Auto-generated method stub

		}

	}

	@Override
	public void run() {
		for(;;) {
			try {
			Frame replay = new DataFrame(FrameType.TEXT, "poop");
			this.distribute(replay);
			Thread.sleep(5000);
			}catch(Exception e) {
				SDFSLogger.getLog().error("unable to send message", e);
			}
		}
		
	}

}
