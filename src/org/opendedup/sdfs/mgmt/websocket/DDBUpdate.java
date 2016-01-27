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
import org.opendedup.sdfs.io.events.SFileWritten;
import org.opendedup.util.RandomGUID;
import org.simpleframework.http.Request;
import org.simpleframework.http.socket.DataFrame;
import org.simpleframework.http.socket.Frame;
import org.simpleframework.http.socket.FrameType;
import org.simpleframework.http.socket.FrameChannel;
import org.simpleframework.http.socket.FrameListener;
import org.simpleframework.http.socket.Reason;
import org.simpleframework.http.socket.service.Service;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;


public class DDBUpdate implements Service {
	private final DDBUpdateListener listener;
	private final Map<String, FrameChannel> sockets;
	private final Set<String> users;
	
	

	public DDBUpdate() {
		sockets = new ConcurrentHashMap<String, FrameChannel>();
		listener = new DDBUpdateListener(this);
		this.users = new CopyOnWriteArraySet<String>();
	}

	@Override
	public void connect(Session connection) {
		FrameChannel socket = connection.getChannel();
		Request req = connection.getRequest();
		String password = req.getQuery().get("password");
		String vol = req.getQuery().get("volumeid");
		boolean auth = false;
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
				socket.register(listener);
				join(vol, socket);
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to connect", e);
			}
		} else
			try {
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
	
	@Subscribe
	@AllowConcurrentEvents
	public void ddbFileWritten(SFileWritten evt) throws IOException {
		Frame replay = new DataFrame(FrameType.TEXT, evt.sf.getDatabasePath());
		this.distribute(replay);
	}

	private static class DDBUpdateListener implements FrameListener {
		

		private DDBUpdateListener(DDBUpdate service) {
			
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

}
