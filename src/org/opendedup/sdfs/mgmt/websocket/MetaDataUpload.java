/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.mgmt.websocket;

import org.simpleframework.http.socket.Session
;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.sdfs.io.events.MFileRenamed;
import org.opendedup.sdfs.io.events.MFileWritten;
import org.opendedup.sdfs.io.events.MMetaUpdated;
import org.opendedup.sdfs.io.events.SFileDeleted;
import org.opendedup.sdfs.io.events.SFileWritten;
import org.opendedup.util.RandomGUID;
import org.simpleframework.http.Request;
import org.simpleframework.http.socket.DataFrame;
import org.simpleframework.http.socket.Frame;
import org.simpleframework.http.socket.FrameChannel;
import org.simpleframework.http.socket.FrameListener;
import org.simpleframework.http.socket.FrameType;
import org.simpleframework.http.socket.Reason;
import org.simpleframework.http.socket.service.Service;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;

public class MetaDataUpload implements Service {
	private final MetaDataUpdateListener listener;
	private final Map<String, FrameChannel> sockets;
	private final Set<String> users;
	private static ReentrantLock iLock = new ReentrantLock(true);
	private ConcurrentHashMap<String, ReentrantLock> activeTasks = new ConcurrentHashMap<String, ReentrantLock>();

	public MetaDataUpload() {
		sockets = new ConcurrentHashMap<String, FrameChannel>();
		listener = new MetaDataUpdateListener(this);
		this.users = new CopyOnWriteArraySet<String>();
		FileReplicationService.registerEvents(this);
		if(Main.matcher != null) {
			Main.matcher.registerEvents(this);
		}
	}

	private ReentrantLock getLock(String st) {
		iLock.lock();
		try {
			ReentrantLock l = activeTasks.get(st);
			if (l == null) {
				l = new ReentrantLock(true);
				activeTasks.put(st, l);
			}
			return l;
		} finally {
			iLock.unlock();
		}
	}

	private void removeLock(String st) {
		iLock.lock();
		try {
			ReentrantLock l = activeTasks.get(st);
			try {

				if (l != null && !l.hasQueuedThreads()) {
					this.activeTasks.remove(st);
				}
			} finally {
				if (l != null)
					l.unlock();
			}
		} finally {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"hmpa size=" + this.activeTasks.size());
			iLock.unlock();
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void metaFileDeleted(MFileDeleted evt) throws IOException {
		try {
			ReentrantLock l = this.getLock(evt.mf.getPath());
			l.lock();
			Frame replay = new DataFrame(FrameType.TEXT, evt.toJSON());
			this.distribute(replay);
		} catch (Exception e) {
			SDFSLogger.getLog()
					.error("unable to delete " + evt.mf.getPath(), e);
			throw new IOException(e);
		} finally {
			removeLock(evt.mf.getPath());
		}
	}
	@Subscribe
	@AllowConcurrentEvents
	public void metaFileRenamed(MFileRenamed evt) {
		try {
			ReentrantLock l = this.getLock(evt.mf.getPath());
			l.lock();
			SDFSLogger.getLog().info(evt.toJSON());
			
			Frame replay = new DataFrame(FrameType.TEXT, evt.toJSON());
			this.distribute(replay);
		} catch (Exception e) {
			SDFSLogger.getLog()
					.error("unable to rename " + evt.mf.getPath(), e);
		} finally {
			removeLock(evt.mf.getPath());
		}

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
					hash = HashFunctions.getSHAHash(password.trim().getBytes(),
							Main.sdfsPasswordSalt.getBytes());
					if (hash.equals(Main.sdfsPassword)) {
						auth = true;
					}
				} catch (NoSuchAlgorithmException
						| UnsupportedEncodingException
						| NoSuchProviderException e) {
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
				} catch (Exception e) {
					SDFSLogger.getLog().debug("unable to send message", e);
					try {
						sockets.remove(user);
						users.remove(user);
						operation.close();
					} catch (Exception e1) {

					}
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to send message",e);
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void metaFileWritten(MFileWritten evt) throws IOException {
		try {
			ReentrantLock l = this.getLock(evt.mf.getPath());
			l.lock();
			Frame replay = new DataFrame(FrameType.TEXT, evt.toJSON());
			this.distribute(replay);
		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to write " + evt.mf.getPath(), e);
		} finally {
			removeLock(evt.mf.getPath());
		}
	}
	
	@Subscribe
	@AllowConcurrentEvents
	public void mmetaUpdate(MMetaUpdated evt) throws IOException {
		try {
			ReentrantLock l = this.getLock(evt.mf.getPath());
			l.lock();
			Frame replay = new DataFrame(FrameType.TEXT, evt.toJSON());
			this.distribute(replay);
		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to write " + evt.mf.getPath(), e);
		} finally {
			removeLock(evt.mf.getPath());
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void ddbFileWritten(SFileWritten evt) throws IOException {
		try {
			ReentrantLock l = this.getLock(evt.sf.getGUID());
			l.lock();
			Frame replay = new DataFrame(FrameType.TEXT, evt.toJSON());
			this.distribute(replay);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write " + evt.sf.getGUID(), e);
		} finally {
			removeLock(evt.sf.getGUID());
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void ddbFileDeleted(SFileDeleted evt) throws IOException {
		try {
			ReentrantLock l = this.getLock(evt.sf.getGUID());
			l.lock();
			Frame replay = new DataFrame(FrameType.TEXT, evt.toJSON());
			this.distribute(replay);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write " + evt.sf.getGUID(), e);
		} finally {
			removeLock(evt.sf.getGUID());
		}
	}

	private static class MetaDataUpdateListener implements FrameListener {

		private MetaDataUpdateListener(MetaDataUpload service) {

		}

		@Override
		public void onClose(Session ses, Reason arg1) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onError(Session ses, Exception arg1) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onFrame(Session ses, Frame arg1) {

		}

	}

}
