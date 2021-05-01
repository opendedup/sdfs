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
package org.opendedup.sdfs.mgmt.mqtt;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Set;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;

import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.sdfs.io.events.MFileRenamed;
import org.opendedup.sdfs.io.events.MFileWritten;
import org.opendedup.sdfs.io.events.MMetaUpdated;
import org.opendedup.sdfs.io.events.SFileDeleted;
import org.opendedup.sdfs.io.events.SFileWritten;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import org.opendedup.sdfs.mgmt.DeleteFileCmd;
import org.opendedup.sdfs.mgmt.GetCloudFile;

public class MetaDataPush {
	private static ReentrantLock iLock = new ReentrantLock(true);
	private ConcurrentHashMap<String, ReentrantLock> activeTasks = new ConcurrentHashMap<String, ReentrantLock>();
	private Channel channel;
	private Connection connection;
	private final String topic;

	public MetaDataPush(String host, int port, String userName, String password, String topic)
			throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setPort(port);
		this.topic = topic;
		if (userName != null)
			factory.setUsername(userName);
		if (password != null)
			factory.setPassword(password);
		connection = factory.newConnection();
		channel = connection.createChannel();
		channel.exchangeDeclare(this.topic, "fanout");
		FileReplicationService.registerEvents(this);
		
		new MetaDataSubscriber(this.channel, this.topic);
		SDFSLogger.getLog().info(String.format("Connected to MQTT %s:%d", host, port));
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
				SDFSLogger.getLog().debug("hmpa size=" + this.activeTasks.size());
			iLock.unlock();
		}
	}

	@Subscribe
	@AllowConcurrentEvents
	public void metaFileDeleted(MFileDeleted evt) throws IOException {
		try {
			ReentrantLock l = this.getLock(evt.mf.getPath());
			l.lock();

			this.channel.basicPublish(this.topic, "", null, evt.toJSON().getBytes("UTF-8"));
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to delete " + evt.mf.getPath(), e);
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
			SDFSLogger.getLog().debug(evt.toJSON());

			this.channel.basicPublish(this.topic, "", null, evt.toJSON().getBytes("UTF-8"));
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to rename " + evt.mf.getPath(), e);
		} finally {
			removeLock(evt.mf.getPath());
		}

	}

	@Subscribe
	@AllowConcurrentEvents
	public void metaFileWritten(MFileWritten evt) throws IOException {
		try {
			ReentrantLock l = this.getLock(evt.mf.getPath());
			l.lock();
			this.channel.basicPublish(this.topic, "", null, evt.toJSON().getBytes("UTF-8"));
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
			this.channel.basicPublish(this.topic, "", null, evt.toJSON().getBytes("UTF-8"));
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
			this.channel.basicPublish(this.topic, "", null, evt.toJSON().getBytes("UTF-8"));
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
			this.channel.basicPublish(this.topic, "", null, evt.toJSON().getBytes("UTF-8"));
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write " + evt.sf.getGUID(), e);
		} finally {
			removeLock(evt.sf.getGUID());
		}
	}

	private static class MetaDataSubscriber {
		private final Channel channel;
		private final ConcurrentHashMap<String, Delivery> updateMap = new ConcurrentHashMap<String, Delivery>();
		private ReentrantLock iLock = new ReentrantLock(true);
		long updateTime = 20000;
		long checkTime = 5000;
		private ConcurrentHashMap<String, ReentrantLock> activeTasks = new ConcurrentHashMap<String, ReentrantLock>();
		UpdateProcessor up = null;

		private MetaDataSubscriber(Channel channel, String topic) throws IOException {
			this.channel = channel;
			String queueName = this.channel
					.queueDeclare(Long.toString(Main.volume.getSerialNumber()), true, false, false, null).getQueue();
			this.channel.queueBind(queueName, topic, "");
			up = new UpdateProcessor(this);
			DeliverCallback deliverCallback = (consumerTag, delivery) -> {
				String message = new String(delivery.getBody(), "UTF-8");
				SDFSLogger.getLog()
						.info(" [x] Received '" + message + "' tag = " + delivery.getEnvelope().getDeliveryTag());
				VolumeEvent evt = new VolumeEvent(message);
				ReentrantLock l = this.getLock(evt.getTarget());
				l.lock();
				try {
					if (evt.volumeID == Main.volume.getSerialNumber()) {
						this.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
					} else if (this.updateMap.containsKey(evt.getTarget())) {
						VolumeEvent _evt = new VolumeEvent(
								new String(this.updateMap.get(evt.getTarget()).getBody(), "UTF-8"));
						if (evt.isDBDelete() || evt.isMFDelete() || evt.isDBUpdate()) {
							if (_evt.getVolumeTS() < evt.getVolumeTS()) {
								Delivery d = this.updateMap.remove(evt.getTarget());
								this.channel.basicAck(d.getEnvelope().getDeliveryTag(), false);
								this.updateMap.put(evt.getTarget(), delivery);
							} else {
								SDFSLogger.getLog().info("ignorining event " + evt.getJsonString()
										+ "because timestamp " + evt.getVolumeTS() + " < " + _evt.getVolumeTS());
								this.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

							}
						} else {
							this.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
						}
					} else {
						this.updateMap.put(evt.getTarget(), delivery);
					}
				} finally {
					removeLock(evt.getTarget());
				}

			};
			this.channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
			});

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
				iLock.unlock();
			}
		}
	}

	private static class UpdateProcessor implements Runnable {
		MetaDataSubscriber s = null;
		Thread th = null;

		protected UpdateProcessor(MetaDataSubscriber s) {
			this.s = s;
			th = new Thread(this);
			th.start();
		}

		@Override
		public void run() {
			for (;;) {
				try {
					Thread.sleep(s.checkTime);
					synchronized (th) {
						Set<String> set = s.updateMap.keySet();
						for (String file : set) {
							ReentrantLock l = s.getLock(file);
							l.lock();
							try {
								VolumeEvent evt = new VolumeEvent(new String(s.updateMap.get(file).getBody(), "UTF-8"));
								long ts = System.currentTimeMillis() - evt.getVolumeTS();
								if (ts > s.updateTime) {
									if (evt.getVolumeID() != Main.volume.getSerialNumber()) {
										if (evt.isMFDelete()) {
											try {
												SDFSLogger.getLog().info("Deleting File [" + file + "] ");

												String msg = new DeleteFileCmd().getResult("deletefile", file,
														evt.getChangeID(), true, true);
												SDFSLogger.getLog().info(msg);
												Delivery d = s.updateMap.remove(file);
												s.channel.basicAck(d.getEnvelope().getDeliveryTag(), false);
											} catch (Exception e) {
												SDFSLogger.getLog().info("unable to delete mf " + file, e);
											}
										} else if (evt.isDBUpdate()) {
											try {
												SDFSLogger.getLog().info("Updating File [" + file + "] ");
												new GetCloudFile().getResult(file, null, true, evt.getChangeID());
												Delivery d = s.updateMap.remove(file);
												s.channel.basicAck(d.getEnvelope().getDeliveryTag(), false);
											} catch (Exception e) {
												SDFSLogger.getLog().info("unable to update ddb " + file + " on ", e);
											}
										} else {
											
											Delivery d = s.updateMap.remove(file);
											s.channel.basicAck(d.getEnvelope().getDeliveryTag(), false);
										}
									} else {
										Delivery d = s.updateMap.remove(file);
										s.channel.basicAck(d.getEnvelope().getDeliveryTag(), false);
										SDFSLogger.getLog().debug("ignoring");
									}
								} else {
									SDFSLogger.getLog().debug("Waiting on event time diff =" + ts);

								}

							} finally {
								s.removeLock(file);
							}
						}
					}
				} catch (InterruptedException e) {

				} catch (Exception e) {
					SDFSLogger.getLog().error("unable run update thread ", e);
				}

			}

		}
	}

}
