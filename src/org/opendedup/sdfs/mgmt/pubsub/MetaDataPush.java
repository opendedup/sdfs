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
package org.opendedup.sdfs.mgmt.pubsub;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.sdfs.io.events.MFileRenamed;
import org.opendedup.sdfs.io.events.MFileWritten;
import org.opendedup.sdfs.io.events.MMetaUpdated;
import org.opendedup.sdfs.io.events.SFileDeleted;
import org.opendedup.sdfs.io.events.SFileWritten;
import org.opendedup.sdfs.mgmt.DeleteFileCmd;
import org.opendedup.sdfs.mgmt.GetCloudFile;
import org.opendedup.sdfs.mgmt.mqtt.VolumeEvent;

public class MetaDataPush {
	private static ReentrantLock iLock = new ReentrantLock(true);
	private ConcurrentHashMap<String, ReentrantLock> activeTasks = new ConcurrentHashMap<String, ReentrantLock>();
	private Publisher publisher;

	public MetaDataPush(String topicName, String subName, String project, String credsPath)
			throws IOException, TimeoutException {
		TopicName tn = TopicName.of(project, topicName);
		PublisherStubSettings.Builder stubSettings = PublisherStubSettings.newBuilder();
		if (credsPath != null) {
			Credentials creds = ServiceAccountCredentials.fromStream(new FileInputStream(credsPath));
			stubSettings.setCredentialsProvider(FixedCredentialsProvider.create(creds));
		}
		TopicAdminClient topicAdminClient = TopicAdminClient.create(stubSettings.build().createStub());
		try {
			Topic t = topicAdminClient.createTopic(tn.toString());
			SDFSLogger.getLog().debug("Created topic: " + t.getAllFields());
		} catch (com.google.api.gax.rpc.AlreadyExistsException e) {
			SDFSLogger.getLog().debug("Topic Alread Created");
		}
		Publisher.Builder b = Publisher.newBuilder(tn);
		if (credsPath != null) {
			Credentials creds = ServiceAccountCredentials.fromStream(new FileInputStream(credsPath));
			b.setCredentialsProvider(FixedCredentialsProvider.create(creds));
		}
		b.setEnableMessageOrdering(true);
		publisher = b.build();
		FileReplicationService.registerEvents(this);

		new MetaDataSubscriber(topicName, subName, project, credsPath);
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
			ByteString data = ByteString.copyFromUtf8(evt.toJSON());
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
			publisher.publish(pubsubMessage);
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
			ByteString data = ByteString.copyFromUtf8(evt.toJSON());
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
			publisher.publish(pubsubMessage);
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
			ByteString data = ByteString.copyFromUtf8(evt.toJSON());
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
			publisher.publish(pubsubMessage);
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
			ByteString data = ByteString.copyFromUtf8(evt.toJSON());
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
			publisher.publish(pubsubMessage);
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
			ByteString data = ByteString.copyFromUtf8(evt.toJSON());
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
			publisher.publish(pubsubMessage);
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
			ByteString data = ByteString.copyFromUtf8(evt.toJSON());
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
			publisher.publish(pubsubMessage);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to write " + evt.sf.getGUID(), e);
		} finally {
			removeLock(evt.sf.getGUID());
		}
	}

	private static class MetaDataSubscriber {
		private Subscription subscription;
		private final ConcurrentHashMap<String, PSMsg> updateMap = new ConcurrentHashMap<String, PSMsg>();
		private ReentrantLock iLock = new ReentrantLock(true);
		long updateTime = 20000;
		long checkTime = 5000;
		private ConcurrentHashMap<String, ReentrantLock> activeTasks = new ConcurrentHashMap<String, ReentrantLock>();

		private MetaDataSubscriber(String topic, String subName, String project, String credsFile) throws IOException {
			SubscriberStubSettings.Builder stubSettings = SubscriberStubSettings.newBuilder();
			if (credsFile != null) {
				Credentials creds = ServiceAccountCredentials.fromStream(new FileInputStream(credsFile));
				stubSettings.setCredentialsProvider(FixedCredentialsProvider.create(creds));
			}
			SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient
					.create(stubSettings.build().createStub());
			if (!subName.startsWith("sdfs")) {
				subName = "sdfs" + subName;
			}
			ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(project, subName);
			TopicName tn = TopicName.of(project, topic);

			Subscription.Builder b = Subscription.newBuilder().setName(subscriptionName.toString())
					.setTopic(tn.toString()).setEnableMessageOrdering(true).setAckDeadlineSeconds(600);
			try {
				subscription = subscriptionAdminClient.createSubscription(b.build());

				SDFSLogger.getLog().debug("Created a subscription with ordering: " + subscription.getAllFields());
			} catch (com.google.api.gax.rpc.AlreadyExistsException e) {

			}

			new UpdateProcessor(this);
			MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
				String mString = message.getData().toStringUtf8();
				SDFSLogger.getLog().debug(" [x] Received '" + message.getMessageId());
				VolumeEvent evt = new VolumeEvent(mString);
				ReentrantLock l = this.getLock(evt.getTarget());
				l.lock();
				try {
					if (evt.volumeID == Main.volume.getSerialNumber()) {
						consumer.ack();
					} else if (this.updateMap.containsKey(evt.getTarget())) {
						VolumeEvent _evt = new VolumeEvent(
								this.updateMap.get(evt.getTarget()).message.getData().toStringUtf8());
						if (evt.isDBDelete() || evt.isMFDelete() || evt.isDBUpdate()) {
							if (_evt.getVolumeTS() < evt.getVolumeTS()) {
								PSMsg d = this.updateMap.remove(evt.getTarget());
								d.consumer.ack();
								PSMsg n = new PSMsg();
								n.consumer = consumer;
								n.message = message;
								this.updateMap.put(evt.getTarget(), n);
							} else {
								SDFSLogger.getLog().debug("ignorining event " + evt.getJsonString()
										+ "because timestamp " + evt.getVolumeTS() + " < " + _evt.getVolumeTS());
								consumer.ack();
							}
						} else {
							consumer.ack();
						}
					} else {
						PSMsg n = new PSMsg();
						n.consumer = consumer;
						n.message = message;
						this.updateMap.put(evt.getTarget(), n);
					}
				} finally {
					removeLock(evt.getTarget());
				}

			};
			Subscriber.Builder sb = Subscriber.newBuilder(subscriptionName, receiver);
			if (credsFile != null) {
				Credentials creds = ServiceAccountCredentials.fromStream(new FileInputStream(credsFile));
				sb.setCredentialsProvider(FixedCredentialsProvider.create(creds));
			}
			Subscriber subscriber = sb.build();
			subscriber.startAsync();

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

	private static class PSMsg {
		PubsubMessage message;
		AckReplyConsumer consumer;
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
								VolumeEvent evt = new VolumeEvent(
										s.updateMap.get(file).message.getData().toStringUtf8());
								long ts = System.currentTimeMillis() - evt.getVolumeTS();
								if (ts > s.updateTime) {
									if (evt.getVolumeID() != Main.volume.getSerialNumber()) {
										if (evt.isMFDelete()) {
											try {
												SDFSLogger.getLog().debug("Deleting File [" + file + "] ");

												String msg = new DeleteFileCmd().getResult("deletefile", file,
														evt.getChangeID(), true, true);
												SDFSLogger.getLog().debug(msg);
												s.updateMap.remove(file).consumer.ack();
											} catch (Exception e) {
												SDFSLogger.getLog().debug("unable to delete mf " + file, e);
											}
										} else if (evt.isDBUpdate()) {
											try {
												SDFSLogger.getLog().debug("Updating File [" + file + "] ");
												new GetCloudFile().getResult(file, null, true, evt.getChangeID());
												s.updateMap.remove(file).consumer.ack();
											} catch (Exception e) {
												SDFSLogger.getLog().debug("unable to update ddb " + file + " on ", e);
											}
										} else {

											s.updateMap.remove(file).consumer.ack();
										}
									} else {
										s.updateMap.remove(file).consumer.ack();
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
