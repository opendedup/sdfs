package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.mgmt.grpc.IOServer;
import org.opendedup.sdfs.mgmt.mqtt.MetaDataPush;
import org.opendedup.util.FindOpenPort;
import org.opendedup.util.KeyGenerator;

public class MgmtWebServer {
	private static IOServer grpcServer = null;

	public static void start(boolean useSSL) throws IOException {

		if (Main.sdfsCliEnabled) {

			if (useSSL && Main.jarFilePath.equals("") && Main.classInfo.equals("")) {
				String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
				String key = keydir + File.separator + "tls_key.pem";
				if (!new File(key).exists() || !IOServer.keyFileExists()) {
					KeyGenerator.generateKey(new File(key));
				}
			}
			Main.sdfsCliPort = FindOpenPort.pickFreePort(Main.sdfsCliPort);
			grpcServer = new IOServer();
			grpcServer.start(useSSL, Main.sdfsCliRequireMutualTLSAuth, Main.sdfsCliListenAddr, Main.sdfsCliPort);

			if (Main.volume.rabbitMQNode != null) {
				try {
					new MetaDataPush(Main.volume.rabbitMQNode, Main.volume.rabbitMQPort, Main.volume.rabbitMQUser,
							Main.volume.rabbitMQPassword, Main.volume.rabbitMQTopic);

				} catch (TimeoutException e) {
					throw new IOException(e);
				}
			}
			if (Main.volume.pubsubTopic != null) {
				try {
					new org.opendedup.sdfs.mgmt.pubsub.MetaDataPush(Main.volume.pubsubTopic,
							Main.volume.pubsubSubscription, Main.volume.gcpProject, Main.volume.gcpCredsPath);
				} catch (TimeoutException e) {
					throw new IOException(e);
				}
			}

		}
	}

	public static void stop() {
		try {
			grpcServer.stop();
		} catch (Throwable e) {
			SDFSLogger.getLog().error("unable to stop Web Management Server", e);
		}
	}

}
