package org.opendedup.sdfs.mgmt.grpc.replication;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.X509TrustManager;

import org.apache.commons.io.FileUtils;
import org.opendedup.grpc.FileIOServiceGrpc;
import org.opendedup.grpc.FileIOServiceGrpc.FileIOServiceBlockingStub;
import org.opendedup.grpc.SDFSEventServiceGrpc;
import org.opendedup.grpc.SDFSEventServiceGrpc.SDFSEventServiceBlockingStub;
import org.opendedup.grpc.StorageServiceGrpc;
import org.opendedup.grpc.StorageServiceGrpc.StorageServiceBlockingStub;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.mgmt.grpc.IOServer;
import org.opendedup.sdfs.mgmt.grpc.tls.DynamicTrustManager;
import org.opendedup.sdfs.mgmt.grpc.tls.WatchForFile;
import org.opendedup.sdfs.notification.ReplicationImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.EasyX509ClientTrustManager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import org.opendedup.grpc.Storage.FileReplicationRequest.ReplicationFileLocation;

public class ReplicationClient {
    ManagedChannel channel = null;
    protected StorageServiceBlockingStub storageBlockingStub;
    protected FileIOServiceBlockingStub ioBlockingStub;
    protected SDFSEventServiceBlockingStub evtBlockingStub;
    public String url;
    public long volumeid;
    public boolean mtls;
    public Map<String, ImportFile> imports = new ConcurrentHashMap<String, ImportFile>();
    public Set<String> activeImports = new HashSet<String>();
    public long sequence = 0;
    public Thread downloadThread;
    public Thread listThread;
    public Thread syncThread;
    File jsonFile = null;
    protected Gson objGson = new GsonBuilder().setPrettyPrinting().create();
    protected int failRetries = 10;
    protected boolean closed = false;

    public ReplicationClient(String url, long volumeid, boolean mtls) {
        this.url = url;
        this.volumeid = volumeid;
        this.mtls = mtls;
    }

    public static void RecoverReplicationClients() throws IOException {
        File directory = new File(Main.volume.getReplPath() + File.separator);
        if (directory.exists()) {
            for (File jf : directory.listFiles()) {
                if (jf.getPath().endsWith(".json")) {
                    SDFSLogger.getLog().info("Reading From " + jf.getPath());
                    Type listType = new TypeToken<HashSet<String>>() {
                    }.getType();
                    Gson objGson = new GsonBuilder().setPrettyPrinting().create();
                    String content = FileUtils.readFileToString(jf, "UTF-8");
                    Set<String> jsonToMap = objGson.fromJson(content, listType);
                    ReplicationClient rc = null;
                    for (String id : jsonToMap) {
                        try {
                            ReplicationImportEvent evt = (ReplicationImportEvent) SDFSEvent.getEvent(id);
                            if (rc == null) {
                                rc = new ReplicationClient(evt.url, evt.volumeid, evt.mtls);
                                rc.connect();
                            }
                            rc.importFile(evt.src, evt.dst,evt);
                        } catch (Exception e) {
                            SDFSLogger.getLog().warn("recovery import failed for " + id, e);
                        }
                    }
                    if (rc != null) {
                        SDFSLogger.getLog()
                                .info("Recovered " + jsonToMap.size() + " Active Imports for " + rc.volumeid);
                    }
                }
            }
        }
    }

    public void connect() throws Exception {
        try {
            if (this.channel != null) {
                try {
                    this.channel.shutdown();
                    this.channel.awaitTermination(60, TimeUnit.SECONDS);
                    this.channel = null;
                    this.storageBlockingStub = null;
                    this.evtBlockingStub = null;
                    this.ioBlockingStub = null;

                } catch (Exception e) {
                }
            }
            String keydir = new File(Main.volume.getPath()).getParent() + File.separator + "keys";
            String certChainFilePath = keydir + File.separator + "tls_key.pem";
            String privateKeyFilePath = keydir + File.separator + "tls_key.key";
            String trustCertCollectionFilePath = keydir + File.separator + "signer_key.crt";
            Map<String, String> env = System.getenv();
            if (env.containsKey("SDFS_PRIVATE_KEY")) {
                privateKeyFilePath = env.get("SDFS_PRIVATE_KEY");
            }
            if (env.containsKey("SDFS_CERT_CHAIN")) {
                certChainFilePath = env.get("SDFS_CERT_CHAIN");
            }

            if (env.containsKey("SDFS_SIGNER_CHAIN")) {
                trustCertCollectionFilePath = env.get("SDFS_SIGNER_CHAIN");
            }
            boolean tls = false;
            String target = null;
            if (url.toLowerCase().startsWith("sdfs://")) {
                target = url.toLowerCase().replace("sdfs://", "");
            } else if (url.toLowerCase().startsWith("sdfss://")) {
                target = url.toLowerCase().replace("sdfss://", "");
                tls = true;
            } else {
                throw new IOException("invalid url");
            }
            String host = target.split(":")[0];
            int port = Integer.parseInt(target.split(":")[1]);
            int maxMsgSize = 240 * 1024 * 1024;
            if (mtls || tls) {

                this.channel = NettyChannelBuilder.forAddress(host, port)
                        .negotiationType(NegotiationType.TLS).maxInboundMessageSize(maxMsgSize)
                        .sslContext(
                                getSslContextBuilder(certChainFilePath, privateKeyFilePath, trustCertCollectionFilePath)
                                        .build())
                        .build();
            } else {
                this.channel = NettyChannelBuilder.forAddress(host, port).maxInboundMessageSize(maxMsgSize)
                        .negotiationType(NegotiationType.PLAINTEXT)
                        .build();
            }

            SDFSLogger.getLog().info("Replication connected to " + host + " " + port + " " + channel.toString()
                    + " connection state " + channel.getState(true));
            storageBlockingStub = StorageServiceGrpc.newBlockingStub(channel);
            evtBlockingStub = SDFSEventServiceGrpc.newBlockingStub(channel);
            ioBlockingStub = FileIOServiceGrpc.newBlockingStub(channel);
            File directory = new File(Main.volume.getReplPath() + File.separator);
            directory.mkdirs();
            jsonFile = new File(directory, "activereplications-" + this.volumeid + ".json");
            this.closed = false;

        } catch (Exception e) {
            this.channel = null;
            this.storageBlockingStub = null;
            this.evtBlockingStub = null;
            this.ioBlockingStub = null;
            throw e;
        }
    }

    public boolean isConnected() {
        if (channel == null) {
            return false;
        }
        return channel.getState(true) == ConnectivityState.READY;
    }

    public void checkConnection() {
        if (!this.isConnected()) {
            SDFSLogger.getLog().warn("Replication Client not connected for " + this.url + " volid " + this.volumeid);
            try {
                this.shutDown();
                this.connect();
                this.replicationSink();
            } catch (Exception e) {
                SDFSLogger.getLog().warn("Unable to reconnect to " + this.url + " volid " + this.volumeid, e);
            }
        }
    }

    private SslContextBuilder getSslContextBuilder(String certChainFilePath, String privateKeyFilePath,
            String trustCertCollectionFilePath) throws Exception {
        SslContextBuilder sslClientContextBuilder = null;
        if (!Main.authJarFilePath.equals("") && !Main.authClassInfo.equals("")) {

            EasyX509ClientTrustManager tm = new EasyX509ClientTrustManager();
            sslClientContextBuilder = SslContextBuilder.forClient().trustManager(tm).keyManager(IOServer.pvtKey,
                    IOServer.serverCertChain);
        } else {
            try {
                sslClientContextBuilder = SslContextBuilder.forClient();
                DynamicTrustManager tm = new DynamicTrustManager(new File(trustCertCollectionFilePath).getParent());
                sslClientContextBuilder.trustManager(tm);
                sslClientContextBuilder.clientAuth(ClientAuth.REQUIRE);
                WatchForFile wf = new WatchForFile(tm);
                Thread th = new Thread(wf);
                th.start();
            } catch (Exception e) {

                sslClientContextBuilder = SslContextBuilder.forClient().trustManager(new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new java.security.cert.X509Certificate[] {};
                    }
                });
            }
        }
        return GrpcSslContexts.configure(sslClientContextBuilder);
    }

    public SDFSEvent[] replicate(List<ReplicationFileLocation> locations) throws IOException {
        SDFSEvent[] evts = new SDFSEvent[locations.size()];
        if (evts.length > 10) {
            throw new IOException("The maximum number of replications is 10 ");
        }
        synchronized (activeImports) {
            for (int i = 0; i < evts.length; i++) {

                ReplicationFileLocation location = locations.get(i);
                ReplicationImportEvent evt = new ReplicationImportEvent(location.getSrcFilePath(),
                        location.getDstFilePath(), this.url, this.volumeid,
                        this.mtls, true);
                if (activeImports.contains(location.getDstFilePath())) {
                    evt.endEvent("Replication already occuring for" + location.getDstFilePath(), SDFSEvent.ERROR);
                } else {
                    activeImports.add(location.getDstFilePath());
                    SDFSLogger.getLog().info("Will Replicate " + location.getSrcFilePath() + " to "
                            + location.getDstFilePath() + " from " + url);
                    evt.persistEvent();
                    if (location.getSrcFilePath().equalsIgnoreCase(".")) {
                        if (downloadThread == null || !downloadThread.isAlive()) {
                            downloadThread = new Thread(new DownloadAll(this, evt));
                            downloadThread.start();
                        } else {
                            evt.endEvent("DownloadAll Thread already Active", SDFSEvent.WARN);
                        }
                    } else {
                        ImportFile fl = new ImportFile(location.getSrcFilePath(), location.getDstFilePath(), this, evt,
                                false);
                        Thread th = new Thread(fl);
                        th.start();
                    }
                }
                evts[i] = evt;
            }
            return evts;
        }
    }

    private SDFSEvent importFile(String src, String dst,ReplicationImportEvent evt) throws IOException {
        if (activeImports.contains(dst)) {
            evt.endEvent("Replication already occuring for" + dst, SDFSEvent.ERROR);
        } else {
            activeImports.add(dst);
            SDFSLogger.getLog().info("Will Replicate " + src + " to "
                    + dst + " from " + url);
            evt.persistEvent();
            if (src.equalsIgnoreCase(".")) {
                if (downloadThread == null || !downloadThread.isAlive()) {
                    downloadThread = new Thread(new DownloadAll(this, evt));
                    downloadThread.start();
                } else {
                    evt.endEvent("DownloadAll Thread already Active", SDFSEvent.WARN);
                }
            } else {
                ImportFile fl = new ImportFile(src, dst, this, evt,
                        true);
                Thread th = new Thread(fl);
                th.start();
            }
        }
        return evt;
    }

    public void replicationSink() throws IOException {
        SDFSLogger.getLog().info("Sink Started at sequence " + this.sequence);
        this.failRetries = 288;
        if (this.sequence == 0) {
            ReplicationImportEvent evt = new ReplicationImportEvent(".",
                    ".", this.url, this.volumeid,
                    this.mtls, false);
            if (downloadThread == null || !downloadThread.isAlive()) {
                downloadThread = new Thread(new DownloadAll(this, evt));
                downloadThread.start();
            } else {
                evt.endEvent("DownloadAll Thread already Active", SDFSEvent.WARN);
                throw new IOException("DownloadAll Thread already Active");
            }
        } else {
            this.listThread = new Thread(new ListReplLogs(this));
            this.listThread.start();
        }
        syncThread = new Thread(new ListenRepl(this));
        syncThread.start();
    }

    public void shutDown() {
        try {
            this.channel.shutdown();
            this.channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            SDFSLogger.getLog().warn("unable to shutdown client connection", e);
        }
        synchronized (this.activeImports) {
            this.closed = true;
            if (this.channel != null) {
                this.channel.shutdown();
            }
            if (this.listThread != null) {
                this.listThread.interrupt();
            }
            if (this.downloadThread != null) {
                this.downloadThread.interrupt();
            }
            if (this.downloadThread != null) {
                this.syncThread.interrupt();
            }
            String mapToJson = objGson.toJson(this.imports.keySet());
            try {
                FileUtils.writeStringToFile(jsonFile, mapToJson, Charset.forName("UTF-8"));
            } catch (IOException e) {
                SDFSLogger.getLog().warn("Unable to persist active imports", e);
            }
            this.activeImports.clear();
        }
    }

}
