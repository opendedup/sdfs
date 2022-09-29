package org.opendedup.sdfs.mgmt.grpc.replication;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.X509TrustManager;

import org.apache.commons.io.FileUtils;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.RocksDBMap.ProcessPriorityThreadFactory;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.grpc.FileIOServiceGrpc;
import org.opendedup.grpc.FileIOServiceGrpc.FileIOServiceBlockingStub;
import org.opendedup.grpc.FileInfo.FileInfoRequest;
import org.opendedup.grpc.FileInfo.FileInfoResponse;
import org.opendedup.grpc.FileInfo.FileMessageResponse;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse;
import org.opendedup.grpc.SDFSEventServiceGrpc;
import org.opendedup.grpc.SDFSEventServiceGrpc.SDFSEventServiceBlockingStub;
import org.opendedup.grpc.Storage.ChunkEntry;
import org.opendedup.grpc.Storage.GetChunksRequest;
import org.opendedup.grpc.Storage.GetChunksRequest.Builder;
import org.opendedup.grpc.Storage.MetaDataDedupeFileRequest;
import org.opendedup.grpc.Storage.MetaDataDedupeFileResponse;
import org.opendedup.grpc.Storage.RestoreArchivesRequest;
import org.opendedup.grpc.Storage.RestoreArchivesResponse;
import org.opendedup.grpc.Storage.SparseDataChunkP;
import org.opendedup.grpc.Storage.SparseDedupeFileRequest;
import org.opendedup.grpc.Storage.VolumeEvent;
import org.opendedup.grpc.Storage.VolumeEventListenRequest;
import org.opendedup.grpc.Storage.actionType;
import org.opendedup.grpc.StorageServiceGrpc;
import org.opendedup.grpc.StorageServiceGrpc.StorageServiceBlockingStub;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.mgmt.CloseFile;
import org.opendedup.sdfs.mgmt.grpc.FileIOServiceImpl;
import org.opendedup.sdfs.mgmt.grpc.IOServer;
import org.opendedup.sdfs.mgmt.grpc.tls.DynamicTrustManager;
import org.opendedup.sdfs.mgmt.grpc.tls.WatchForFile;
import org.opendedup.sdfs.notification.ReplicationImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.EasyX509ClientTrustManager;
import org.opendedup.util.StringUtils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;

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
    private StorageServiceBlockingStub storageBlockingStub;
    private FileIOServiceBlockingStub ioBlockingStub;
    private SDFSEventServiceBlockingStub evtBlockingStub;
    public String url;
    public long volumeid;
    public boolean mtls;
    public Map<String, ImportFile> imports = new ConcurrentHashMap<String, ImportFile>();
    public Set<String> activeImports = new HashSet<String>();
    public long sequence = 0;
    public Thread downloadThread;
    public Thread listThread;
    public Thread syncThread;
    private File jsonFile = null;
    private Gson objGson = new GsonBuilder().setPrettyPrinting().create();
    private int failRetries = 10;

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
                            ImportFile imf = new ImportFile(evt.src, evt.dst, rc, evt);
                            imf.replicate();
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
            if (channel != null) {
                try {
                    channel.shutdown();

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
            }
            String host = target.split(":")[0];
            int port = Integer.parseInt(target.split(":")[1]);
            int maxMsgSize = 240 * 1024 * 1024;
            if (mtls || tls) {

                channel = NettyChannelBuilder.forAddress(host, port)
                        .negotiationType(NegotiationType.TLS).maxInboundMessageSize(maxMsgSize)
                        .sslContext(
                                getSslContextBuilder(certChainFilePath, privateKeyFilePath, trustCertCollectionFilePath)
                                        .build())
                        .build();
            } else {
                channel = NettyChannelBuilder.forAddress(host, port).maxInboundMessageSize(maxMsgSize)
                        .negotiationType(NegotiationType.PLAINTEXT)
                        .build();
            }

            SDFSLogger.getLog().info("Replication conneted to " + host + " " + port + " " + channel.toString()
                    + " connection state " + channel.getState(true));
            storageBlockingStub = StorageServiceGrpc.newBlockingStub(channel);
            evtBlockingStub = SDFSEventServiceGrpc.newBlockingStub(channel);
            ioBlockingStub = FileIOServiceGrpc.newBlockingStub(channel);
            File directory = new File(Main.volume.getReplPath() + File.separator);
            directory.mkdirs();
            jsonFile = new File(directory, "activereplications-" + this.volumeid + ".json");

        } catch (Exception e) {
            channel = null;
            storageBlockingStub = null;
            evtBlockingStub = null;
            ioBlockingStub = null;
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
                        ImportFile fl = new ImportFile(location.getSrcFilePath(), location.getDstFilePath(), this, evt);
                        Thread th = new Thread(fl);
                        th.start();
                    }
                }
                evts[i] = evt;
            }
            return evts;
        }
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

    public static class ListenRepl implements Runnable {
        ReplicationClient client;
        private transient RejectedExecutionHandler executionHandler = new BlockPolicy();

        public ListenRepl(ReplicationClient client) {
            this.client = client;
        }

        public void listen() throws IOException {
            SDFSLogger.getLog().info("listening for new file changes");
            Iterator<VolumeEvent> fi = client.storageBlockingStub
                    .subscribeToVolume(VolumeEventListenRequest.newBuilder()
                            .setPvolumeID(this.client.volumeid).setStartSequence(this.client.sequence).build());
            BlockingQueue<Runnable> aworksQueue = new ArrayBlockingQueue<Runnable>(2);
            ThreadPoolExecutor arExecutor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads + 1,
                    10, TimeUnit.SECONDS, aworksQueue, new ProcessPriorityThreadFactory(Thread.NORM_PRIORITY),
                    executionHandler);
            long seq = 0;
            while (fi.hasNext()) {

                VolumeEvent rs = fi.next();

                if (rs.getErrorCode() != errorCodes.NOERR) {
                    SDFSLogger.getLog().warn("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                    throw new IOException("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                }
                ImportFile impf = null;
                synchronized (client.activeImports) {
                    try {
                        if (!client.activeImports.contains(rs.getFile().getFilePath())) {
                            client.activeImports.add(rs.getFile().getFilePath());
                            if (rs.getActionType() == actionType.MFILEWRITTEN) {
                                ReplicationImportEvent evt = new ReplicationImportEvent(rs.getFile().getFilePath(),
                                        rs.getFile().getFilePath(),
                                        client.url, client.volumeid, client.mtls, false);
                                impf = new ImportFile(rs.getFile().getFilePath(), rs.getFile().getFilePath(), client,
                                        evt);

                            } else if (rs.getActionType() == actionType.MFILEDELETED) {
                                String pt = Main.volume.getPath() + File.separator + rs.getFile().getFilePath();
                                File _f = new File(pt);
                                FileIOServiceImpl.ImmuteLinuxFDFileFile(_f.getPath(), false);
                                MetaFileStore.getMF(_f).clearRetentionLock();
                                MetaFileStore.removeMetaFile(_f.getPath());
                            } else if (rs.getActionType() == actionType.MFILERENAMED) {
                                String spt = Main.volume.getPath() + File.separator + rs.getSrcfile();
                                File _sf = new File(spt);
                                String dpt = Main.volume.getPath() + File.separator + rs.getDstfile();
                                File _df = new File(dpt);

                                MetaFileStore.rename(_sf.getPath(), _df.getPath());
                            }
                            seq = rs.getSeq();
                            SDFSLogger.getLog().info("Sequence = " + seq);
                            if (client.sequence < seq) {
                                client.sequence = seq;
                                SDFSLogger.getLog().info("Client Sequence = " + client.sequence);
                            }

                        }
                        if (impf != null) {
                            arExecutor.execute(impf);
                        }
                    } finally {
                        if (impf == null) {
                            client.activeImports.remove(rs.getFile().getFilePath());
                        }
                    }

                }
            }
            arExecutor.shutdown();
            try {
                while (!arExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    SDFSLogger.getLog().debug("Awaiting replication to finish.");
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            synchronized (client.activeImports) {
                if (client.sequence < seq) {
                    client.sequence = seq;
                }
            }
        }

        @Override
        public void run() {
            for (;;) {
                try {
                    this.listen();
                    break;
                } catch (Exception e) {
                    SDFSLogger.getLog().warn("ListenRepl Replication failed", e);
                    try {
                        Thread.sleep(60 * 1000 * 5);
                    } catch (InterruptedException e1) {
                        break;
                    }
                    if (client.listThread == null || !client.listThread.isAlive()) {
                        client.listThread = new Thread(new ListReplLogs(this.client));
                        client.listThread.start();
                    }
                }

            }
        }

    }

    public static class ListReplLogs implements Runnable {
        ReplicationClient client;
        private transient RejectedExecutionHandler executionHandler = new BlockPolicy();

        public ListReplLogs(ReplicationClient client) {
            this.client = client;
        }

        public void list() throws IOException {
            SDFSLogger.getLog().info("listing replication logs");
            Iterator<VolumeEvent> fi = client.storageBlockingStub.listReplLogs(VolumeEventListenRequest.newBuilder()
                    .setPvolumeID(this.client.volumeid).setStartSequence(this.client.sequence).build());
            BlockingQueue<Runnable> aworksQueue = new ArrayBlockingQueue<Runnable>(2);
            ThreadPoolExecutor arExecutor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads + 1,
                    10, TimeUnit.SECONDS, aworksQueue, new ProcessPriorityThreadFactory(Thread.NORM_PRIORITY),
                    executionHandler);
            long seq = 0;
            while (fi.hasNext()) {

                VolumeEvent rs = fi.next();

                if (rs.getErrorCode() != errorCodes.NOERR) {
                    SDFSLogger.getLog().warn("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                    SDFSLogger.getLog().warn("Downloading all files");
                    ReplicationImportEvent evt = new ReplicationImportEvent(".",
                            ".", client.url, client.volumeid,
                            client.mtls, false);
                    try {
                        new DownloadAll(this.client, evt).replicationSinkAll();
                    } catch (Exception e) {
                        evt.endEvent("unable to download all", SDFSEvent.ERROR, e);
                        throw new IOException(e);
                    }
                    return;
                }
                ImportFile impf = null;
                synchronized (client.activeImports) {
                    try {
                        if (!client.activeImports.contains(rs.getFile().getFilePath())) {
                            client.activeImports.add(rs.getFile().getFilePath());
                            if (rs.getActionType() == actionType.MFILEWRITTEN) {
                                ReplicationImportEvent evt = new ReplicationImportEvent(rs.getFile().getFilePath(),
                                        rs.getFile().getFilePath(),
                                        client.url, client.volumeid, client.mtls, false);
                                impf = new ImportFile(rs.getFile().getFilePath(), rs.getFile().getFilePath(), client,
                                        evt);

                            } else if (rs.getActionType() == actionType.MFILEDELETED) {
                                String pt = Main.volume.getPath() + File.separator + rs.getFile().getFilePath();
                                File _f = new File(pt);
                                FileIOServiceImpl.ImmuteLinuxFDFileFile(_f.getPath(), false);
                                MetaFileStore.getMF(_f).clearRetentionLock();
                                MetaFileStore.removeMetaFile(_f.getPath());
                            } else if (rs.getActionType() == actionType.MFILERENAMED) {
                                String spt = Main.volume.getPath() + File.separator + rs.getSrcfile();
                                File _sf = new File(spt);
                                String dpt = Main.volume.getPath() + File.separator + rs.getDstfile();
                                File _df = new File(dpt);

                                MetaFileStore.rename(_sf.getPath(), _df.getPath());
                            }

                        }
                        if (impf != null) {
                            arExecutor.execute(impf);
                        }
                    } finally {
                        if (impf == null) {
                            client.activeImports.remove(rs.getFile().getFilePath());
                        }
                    }
                    seq = rs.getSeq();

                }
            }
            arExecutor.shutdown();
            try {
                while (!arExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    SDFSLogger.getLog().debug("Awaiting replication to finish.");
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            synchronized (client.activeImports) {
                if (client.sequence < seq) {
                    client.sequence = seq;
                    SDFSLogger.getLog().info("Client Sequence = " + client.sequence);
                }
            }
        }

        @Override
        public void run() {
            int retries = 0;
            for (;;) {
                try {
                    this.list();
                    break;
                } catch (Exception e) {
                    retries++;
                    SDFSLogger.getLog().warn("Downloadall Replication failed", e);
                    if (retries > 10) {
                        SDFSLogger.getLog().warn("Download all replication failed. Giving up");
                        break;
                    } else {
                        try {
                            Thread.sleep(60 * 1000 * 5);
                        } catch (InterruptedException e1) {
                            break;
                        }
                    }

                }
            }

        }

    }

    public void shutDown() {
        synchronized (this.activeImports) {
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

    public static class DownloadAll implements Runnable {
        ReplicationClient client;
        private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
        SDFSEvent evt;

        public DownloadAll(ReplicationClient client, SDFSEvent evt) {
            this.client = client;
            this.client.sequence = 0;
            this.evt = evt;
        }

        public void replicationSinkAll() throws IOException {
            SDFSLogger.getLog().info("downloading all files");
            Iterator<FileMessageResponse> fi = client.ioBlockingStub.getaAllFileInfo(
                    FileInfoRequest.newBuilder().setPvolumeID(client.volumeid).setFileName(".").build());
            BlockingQueue<Runnable> aworksQueue = new ArrayBlockingQueue<Runnable>(2);
            ThreadPoolExecutor arExecutor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads + 1,
                    10, TimeUnit.SECONDS, aworksQueue, new ProcessPriorityThreadFactory(Thread.NORM_PRIORITY),
                    executionHandler);
            while (fi.hasNext()) {

                FileMessageResponse rs = fi.next();

                if (rs.getErrorCode() != errorCodes.NOERR) {
                    SDFSLogger.getLog().warn("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                    throw new IOException("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                }
                FileInfoResponse file = rs.getResponseList().get(0);
                ImportFile impf = null;
                synchronized (client.activeImports) {
                    if (!client.activeImports.contains(file.getFilePath())) {
                        client.activeImports.add(file.getFilePath());
                        String pt = Main.volume.getPath() + File.separator + file.getFilePath();
                        File _f = new File(pt);
                        if (_f.exists()) {
                            if (MetaFileStore.getMF(_f).lastModified() != file.getMtime()) {
                                ReplicationImportEvent evt = new ReplicationImportEvent(file.getFilePath(),
                                        file.getFilePath(),
                                        client.url, client.volumeid, client.mtls, false);
                                impf = new ImportFile(file.getFilePath(), file.getFilePath(), client, evt);
                            } else {
                                ReplicationImportEvent evt = new ReplicationImportEvent(file.getFilePath(),
                                        file.getFilePath(),
                                        client.url, client.volumeid, client.mtls, false);
                                evt.endEvent("File Already Exists and looks like the same " + file.getFilePath());
                            }
                        } else {
                            ReplicationImportEvent evt = new ReplicationImportEvent(file.getFilePath(),
                                    file.getFilePath(),
                                    client.url, client.volumeid, client.mtls, false);
                            impf = new ImportFile(file.getFilePath(), file.getFilePath(), client, evt);
                        }
                    }
                }
                if (impf != null) {
                    arExecutor.execute(impf);
                }

            }
            arExecutor.shutdown();
            try {
                while (!arExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    SDFSLogger.getLog().debug("Awaiting replication to finish.");
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            evt.endEvent("Download All Replication ended Successfully");
        }

        @Override
        public void run() {
            int retries = 0;
            for (;;) {
                try {
                    replicationSinkAll();
                    break;
                } catch (Exception e) {
                    retries++;
                    SDFSLogger.getLog().warn("Downloadall Replication failed", e);
                    if (retries > 10) {
                        SDFSLogger.getLog().warn("Download all replication failed. Giving up");
                        evt.endEvent("Download all replication failed. Giving up");
                        break;
                    } else {
                        try {
                            Thread.sleep(60 * 1000 * 5);
                        } catch (InterruptedException e1) {
                            break;
                        }
                    }

                }
            }
            client.downloadThread = null;
        }
    }

    public static class ImportFile implements Runnable {
        ReplicationImportEvent evt;
        String srcFile;
        String dstFile;
        ReplicationClient client;
        boolean canceled = false;
        boolean paused = false;
        private Gson objGson = new GsonBuilder().setPrettyPrinting().create();

        public ImportFile(String srcFile, String dstFile, ReplicationClient client, ReplicationImportEvent evt) {
            this.client = client;
            this.evt = evt;
            this.srcFile = srcFile;
            this.dstFile = dstFile;
            SDFSLogger.getLog().info("Importing " + this.srcFile);

            client.imports.put(evt.uid, this);
            String mapToJson = objGson.toJson(client.imports.keySet());
            try {
                FileUtils.writeStringToFile(client.jsonFile, mapToJson, Charset.forName("UTF-8"));
            } catch (IOException e) {
                SDFSLogger.getLog().warn("unable to persist active imports", e);
            }
        }

        @Override
        public void run() {
            Exception e = null;
            for (int i = 0; i < client.failRetries; i++) {
                try {
                    replicate();
                    SDFSLogger.getLog().info("Replication Completed Successfully for " + srcFile);
                    this.evt.endEvent("Replication Successful");
                    e = null;
                    break;
                } catch (ReplicationCanceledException e1) {
                    SDFSLogger.getLog().info("Replication Canceled");
                    this.evt.endEvent("Replication Canceled", SDFSEvent.WARN);
                    break;
                } catch (Exception e1) {
                    SDFSLogger.getLog().warn("unable to complete replication to " + client.url + " volume id "
                            + client.volumeid + " for " + srcFile + " will retry in 5 minutes", e1);
                    e = e1;
                    this.evt.shortMsg = "unable to complete replication to " + client.url + " volume id "
                            + client.volumeid + " for " + srcFile + " will retry in 5 minutes";
                    try {
                        Thread.sleep(5 * 60 * 1000);
                    } catch (InterruptedException e2) {
                        break;
                    }
                }
            }
            if (e != null) {
                SDFSLogger.getLog().warn("replication failed for " + client.url + " volume id "
                        + client.volumeid + " for " + srcFile, e);
                synchronized (client.activeImports) {
                    client.activeImports.remove(dstFile);
                    if (client.imports.remove(this.evt.uid) != null) {
                        String mapToJson = objGson.toJson(client.imports.keySet());
                        try {
                            FileUtils.writeStringToFile(client.jsonFile, mapToJson, Charset.forName("UTF-8"));
                        } catch (IOException e1) {
                            SDFSLogger.getLog().warn("unable to persist active imports", e);
                        }
                    }
                }
            }
        }

        private void replicate() throws ReplicationCanceledException, Exception {

            String pt = Main.volume.getPath() + File.separator + this.dstFile;
            File _f = new File(pt);
            if (_f.exists()) {
                FileIOServiceImpl.ImmuteLinuxFDFileFile(_f.getPath(), false);
                MetaFileStore.getMF(_f).clearRetentionLock();
                MetaFileStore.removeMetaFile(_f.getPath());
            }
            if (this.evt.canceled) {
                throw new ReplicationCanceledException("Replication Canceled");
            }
            if (this.evt.paused) {
                while (this.evt.paused) {
                    Thread.sleep(1000);
                }
            }
            evt.shortMsg = "Importing metadata file for " + this.dstFile;
            MetaDataDedupFile mf = downloadMetaFile();
            evt.addCount(1);
            if (this.evt.canceled) {
                mf.deleteStub(false);
                throw new ReplicationCanceledException("Replication Canceled");
            }
            if (this.evt.paused) {
                while (this.evt.paused) {
                    Thread.sleep(1000);
                }
            }
            evt.shortMsg = "Importing map for " + this.dstFile;
            String sguid = mf.getDfGuid();
            String ng = UUID.randomUUID().toString();
            mf.setLength(0, true);
            mf.setDfGuid(ng);
            mf.sync();
            try {
                if (sguid != null && sguid.trim().length() > 0) {
                    downloadDDB(mf, sguid);
                }
                mf.setRetentionLock();
                CloseFile.close(mf, true);
            } catch (ReplicationCanceledException e) {
                FileIOServiceImpl.ImmuteLinuxFDFileFile(mf.getPath(), false);
                MetaFileStore.getMF(mf.getAbsolutePath()).clearRetentionLock();
                MetaFileStore.removeMetaFile(mf.getPath(), false, false, true);
                SDFSLogger.getLog().warn(e);
                throw e;
            }

            MetaFileStore.removedCachedMF(mf.getPath());
            MetaFileStore.addToCache(mf);
            evt.addCount(1);
            FileIOServiceImpl.ImmuteLinuxFDFileFile(mf.getPath(), true);
            evt.endEvent("Import Successful for " + this.dstFile, SDFSEvent.INFO);
            SDFSLogger.getLog().info("Imported " + this.dstFile);

            synchronized (client.activeImports) {
                client.activeImports.remove(dstFile);
                if (client.imports.remove(this.evt.uid) != null) {
                    String mapToJson = objGson.toJson(client.imports.keySet());
                    try {
                        FileUtils.writeStringToFile(client.jsonFile, mapToJson, Charset.forName("UTF-8"));
                    } catch (IOException e) {
                        SDFSLogger.getLog().warn("unable to persist active imports", e);
                    }
                }
            }

        }

        private MetaDataDedupFile downloadMetaFile() throws Exception {
            if (this.evt.canceled) {
                throw new ReplicationCanceledException("Replication Canceled");
            }
            if (this.evt.paused) {
                while (this.evt.paused) {
                    Thread.sleep(1000);
                }
            }
            MetaDataDedupeFileRequest mr = MetaDataDedupeFileRequest.newBuilder().setPvolumeID(client.volumeid)
                    .setFilePath(srcFile).build();
            MetaDataDedupeFileResponse crs = this.client.storageBlockingStub.getMetaDataDedupeFile(mr);
            if (crs.getErrorCode() != errorCodes.NOERR) {
                throw new IOException(
                        "Error found during file request " + crs.getErrorCode() + " err : " + crs.getError());
            }
            String pt = Main.volume.getPath() + File.separator + this.dstFile;
            File _f = new File(pt);
            return MetaDataDedupFile.fromProtoBuf(crs.getFile(), _f.getPath());
        }

        private void downloadDDB(MetaDataDedupFile mf, String guid) throws Exception {
            if (this.evt.canceled) {
                throw new ReplicationCanceledException("Replication Canceled");
            }
            if (this.evt.paused) {
                while (this.evt.paused) {
                    Thread.sleep(1000);
                }
            }
            SparseDedupeFileRequest req = SparseDedupeFileRequest.newBuilder()
                    .setPvolumeID(this.client.volumeid).setGuid(guid).build();
            Iterator<SparseDataChunkP> crs = this.client.storageBlockingStub.getSparseDedupeFile(req);

            LongByteArrayMap mp = LongByteArrayMap.getMap(mf.getDfGuid());
            mf.getIOMonitor().clearFileCounters(false);
            long pos = 0;
            while (crs.hasNext()) {
                if (this.evt.canceled) {
                    throw new ReplicationCanceledException("Replication Canceled");
                }
                if (this.evt.paused) {
                    while (this.evt.paused) {
                        Thread.sleep(1000);
                    }
                }
                SparseDataChunkP cr = crs.next();
                if (cr.getErrorCode() != errorCodes.NOERR) {
                    String msg = "Unable to import map file [" + guid + "] " +
                            " because :" + cr.getError() + " code:" + cr.getErrorCodeValue();

                    throw new IOException(msg);

                }
                SparseDataChunk ck = importSparseDataChunk(new SparseDataChunk(cr), mf);
                mp.put(pos, ck);
                pos += ck.len;
                mf.setLength(mf.length() + ck.len, false);
            }
            SDFSLogger.getLog().info("MF Size = " + mf.length());
            mp.close();
        }

        private SparseDataChunk importSparseDataChunk(SparseDataChunk ck, MetaDataDedupFile mf)
                throws IOException, HashtableFullException {

            TreeMap<Integer, HashLocPair> al = ck.getFingers();
            HashMap<ByteString, ChunkEntry> ces = new HashMap<ByteString, ChunkEntry>();
            HashMap<ByteString, CtLoc> claims = new HashMap<ByteString, CtLoc>();
            Builder b = GetChunksRequest.newBuilder();
            b.setPvolumeID(client.volumeid);
            for (HashLocPair p : al.values()) {
                long pos = HCServiceProxy.hashExists(p.hash);
                ByteString bs = ByteString.copyFrom(p.hash);
                if (!claims.containsKey(bs)) {
                    CtLoc ctl = new CtLoc();
                    ctl.ct = 0;
                    ctl.loc = pos;
                    claims.put(bs, ctl);
                }
                CtLoc ctl = claims.get(bs);
                ctl.ct++;
                if (pos == -1) {
                    claims.get(bs).newdata = true;
                    ChunkEntry ce = ChunkEntry.newBuilder().setHash(bs).build();
                    ces.put(bs, ce);
                    b.addChunks(ce);

                } else {
                    mf.getIOMonitor().addDulicateData(p.nlen, false);
                }

            }
            GetChunksRequest sreq = b.build();
            Iterator<ChunkEntry> rces = this.client.storageBlockingStub.getChunks(sreq);

            while (rces.hasNext()) {
                ChunkEntry ce = rces.next();
                if (ce.getErrorCode() == errorCodes.EARCHIVEIO) {
                    this.restoreArchives(mf);
                } else if (ce.getErrorCode() != errorCodes.NOERR) {
                    String msg = "error code returned :" + ce.getErrorCode() +
                            " message : " + ce.getError();
                    SDFSLogger.getLog().warn(msg);
                    throw new IOException(msg);
                }
                byte[] dt = ce.getData().toByteArray();
                // SDFSLogger.getLog().info("dt len " + dt.length + " dt = " + new String(dt));
                HashFunction hf = Hashing.sha256();
                if (!StringUtils.getHexString(hf.hashBytes(dt).asBytes())
                        .equals(StringUtils.getHexString(ce.getHash().toByteArray()))) {
                    SDFSLogger.getLog().info(StringUtils.getHexString(hf.hashBytes(dt).asBytes())
                            +
                            " = " + StringUtils.getHexString(ce.getHash().toByteArray()));
                }
                ChunkData cd = new ChunkData(ce.getHash().toByteArray(), dt.length, dt, mf.getDfGuid());
                cd.references = claims.get(ce.getHash()).ct;
                InsertRecord ir = HCServiceProxy.getHashesMap().put(cd, true);
                claims.get(ce.getHash()).loc = Longs.fromByteArray(ir.getHashLocs());
                if (claims.get(ce.getHash()).loc == -1) {
                    throw new IOException(
                            "Hash not found for " + StringUtils.getHexString(ce.getHash().toByteArray()));
                }
                mf.getIOMonitor().addActualBytesWritten(ir.getCompressedLength(), false);
            }
            for (HashLocPair p : al.values()) {
                ByteString bs = ByteString.copyFrom(p.hash);
                CtLoc ctl = claims.get(bs);
                if (!ctl.newdata) {
                    ChunkData cm = new ChunkData(Longs.fromByteArray(p.hashloc), p.hash);
                    cm.references = ctl.ct;
                    InsertRecord ir = HCServiceProxy.getHashesMap().put(cm, false);
                    if (ir.getInserted()) {
                        HCServiceProxy.getHashesMap().claimKey(p.hash, Longs.fromByteArray(p.hashloc), ctl.ct);
                        throw new IOException(
                                "Hash Invalid state found for " + StringUtils.getHexString(p.hash) + " "
                                        + Longs.fromByteArray(p.hashloc));
                    }
                    ctl.loc = Longs.fromByteArray(ir.getHashLocs());
                }
                p.hashloc = Longs.toByteArray(ctl.loc);
                mf.getIOMonitor().addVirtualBytesWritten(p.nlen, false);
            }
            return ck;
        }

        private void restoreArchives(MetaDataDedupFile mf) throws IOException {
            String msg = "During Replication Archive Data Found in " + mf.getPath() +
                    "requesting recovery of data from archive.";
            SDFSLogger.getLog().info(msg);
            evt.shortMsg = msg;
            String basePath = Main.volume.getPath() + File.separator;
            String subPath = mf.getPath().substring(basePath.length());
            RestoreArchivesResponse rresp = this.client.storageBlockingStub.restoreArchives(
                    RestoreArchivesRequest.newBuilder().setPvolumeID(this.client.volumeid)
                            .setFilePath(subPath)
                            .build());

            if (rresp.getErrorCode() != errorCodes.NOERR) {
                throw new IOException("Error during restore of archive : " + rresp.getErrorCode() +
                        " " + rresp.getError());
            }
            Iterator<SDFSEventResponse> evtResps = this.client.evtBlockingStub.subscribeEvent(
                    SDFSEventRequest.newBuilder().setPvolumeID(this.client.volumeid)
                            .setUuid(rresp.getEventID())
                            .build());
            while (evtResps.hasNext()) {
                SDFSEventResponse evtResp = evtResps.next();
                if (evtResp.getErrorCode() != errorCodes.NOERR) {
                    throw new IOException(
                            "Error during restore of archive : " + evtResp.getErrorCode() +
                                    " " + evtResp.getError());
                } else if (evtResp.getEvent().getLevel().equalsIgnoreCase("error")) {
                    throw new IOException(
                            "Error during restore of archive : " + evtResp.getEvent().getShortMsg());
                } else if (evtResp.getEvent().getEndTime() > 0) {
                    break;
                }

            }
            SDFSEventResponse evtResp = this.client.evtBlockingStub
                    .getEvent(SDFSEventRequest.newBuilder().setPvolumeID(this.client.volumeid)
                            .setUuid(rresp.getEventID())
                            .build());
            msg = "Recovered " + mf.getPath() +
                    " event id = " + evtResp.getEvent().getUuid() +
                    " event message = " + evtResp.getEvent().getShortMsg() +
                    " event endtime = " + evtResp.getEvent().getEndTime() +
                    " event level = " + evtResp.getEvent().getLevel();
            SDFSLogger.getLog().info(msg);
            evt.shortMsg = msg;

        }

    }

    private static class CtLoc {
        int ct;
        long loc;
        boolean newdata;
    }

}
