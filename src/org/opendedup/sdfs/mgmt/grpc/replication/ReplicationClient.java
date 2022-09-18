package org.opendedup.sdfs.mgmt.grpc.replication;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.X509TrustManager;

import org.apache.commons.io.FileExistsException;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.grpc.SDFSEventServiceGrpc;
import org.opendedup.grpc.StorageServiceGrpc;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse;
import org.opendedup.grpc.SDFSEventServiceGrpc.SDFSEventServiceBlockingStub;
import org.opendedup.grpc.Storage.ChunkEntry;
import org.opendedup.grpc.Storage.GetChunksRequest;
import org.opendedup.grpc.Storage.MetaDataDedupeFileRequest;
import org.opendedup.grpc.Storage.MetaDataDedupeFileResponse;
import org.opendedup.grpc.Storage.RestoreArchivesRequest;
import org.opendedup.grpc.Storage.RestoreArchivesResponse;
import org.opendedup.grpc.Storage.SparseDataChunkP;
import org.opendedup.grpc.Storage.SparseDedupeFileRequest;
import org.opendedup.grpc.Storage.GetChunksRequest.Builder;
import org.opendedup.grpc.StorageServiceGrpc.StorageServiceBlockingStub;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.mgmt.CloseFile;
import org.opendedup.sdfs.mgmt.grpc.FileIOServiceImpl;
import org.opendedup.sdfs.mgmt.grpc.tls.DynamicTrustManager;
import org.opendedup.sdfs.mgmt.grpc.tls.WatchForFile;
import org.opendedup.sdfs.notification.ReplicationImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.EasyX509ClientTrustManager;
import org.opendedup.util.StringUtils;
import org.opendedup.sdfs.io.HashLocPair;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.InsertRecord;

import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.servers.HCServiceProxy;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;

public class ReplicationClient {
    ManagedChannel channel = null;
    private StorageServiceBlockingStub storageBlockingStub;
    private SDFSEventServiceBlockingStub evtBlockingStub;
    private String url;
    private long volumeid;
    public static Map<String, ImportFile> imports = new ConcurrentHashMap<String, ImportFile>();
    public static Set<String> activeImports = new HashSet<String>();

    public ReplicationClient(String url, long volumeid, boolean mtls) throws Exception {
        this.url = url;
        this.volumeid = volumeid;
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
                    .sslContext(getSslContextBuilder(certChainFilePath, privateKeyFilePath, trustCertCollectionFilePath)
                            .build())
                    .build();
        } else {
            channel = NettyChannelBuilder.forAddress(host, port).maxInboundMessageSize(maxMsgSize)
                    .negotiationType(NegotiationType.PLAINTEXT)
                    .build();
        }

        SDFSLogger.getLog().info("Replication conneted to " + host + " " + port + " " + channel.toString());
        storageBlockingStub = StorageServiceGrpc.newBlockingStub(channel);
        evtBlockingStub = SDFSEventServiceGrpc.newBlockingStub(channel);
    }

    private SslContextBuilder getSslContextBuilder(String certChainFilePath, String privateKeyFilePath,
            String trustCertCollectionFilePath) throws Exception {
        SslContextBuilder sslClientContextBuilder = null;
        if (!Main.authJarFilePath.equals("") && !Main.authClassInfo.equals("")) {

            EasyX509ClientTrustManager tm = new EasyX509ClientTrustManager();
            sslClientContextBuilder = SslContextBuilder.forClient()
                    .clientAuth(ClientAuth.REQUIRE).trustManager(tm);
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

    public SDFSEvent replicate(String srcFile, String dstFile) throws FileExistsException {
        synchronized (activeImports) {
            if (activeImports.contains(dstFile)) {
                throw new FileExistsException("Replication already occuring for + dstFile");
            }
            activeImports.add(dstFile);
            SDFSLogger.getLog().info("Will Replicate " + srcFile + " to " + dstFile + " from " + url);
            ReplicationImportEvent evt = new ReplicationImportEvent(srcFile, dstFile, this.url, this.volumeid);
            ImportFile fl = new ImportFile(srcFile, dstFile, this, evt);

            Thread th = new Thread(fl);
            th.start();
            return evt;
        }
    }

    public static class ImportFile implements Runnable {
        ReplicationImportEvent evt;
        String srcFile;
        String dstFile;
        ReplicationClient client;
        boolean canceled = false;
        boolean paused = false;

        public ImportFile(String srcFile, String dstFile, ReplicationClient client, ReplicationImportEvent evt) {
            this.client = client;
            this.evt = evt;
            this.srcFile = srcFile;
            this.dstFile = dstFile;
            ReplicationClient.imports.put(evt.uid, this);
        }

        @Override
        public void run() {
            try {
                if (new File(this.dstFile).exists()) {
                    evt.endEvent("Unable to import archive [" + srcFile + "] " + "Destination [" + dstFile + "] " +
                            " because destination exists",
                            SDFSEvent.ERROR);
                }
                try {
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
                        downloadDDB(mf,sguid);
                        FileIOServiceImpl.ImmuteLinuxFDFileFile(mf.getPath(), true);
                        CloseFile.close(mf, true);
                    } catch (ReplicationCanceledException e) {
                        FileIOServiceImpl.ImmuteLinuxFDFileFile(mf.getPath(), false);
                        MetaFileStore.getMF(mf.getAbsolutePath()).clearRetentionLock();
                        MetaFileStore.removeMetaFile(mf.getPath(), false, false, true);
                        SDFSLogger.getLog().warn(e);
                        return;
                    }
                    MetaFileStore.removedCachedMF(mf.getPath());
                    MetaFileStore.addToCache(mf);
                    evt.addCount(1);
                    evt.endEvent("Import Successful for " + this.dstFile, SDFSEvent.INFO);
                } catch (ReplicationCanceledException e) {
                    SDFSLogger.getLog().warn(e);
                    return;
                } catch (Exception e) {
                    SDFSLogger.getLog().warn("unable to replicate", e);
                    evt.endEvent(e.getMessage(), SDFSEvent.ERROR);
                    return;
                }
            } finally {
                ReplicationClient.activeImports.remove(dstFile);
                ReplicationClient.imports.remove(this.evt.uid);
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
            MetaDataDedupFile.fromProtoBuf(crs.getFile(), _f.getPath());
            return MetaFileStore.getMF(_f);
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
                SparseDataChunk ck = importSparseDataChunk( new SparseDataChunk(cr), mf);
                mp.put(ck.getFpos(), ck);
                mf.setLength(ck.getFpos() + ck.len, false);
            }
            mp.forceClose();
        }

        private SparseDataChunk importSparseDataChunk(SparseDataChunk ck, MetaDataDedupFile mf) throws IOException, HashtableFullException {

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
                //HashFunction hf = Hashing.sha256();
                //SDFSLogger.getLog().info(StringUtils.getHexString(hf.hashBytes(dt).asBytes()) + 
                //" = " + StringUtils.getHexString(ce.getHash().toByteArray()));
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
                        throw new IOException(
                                "Hash Invalid state found for " + StringUtils.getHexString(p.hash));
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
