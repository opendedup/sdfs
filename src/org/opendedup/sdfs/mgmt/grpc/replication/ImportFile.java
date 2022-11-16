package org.opendedup.sdfs.mgmt.grpc.replication;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventRequest;
import org.opendedup.grpc.SDFSEventOuterClass.SDFSEventResponse;
import org.opendedup.grpc.Storage.ChunkEntry;
import org.opendedup.grpc.Storage.GetChunksRequest;
import org.opendedup.grpc.Storage.GetChunksRequest.Builder;
import org.opendedup.grpc.Storage.MetaDataDedupeFileRequest;
import org.opendedup.grpc.Storage.MetaDataDedupeFileResponse;
import org.opendedup.grpc.Storage.RestoreArchivesRequest;
import org.opendedup.grpc.Storage.RestoreArchivesResponse;
import org.opendedup.grpc.Storage.SparseDataChunkP;
import org.opendedup.grpc.Storage.SparseDedupeFileRequest;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.mgmt.CloseFile;
import org.opendedup.sdfs.mgmt.grpc.FileIOServiceImpl;
import org.opendedup.sdfs.mgmt.grpc.replication.ReplicationClient.ReplicationConnection;
import org.opendedup.sdfs.notification.ReplicationImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.StringUtils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;

public class ImportFile implements Runnable {
    ReplicationImportEvent evt;
    String srcFile;
    String dstFile;
    ReplicationClient client;
    boolean canceled = false;
    boolean paused = false;
    boolean overwrite;
    ReplicationConnection rc = null;

    private Gson objGson = new GsonBuilder().setPrettyPrinting().create();
    private static HashMap<String, ReentrantLock> actives = new HashMap<String, ReentrantLock>();

    public ImportFile(String srcFile, String dstFile, ReplicationClient client, ReplicationImportEvent evt,
            boolean overwrite) {
        this.client = client;
        this.evt = evt;
        this.srcFile = srcFile;
        this.dstFile = dstFile;
        this.overwrite = overwrite;
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
        ReentrantLock active = null;
        synchronized (actives) {
            if (actives.containsKey(this.dstFile)) {
                active = actives.get(this.dstFile);
            } else {
                active = new ReentrantLock();
                actives.put(this.dstFile, active);
            }
        }

        Exception e = null;
        for (int i = 0; i < client.failRetries; i++) {
            active.lock();
            try {
                replicate();
                SDFSLogger.getLog().info("Replication Completed Successfully for " + srcFile);
                this.evt.endEvent("Replication Successful");
                e = null;
                active.unlock();

                break;
            } catch (ReplicationCanceledException e1) {
                SDFSLogger.getLog().info("Replication Canceled");
                try {
                    synchronized (client.activeImports) {
                        client.activeImports.remove(dstFile);
                        if (client.imports.remove(this.evt.uid) != null) {
                            String mapToJson = objGson.toJson(client.imports.keySet());
                            try {
                                FileUtils.writeStringToFile(client.jsonFile, mapToJson, Charset.forName("UTF-8"));
                            } catch (IOException e2) {
                                SDFSLogger.getLog().warn("unable to persist active imports", e);
                            }
                        }
                    }

                    this.evt.endEvent("Replication Canceled", SDFSEvent.WARN);
                } finally {
                    active.unlock();
                }
                break;
            } catch (FileAlreadyExistsException e1) {
                try {
                    SDFSLogger.getLog().warn("File Already exists", e);
                    this.evt.endEvent("File Already exists", SDFSEvent.WARN);
                } finally {
                    active.unlock();
                }
                break;
            } catch (Exception e1) {
                try {
                    SDFSLogger.getLog().warn("unable to complete replication to " + client.url + " volume id "
                            + client.volumeid + " for " + srcFile + " will retry in 5 minutes", e1);
                    e = e1;
                    this.evt.setShortMsg("unable to complete replication to " + client.url + " volume id "
                            + client.volumeid + " for " + srcFile + " will retry in 5 minutes");
                    String pt = Main.volume.getPath() + File.separator + this.dstFile;
                    File _f = new File(pt);
                    try {

                        if (_f.exists()) {
                            FileIOServiceImpl.ImmuteLinuxFDFileFile(_f.getPath(), false);
                            MetaFileStore.getMF(_f.getAbsolutePath()).clearRetentionLock();
                            MetaFileStore.removeMetaFile(_f.getPath(), false, false, true);
                        }
                    } catch (Exception e2) {

                    }
                    _f.delete();

                } finally {
                    active.unlock();
                }
                try {
                    for (int z = 0; z < 5 * 60; z++) {
                        Thread.sleep(1000);
                        if (this.canceled) {
                            synchronized (client.activeImports) {
                                client.activeImports.remove(dstFile);
                                if (client.imports.remove(this.evt.uid) != null) {
                                    String mapToJson = objGson.toJson(client.imports.keySet());
                                    try {
                                        FileUtils.writeStringToFile(client.jsonFile, mapToJson,
                                                Charset.forName("UTF-8"));
                                    } catch (IOException e2) {
                                        SDFSLogger.getLog().warn("unable to persist active imports", e);
                                    }
                                }
                            }
                            this.evt.endEvent("Replication Canceled", SDFSEvent.WARN);
                            break;
                        }
                    }
                } catch (InterruptedException e2) {
                    break;
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
        synchronized (actives) {
            if (!active.isLocked()) {
                actives.remove(this.dstFile);
            } 
        }

    }

    void replicate() throws ReplicationCanceledException, Exception {
        rc = client.getReplicationConnection();
        try {

            String pt = Main.volume.getPath() + File.separator + this.dstFile;
            File _f = new File(pt);
            if (_f.exists() && !this.overwrite) {
                throw new FileAlreadyExistsException(pt);
            }
            if (this.evt.canceled || this.client.removed || this.client.closed) {
                throw new ReplicationCanceledException("Replication Canceled");
            }
            if (this.evt.paused) {
                SDFSLogger.getLog().info("Event " + this.evt.uid + " is paused");
                while (this.evt.paused) {
                    Thread.sleep(1000);
                }
                SDFSLogger.getLog().info("Event " + this.evt.uid + " is unpaused");
            }
            evt.setShortMsg("Importing metadata file for " + this.dstFile);
            MetaDataDedupFile mf = null;
            try {
                mf = downloadMetaFile();
            } catch (ReplicationCanceledException e) {

                throw e;
            }
            evt.addCount(1);
            if (this.evt.canceled) {
                mf.deleteStub(false);
                throw new ReplicationCanceledException("Replication Canceled");
            }
            if (this.evt.paused) {
                SDFSLogger.getLog().info("Event " + this.evt.uid + " is paused");
                while (this.evt.paused) {
                    Thread.sleep(1000);
                }
                SDFSLogger.getLog().info("Event " + this.evt.uid + " is unpaused");
            }
            evt.setShortMsg("Importing map for " + this.dstFile);
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
                if (_f.exists()) {
                    FileIOServiceImpl.ImmuteLinuxFDFileFile(mf.getPath(), false);
                    MetaFileStore.getMF(mf.getAbsolutePath()).clearRetentionLock();
                    MetaFileStore.removeMetaFile(mf.getPath(), false, false, true);
                }
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
        } finally {
            try {
                client.closeReplicationConnection(rc);
            } catch (Exception e) {

            }
        }

    }

    private MetaDataDedupFile downloadMetaFile() throws Exception {
        if (this.evt.canceled || this.client.removed || this.client.closed) {
            throw new ReplicationCanceledException("Replication Canceled");
        }
        if (this.evt.paused) {
            SDFSLogger.getLog().info("Event " + this.evt.uid + " is paused");
            while (this.evt.paused) {
                Thread.sleep(1000);
            }
            SDFSLogger.getLog().info("Event " + this.evt.uid + " is unpaused");
        }
        MetaDataDedupeFileRequest mr = MetaDataDedupeFileRequest.newBuilder().setPvolumeID(client.volumeid)
                .setFilePath(srcFile).build();
        MetaDataDedupeFileResponse crs = this.rc.getStorageBlockingStub().getMetaDataDedupeFile(mr);
        if (crs.getErrorCode() != errorCodes.NOERR) {
            throw new IOException(
                    "Error found during file request " + crs.getErrorCode() + " err : " + crs.getError());
        }
        String pt = Main.volume.getPath() + File.separator + this.dstFile;
        File _f = new File(pt);
        this.evt.fileSize = crs.getFile().getSize();
        return MetaDataDedupFile.fromProtoBuf(crs.getFile(), _f.getPath());
    }

    private void downloadDDB(MetaDataDedupFile mf, String guid) throws Exception {
        if (this.evt.canceled || this.client.removed || this.client.closed) {
            throw new ReplicationCanceledException("Replication Canceled");
        }
        if (this.evt.paused) {
            SDFSLogger.getLog().info("Event " + this.evt.uid + " is paused");
            while (this.evt.paused) {
                Thread.sleep(1000);
            }
            SDFSLogger.getLog().info("Event " + this.evt.uid + " is upaused");
        }
        SparseDedupeFileRequest req = SparseDedupeFileRequest.newBuilder()
                .setPvolumeID(this.client.volumeid).setGuid(guid).build();
        Iterator<SparseDataChunkP> crs = this.rc.getStorageBlockingStub().getSparseDedupeFile(req);

        LongByteArrayMap mp = LongByteArrayMap.getMap(mf.getDfGuid());
        mf.getIOMonitor().clearFileCounters(false);
        long pos = 0;
        while (crs.hasNext()) {
            if (this.evt.canceled || this.client.removed || this.client.closed) {
                throw new ReplicationCanceledException("Replication Canceled");
            }
            if (this.evt.paused) {
                SDFSLogger.getLog().info("Event " + this.evt.uid + " is paused");
                while (this.evt.paused) {
                    Thread.sleep(1000);
                }
                SDFSLogger.getLog().info("Event " + this.evt.uid + " is unpaused");
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
        Iterator<ChunkEntry> rces = this.rc.getStorageBlockingStub().getChunks(sreq);

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
            evt.bytesImported += ir.getCompressedLength();
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
            evt.bytesProcessed += p.nlen;
            mf.getIOMonitor().addVirtualBytesWritten(p.nlen, false);
        }
        return ck;
    }

    private void restoreArchives(MetaDataDedupFile mf) throws IOException {
        String msg = "During Replication Archive Data Found in " + mf.getPath() +
                "requesting recovery of data from archive.";
        SDFSLogger.getLog().info(msg);
        evt.setShortMsg(msg);
        String basePath = Main.volume.getPath() + File.separator;
        String subPath = mf.getPath().substring(basePath.length());
        RestoreArchivesResponse rresp = this.rc.getStorageBlockingStub().restoreArchives(
                RestoreArchivesRequest.newBuilder().setPvolumeID(this.client.volumeid)
                        .setFilePath(subPath)
                        .build());

        if (rresp.getErrorCode() != errorCodes.NOERR) {
            throw new IOException("Error during restore of archive : " + rresp.getErrorCode() +
                    " " + rresp.getError());
        }
        Iterator<SDFSEventResponse> evtResps = this.rc.getEvtBlockingStub().subscribeEvent(
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
        SDFSEventResponse evtResp = this.rc.getEvtBlockingStub()
                .getEvent(SDFSEventRequest.newBuilder().setPvolumeID(this.client.volumeid)
                        .setUuid(rresp.getEventID())
                        .build());
        msg = "Recovered " + mf.getPath() +
                " event id = " + evtResp.getEvent().getUuid() +
                " event message = " + evtResp.getEvent().getShortMsg() +
                " event endtime = " + evtResp.getEvent().getEndTime() +
                " event level = " + evtResp.getEvent().getLevel();
        SDFSLogger.getLog().info(msg);
        evt.setShortMsg(msg);

    }

    private static class CtLoc {
        int ct;
        long loc;
        boolean newdata;
    }
}