package org.opendedup.sdfs.mgmt.grpc.replication;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.grpc.FileInfo.FileInfoResponse;
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
import org.opendedup.sdfs.filestore.HashBlobArchive;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
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
    ReplicationClient client;
    boolean canceled = false;
    boolean paused = false;
    ReplicationConnection rc = null;

    private Gson objGson = new GsonBuilder().setPrettyPrinting().create();
    private static HashMap<String, ReentrantLock> actives = new HashMap<String, ReentrantLock>();

    public ImportFile(ReplicationClient client, ReplicationImportEvent evt) {
        this.client = client;
        this.evt = evt;
        evt.dst = evt.dst.replaceFirst("\\.\\/", "");
        evt.dst = evt.dst.replaceFirst("\\.\\\\", "");

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
            if (actives.containsKey(evt.dst)) {
                active = actives.get(evt.dst);
            } else {
                active = new ReentrantLock();
                actives.put(evt.dst, active);
            }
        }

        Exception e = null;
        for (int i = 0; i < client.failRetries; i++) {
            active.lock();
            synchronized (client.activeImports) {
                List<ReplicationImportEvent> al = null;
                if (client.activeImports.containsKey(evt.dst)) {
                    al = client.activeImports.get(evt.dst);
                } else {
                    al = new ArrayList<ReplicationImportEvent>();
                }
                al.add(evt);
                client.activeImports.put(evt.dst, al);
            }
            try {
                replicate();
                this.evt.endEvent("Replication Successful");
                e = null;
                active.unlock();

                break;
            } catch (FileNotFoundException | ReplicationCanceledException e1) {
                if (e1 instanceof FileNotFoundException) {
                    SDFSLogger.getLog().warn("File " + evt.src + " not found on source");
                }
                SDFSLogger.getLog().info("Replication Canceled");
                try {
                    synchronized (client.activeImports) {
                        List<ReplicationImportEvent> al = null;
                        if (client.activeImports.containsKey(evt.dst)) {
                            al = client.activeImports.get(evt.dst);
                            al.remove(this.evt);
                        } else {
                            al = new ArrayList<ReplicationImportEvent>();
                        }
                        if (al.size() == 0) {
                            client.activeImports.remove(evt.dst);
                        }
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
                            + client.volumeid + " for " + evt.src + " will retry in 5 minutes", e1);
                    e = e1;
                    this.evt.setShortMsg("unable to complete replication to " + client.url + " volume id "
                            + client.volumeid + " for " + evt.src + " will retry in 5 minutes");
                    String pt = Main.volume.getPath() + File.separator + evt.dst;
                    File _f = new File(pt);
                    try {

                        if (_f.exists()) {
                            FileIOServiceImpl.ImmuteLinuxFDFileFile(_f.getPath(), false);
                            MetaFileStore.getMF(_f.getAbsolutePath()).clearRetentionLock();
                            MetaFileStore.removeMetaFile(_f.getPath(), false, false, false);
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
                                client.activeImports.remove(evt.dst);
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
                        + client.volumeid + " for " + evt.src, e);
                synchronized (client.activeImports) {
                    client.activeImports.remove(evt.dst);
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
                actives.remove(evt.dst);
            }
        }

    }

    void replicate() throws ReplicationCanceledException, Exception {
        rc = client.getReplicationConnection();
        try {
            SDFSLogger.getLog().info("Importing " + evt.src + " to " + evt.dst);
            String pt = Main.volume.getPath() + File.separator + evt.dst;
            File _f = new File(pt);
            if (_f.exists() && !evt.overwrite) {
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
            evt.setShortMsg("Importing metadata file for " + evt.dst);
            MetaDataDedupFile mf = null;
            FileInfoResponse fi = null;
            try {
                fi = downloadMetaFile();
                if (!_f.exists()) {
                    mf = MetaDataDedupFile.fromProtoBuf(fi, _f.getPath());
                    String ng = UUID.randomUUID().toString();
                    mf.setLength(0, true);
                    mf.setDfGuid(ng);
                    mf.sync();
                } else {
                    mf = MetaFileStore.getMF(_f);
                    if (mf.getGUID() == null) {
                        String ng = UUID.randomUUID().toString();
                        mf.setLength(0, true);
                        mf.setDfGuid(ng);
                        mf.sync();
                    }
                }
            } catch (ReplicationCanceledException e) {

                throw e;
            }
            long expectedSize = evt.dstOffset + evt.srcSize;
            if (evt.srcSize == 0) {
                expectedSize += fi.getSize();
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
            evt.setShortMsg("Importing map for " + evt.dst);
            String sguid = fi.getMapGuid();

            try {
                if (sguid != null && sguid.trim().length() > 0 && fi.getSize() > 0) {
                    long endPos = evt.srcOffset + evt.srcSize;
                    if (evt.srcSize == 0) {
                        endPos += fi.getSize();
                    }
                    downloadDDB(mf, sguid, endPos);
                }
                mf.setRetentionLock();
                CloseFile.close(mf, true);
            } catch (ReplicationCanceledException e) {
                if (_f.exists()) {
                    FileIOServiceImpl.ImmuteLinuxFDFileFile(mf.getPath(), false);
                    MetaFileStore.getMF(mf.getAbsolutePath()).clearRetentionLock();
                    MetaFileStore.removeMetaFile(mf.getPath(), false, false, false);
                }
                throw e;
            }
            mf.setLength(expectedSize, true);

            MetaFileStore.removedCachedMF(mf.getPath());
            MetaFileStore.addToCache(mf);
            evt.addCount(1);
            FileIOServiceImpl.ImmuteLinuxFDFileFile(mf.getPath(), true);
            evt.endEvent("Import Successful for " + evt.dst, SDFSEvent.INFO);
            SDFSLogger.getLog().info("Imported " + evt.dst);

            synchronized (client.activeImports) {
                client.activeImports.remove(evt.dst);
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

    private FileInfoResponse downloadMetaFile() throws Exception {
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
                .setFilePath(evt.src).build();
        MetaDataDedupeFileResponse crs = this.rc.getStorageBlockingStub().getMetaDataDedupeFile(mr);
        if (crs.getErrorCode() != errorCodes.NOERR) {
            if (crs.getErrorCode() == errorCodes.ENOENT) {
                throw new FileNotFoundException(crs.getError());
            } else {
                throw new IOException(
                        "Error found during file request " + crs.getErrorCode() + " err : " + crs.getError());
            }
        }
        String pt = Main.volume.getPath() + File.separator + evt.dst;
        File _f = new File(pt);
        if (_f.exists() && evt.srcOffset == 0 && evt.dstOffset == 0
                && (evt.srcSize == 0 || evt.srcSize == crs.getFile().getSize())) {
            FileIOServiceImpl.ImmuteLinuxFDFileFile(_f.getPath(), false);
            MetaFileStore.getMF(_f.getAbsolutePath()).clearRetentionLock();
            MetaFileStore.removeMetaFile(_f.getPath(), false, false, false);
        }
        this.evt.fileSize = crs.getFile().getSize();

        return crs.getFile();
    }

    private void downloadDDB(MetaDataDedupFile mf, String guid, long endPos) throws Exception {
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

        long spos = this.getChuckPosition(evt.srcOffset);
        long dpos = this.getChuckPosition(evt.dstOffset);
        ByteBuffer ebf = null;
        ByteBuffer fbf = null;

        if (evt.srcSize > 0) {

            long expectedSize = evt.dstOffset + evt.srcSize;
            if (expectedSize < mf.length()) {
                long edpos = this.getChuckPosition(evt.dstOffset + Main.CHUNK_LENGTH);
                int el = (int) (edpos - evt.dstOffset);
                if (expectedSize < (dpos + el)) {
                    el = (int) (expectedSize - dpos);
                }
                if (el > 0) {
                    DedupFileChannel ch = mf.getDedupFile(false).getChannel(-2);
                    fbf = ByteBuffer.wrap(new byte[el]);
                    ch.read(ebf, 0, el, evt.dstOffset);
                    ch.getDedupFile().unRegisterChannel(ch, -2);
                    ch.getDedupFile().forceClose();
                }
            }
            if (expectedSize >= mf.length()) {
                int rl = (int) (evt.dstOffset - dpos);
                if (rl > 0) {
                    DedupFileChannel ch = mf.getDedupFile(false).getChannel(-2);
                    ebf = ByteBuffer.wrap(new byte[rl]);
                    ch.read(ebf, 0, rl, dpos);
                    ch.getDedupFile().unRegisterChannel(ch, -2);
                    ch.getDedupFile().forceClose();
                }
            }
        }
        LongByteArrayMap mp = LongByteArrayMap.getMap(mf.getDfGuid());
        mf.getIOMonitor().clearFileCounters(false);
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
            SparseDataChunk sp = new SparseDataChunk(cr);
            if (sp.getFpos() >= spos && sp.getFpos() < endPos) {
                SDFSLogger.getLog().debug("fpos = " + sp.getFpos() + " spos = " + spos + " endPos " + endPos + " dpos = " + dpos);
                SparseDataChunk ck = importSparseDataChunk(sp, mf);
                mp.put(dpos, ck);
                dpos += ck.len;
            } else if (sp.getFpos() >= endPos) {
                break;

            }

        }

        mp.close();
        if (ebf != null) {
            DedupFileChannel ch = mf.getDedupFile(false).getChannel(-2);
            long place = this.getChuckPosition(evt.dstOffset);
            int rl = (int) ((int) evt.dstOffset - (int) place);
            ebf.position(0);
            ch.writeFile(ebf, rl, 0, place, false);
            SDFSLogger.getLog().debug("Wrote e " + rl + " at " + evt.dstOffset);
            ch.getDedupFile().unRegisterChannel(ch, -2);
            ch.getDedupFile().forceClose();
        }
        if (fbf != null) {
            DedupFileChannel ch = mf.getDedupFile(false).getChannel(-2);
            fbf.position(0);
            ch.writeFile(fbf, fbf.capacity(), 0, evt.dstOffset, false);
            SDFSLogger.getLog().debug("Wrote f " + fbf.capacity() + " at " + evt.dstOffset);
            ch.getDedupFile().unRegisterChannel(ch, -2);
            ch.getDedupFile().forceClose();
        }
        HashBlobArchive.sync(mf.getDfGuid());
    }

    private long getChuckPosition(long location) {
        long place = location / Main.CHUNK_LENGTH;
        place = place * Main.CHUNK_LENGTH;
        return place;
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
                SDFSLogger.getLog().warn(StringUtils.getHexString(hf.hashBytes(dt).asBytes())
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
            mf.setLength(mf.length() + p.nlen, false);
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
