package org.opendedup.sdfs.mgmt.grpc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.util.CompressionUtils;
import org.rocksdb.RocksIterator;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.Storage.AddReplicaSourceRequest;
import org.opendedup.grpc.Storage.AddReplicaSourceResponse;
import org.opendedup.grpc.Storage.CancelReplicationRequest;
import org.opendedup.grpc.Storage.CancelReplicationResponse;
import org.opendedup.grpc.Storage.CheckHashesRequest;
import org.opendedup.grpc.Storage.CheckHashesResponse;
import org.opendedup.grpc.Storage.ChunkEntry;
import org.opendedup.grpc.Storage.FileReplicationRequest;
import org.opendedup.grpc.Storage.FileReplicationResponse;
import org.opendedup.grpc.Storage.GetChunksRequest;
import org.opendedup.grpc.Storage.HashingInfoRequest;
import org.opendedup.grpc.Storage.HashingInfoResponse;
import org.opendedup.grpc.Storage.MetaDataDedupeFileRequest;
import org.opendedup.grpc.Storage.MetaDataDedupeFileResponse;
import org.opendedup.grpc.Storage.PauseReplicationRequest;
import org.opendedup.grpc.Storage.PauseReplicationResponse;
import org.opendedup.grpc.Storage.RemoveReplicaSourceRequest;
import org.opendedup.grpc.Storage.RemoveReplicaSourceResponse;
import org.opendedup.grpc.Storage.RestoreArchivesRequest;
import org.opendedup.grpc.Storage.RestoreArchivesResponse;
import org.opendedup.grpc.Storage.SparseDataChunkP;
import org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest;
import org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse;
import org.opendedup.grpc.Storage.SparseDedupeFileRequest;
import org.opendedup.grpc.Storage.VolumeEvent;
import org.opendedup.grpc.Storage.VolumeEventListenRequest;
import org.opendedup.grpc.Storage.WriteChunksRequest;
import org.opendedup.grpc.Storage.WriteChunksResponse;
import org.opendedup.grpc.Storage.hashtype;
import org.opendedup.grpc.StorageServiceGrpc.StorageServiceImplBase;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.RestoreArchive;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;
import org.opendedup.sdfs.io.Volume.ReplicationClientExistsException;
import org.opendedup.sdfs.io.Volume.ReplicationClientNotExistsException;
import org.opendedup.sdfs.mgmt.grpc.FileIOServiceImpl.FileIOError;
import org.opendedup.sdfs.mgmt.grpc.replication.ReplicationClient;
import org.opendedup.sdfs.notification.ReplicationImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

import com.google.common.eventbus.Subscribe;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.TimeoutException;

import io.grpc.stub.StreamObserver;

public class StorageServiceImpl extends StorageServiceImplBase {

    public static final String VARIABLE_SHA256 = "VARIABLE_SHA256";
    public static final String VARIABLE_SHA256_160 = "VARIABLE_SHA256_160";
    public static final String VARIABLE_HWY_160 = "VARIABLE_HWY_160";
    public static final String VARIABLE_HWY_128 = "VARIABLE_HWY_128";
    public static final String VARIABLE_HWY_256 = "VARIABLE_HWY_256";
    public static final String VARIABLE_MD5 = "VARIABLE_MD5";
    public static final ThreadPoolExecutor executor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads, 0L,
            TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
            new ThreadPoolExecutor.CallerRunsPolicy());

    ExecutorService threadpool = Executors.newFixedThreadPool(Main.writeThreads * 4);
    ListeningExecutorService service = MoreExecutors.listeningDecorator(threadpool);

    public StorageServiceImpl() {
    }

    @Override
    public void replicateRemoteFile(FileReplicationRequest request,
            StreamObserver<FileReplicationResponse> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            FileReplicationResponse.Builder b = FileReplicationResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            FileReplicationResponse.Builder b = FileReplicationResponse.newBuilder();

            try {
                ReplicationClient client = new ReplicationClient(request.getUrl(),
                        request.getRvolumeID(), request.getMtls());
                client.connect();
                SDFSEvent evt = client.replicate(request.getSrcFilePath(), request.getDstFilePath());
                b.setEventID(evt.uid);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                String msg = "Unable to replicate " + request.getSrcFilePath() + " to " + request.getDstFilePath() +
                        " with url " + request.getUrl() + " volumeid " + request.getPvolumeID();
                SDFSLogger.getLog().error(msg, e);
                b.setError(msg + " because " + e.getMessage());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void restoreArchives(RestoreArchivesRequest request,
            StreamObserver<RestoreArchivesResponse> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            RestoreArchivesResponse.Builder b = RestoreArchivesResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            RestoreArchivesResponse.Builder b = RestoreArchivesResponse.newBuilder();
            File f = new File(Main.volume.getPath() + File.separator + request.getFilePath());
            if (!f.exists()) {
                b.setError("Path not found [" + request.getFilePath() + "]");
                b.setErrorCode(errorCodes.ENOENT);
            }
            MetaDataDedupFile mf = MetaFileStore.getMF(f);
            try {
                RestoreArchive ar = new RestoreArchive(mf, -1);
                Thread th = new Thread(ar);
                th.start();
                b.setEventID(ar.fEvt.uid);
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to restore " + request.getFilePath(), e);
                b.setError("unable to restore " + request.getFilePath());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void pauseReplication(PauseReplicationRequest request,
            StreamObserver<PauseReplicationResponse> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            PauseReplicationResponse.Builder b = PauseReplicationResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            PauseReplicationResponse.Builder b = PauseReplicationResponse.newBuilder();
            try {
                if (SDFSEvent.getEvent(request.getEventID()) != null) {
                    ReplicationImportEvent evt = (ReplicationImportEvent) SDFSEvent.getEvent(request.getEventID());
                    if (evt.endTime > 0) {
                        b.setError("UUID " + request.getEventID() + " alread done");
                        b.setErrorCode(errorCodes.EALREADY);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    } else {
                        evt.pause(request.getPause());
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    }
                } else {
                    b.setError("UUID " + request.getEventID() + " not found");
                    b.setErrorCode(errorCodes.ENOENT);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                }
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to pause " + request.getEventID(), e);
                b.setError("unable to pause " + request.getEventID());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void cancelReplication(CancelReplicationRequest request,
            StreamObserver<CancelReplicationResponse> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            CancelReplicationResponse.Builder b = CancelReplicationResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            CancelReplicationResponse.Builder b = CancelReplicationResponse.newBuilder();
            try {

                if (SDFSEvent.getEvent(request.getEventID()) != null) {
                    ReplicationImportEvent evt = (ReplicationImportEvent) SDFSEvent.getEvent(request.getEventID());
                    if (evt.endTime > 0) {
                        b.setError("UUID " + request.getEventID() + " alread done");
                        b.setErrorCode(errorCodes.EALREADY);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    } else {
                        evt.cancel();
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    }
                } else {
                    b.setError("UUID " + request.getEventID() + " not found");
                    b.setErrorCode(errorCodes.ENOENT);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                }
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to cancel " + request.getEventID(), e);
                b.setError("unable to cancel " + request.getEventID());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }

        }
    }

    @Override
    public void getChunks(GetChunksRequest request, StreamObserver<ChunkEntry> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            ChunkEntry.Builder b = ChunkEntry.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {

            try {

                final ArrayList<Shard> cks = new ArrayList<Shard>();
                for (ChunkEntry entry : request.getChunksList()) {
                    byte[] b = new byte[entry.getHash().toByteArray().length];
                    entry.getHash().copyTo(b, 0);
                    Shard sh = new Shard(b);
                    sh.hashloc = Longs.toByteArray(-1);
                    cks.add(sh);
                }
                List<Future<ChunkEntry>> futures = new ArrayList<Future<ChunkEntry>>();

                CompletionService<ChunkEntry> executorCompletionService = new ExecutorCompletionService<>(threadpool);
                for (Shard sh : cks) {
                    futures.add(executorCompletionService.submit(sh));
                }
                try {
                    for (int i = 0; i < futures.size(); i++) {
                        responseObserver.onNext(executorCompletionService.take().get(5, TimeUnit.MINUTES));
                    }
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    SDFSLogger.getLog().warn("Exception restoring shards too more than 5 minutes", e);
                    ChunkEntry.Builder b = ChunkEntry.newBuilder();
                    b.setError("Exception restoring shards too more than 5 minutes");
                    b.setErrorCode(errorCodes.EIO);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                SDFSLogger.getLog().warn("Exception restoring shards", e);
                ChunkEntry.Builder b = ChunkEntry.newBuilder();
                b.setError("An error occured while reading chunks");
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void getMetaDataDedupeFile(MetaDataDedupeFileRequest request,
            StreamObserver<MetaDataDedupeFileResponse> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            MetaDataDedupeFileResponse.Builder b = MetaDataDedupeFileResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            MetaDataDedupeFileResponse.Builder b = MetaDataDedupeFileResponse.newBuilder();
            try {

                File f = FileIOServiceImpl.resolvePath(request.getFilePath());
                MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
                b.setFile(mf.toGRPC(false));
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            } catch (FileIOError e) {
                b.setError(e.message);
                b.setErrorCode(e.code);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to get read file for " + request.getFilePath(), e);
                b.setError("unable to read file for " + request.getFilePath());
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            }
        }
    }

    @Override
    public void getSparseDedupeFile(SparseDedupeFileRequest request,
            StreamObserver<SparseDataChunkP> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            SparseDataChunkP.Builder b = SparseDataChunkP.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                LongByteArrayMap ddb = LongByteArrayMap.getMap(request.getGuid());
                try {
                    ddb.setIndexed(true);
                    ddb.iterInit();
                    for (;;) {
                        LongKeyValue kv = ddb.nextKeyValue(false);
                        if (kv == null)
                            break;
                        SparseDataChunk ck = kv.getValue();
                        responseObserver.onNext(ck.toProtoBuf());
                    }
                } finally {
                    ddb.close();
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                SparseDataChunkP.Builder b = SparseDataChunkP.newBuilder();
                SDFSLogger.getLog().error("unable to get chunk for " + request.getGuid(), e);
                b.setError("unable to get chunk for " + request.getGuid());
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            } finally {

            }
        }
    }

    @Override
    public void checkHashes(CheckHashesRequest request, StreamObserver<CheckHashesResponse> responseObserver) {
        CheckHashesResponse.Builder b = CheckHashesResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                List<ByteString> hashes = request.getHashesList();
                List<Long> responses = new ArrayList<Long>();
                List<ListenableFuture<Long>> futures = new ArrayList<ListenableFuture<Long>>();

                for (ByteString bs : hashes) {
                    ListenableFuture<Long> lf = service.submit(() -> {
                        return HCServiceProxy.getHashesMap().get(bs.toByteArray());
                    });
                    futures.add(lf);
                }
                for (ListenableFuture<Long> future : futures) {
                    responses.add(future.get(300,TimeUnit.SECONDS));
                }

                b.addAllLocations(responses);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to check hashes ", e);
                b.setError("unable to check hashes");
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            }
        }
    }

    @Override
    public void writeChunks(WriteChunksRequest request, StreamObserver<WriteChunksResponse> responseObserver) {
        WriteChunksResponse.Builder b = WriteChunksResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");

            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                if (Main.volume.isFull()) {
                    b.setError("Volume Full");
                    b.setErrorCode(errorCodes.ENOSPC);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                DedupFileChannel ch = FileIOServiceImpl.dedupChannels.get(request.getFileHandle());
                if (ch == null) {
                    SDFSLogger.getLog().error("file handle " + request.getFileHandle() + " does not exist");
                    b.setError("file handle " + request.getFileHandle() + " does not exist");
                    b.setErrorCode(errorCodes.EACCES);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                List<ListenableFuture<org.opendedup.grpc.Storage.InsertRecord>> futures = new ArrayList<ListenableFuture<org.opendedup.grpc.Storage.InsertRecord>>();
                List<org.opendedup.grpc.Storage.InsertRecord> responses = new ArrayList<org.opendedup.grpc.Storage.InsertRecord>();
                for (ChunkEntry ent : request.getChunksList()) {
                    ListenableFuture<org.opendedup.grpc.Storage.InsertRecord> lf = service.submit(() -> {
                        byte[] chunk = ent.getData().toByteArray();
                        if (chunk.length > 0) {
                            if (ent.getCompressed()) {
                                chunk = CompressionUtils.decompressLz4(chunk, ent.getCompressedLength());
                            }

                            ChunkData cm = new ChunkData(ent.getHash().toByteArray(), chunk.length, chunk,
                                    ch.getDedupFile().getGUID());
                            InsertRecord ir = HCServiceProxy.getHashesMap().put(cm, true);
                            SDFSLogger.getLog().debug("write archive is " + Longs.fromByteArray(ir.getHashLocs()));
                            ch.setWrittenTo(true);
                            return ir.toProtoBuf();
                        } else {
                            return new InsertRecord(false, -1, 0).toProtoBuf();
                        }
                    });
                    futures.add(lf);
                }
                for (ListenableFuture<org.opendedup.grpc.Storage.InsertRecord> lf : futures) {
                    responses.add(lf.get(300,TimeUnit.SECONDS));
                }
                b.addAllInsertRecords(responses);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to write chunks ", e);
                b.setError("unable to write chunks");
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            }
        }
    }

    @Override
    public void writeSparseDataChunk(SparseDedupeChunkWriteRequest request,
            StreamObserver<SparseDedupeChunkWriteResponse> responseObserver) {
        SparseDedupeChunkWriteResponse.Builder b = SparseDedupeChunkWriteResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                if (Main.volume.isFull()) {
                    b.setError("Volume Full");
                    b.setErrorCode(errorCodes.ENOSPC);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                DedupFileChannel ch = FileIOServiceImpl.dedupChannels.get(request.getFileHandle());
                if (ch == null) {
                    SDFSLogger.getLog().error("file handle " + request.getFileHandle() + " does not exist");
                    b.setError("file handle " + request.getFileHandle() + " does not exist");
                    b.setErrorCode(errorCodes.EACCES);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                SparseDataChunk sp = null;
                if (request.getCompressed()) {
                    byte[] chunk = CompressionUtils.decompressLz4(request.getCompressedChunk().toByteArray(),
                            request.getUncompressedLen());

                    sp = new SparseDataChunk(SparseDataChunkP.parseFrom(chunk));
                } else {
                    sp = new SparseDataChunk(request.getChunk());
                }
                HashMap<String, kv> hs = new HashMap<String, kv>();
                for (Entry<Integer, HashLocPair> e : sp.getFingers().entrySet()) {
                    if (!e.getValue().inserted) {
                        String key = BaseEncoding.base64().encode(e.getValue().hash);
                        if (!hs.containsKey(key)) {
                            kv _kv = new kv();
                            _kv.ct = 0;
                            _kv.key = e.getValue().hash;
                            _kv.pos = Longs.fromByteArray(e.getValue().hashloc);
                            hs.put(key, _kv);
                        }
                        kv _kv = hs.get(key);
                        _kv.ct += 1;
                    }
                }
                List<ListenableFuture<Long>> futures = new ArrayList<ListenableFuture<Long>>();
                for (Entry<String, kv> e : hs.entrySet()) {
                    if (e.getValue().ct <= 0) {
                        throw new IOException("Count must be positive ct=" +
                                e.getValue().ct);
                    }
                    ListenableFuture<Long> lf = service.submit(() -> {
                        long pos = DedupFileStore.addRef(e.getValue().key,
                                e.getValue().pos, e.getValue().ct);
                        if (pos != e.getValue().pos) {
                            throw new IOException("Inserted Archive does not match current current=" +
                                    pos + " interted=" + e.getValue().pos);
                        }
                        return pos;
                    });
                    futures.add(lf);

                }
                for (ListenableFuture<Long> future : futures) {
                    future.get(300, TimeUnit.SECONDS);
                }

                ch.setWrittenTo(true);
                ch.getDedupFile().updateMap(sp, request.getFileLocation());
                synchronized (ch) {
                    long ep = sp.getFpos() + sp.len;
                    if (ep > ch.getFile().length()) {
                        ch.getFile().setLength(ep, false);
                        SDFSLogger.getLog().debug("Set length to " + ep + " " + sp.len +
                                " " + ch.getFile().length() + " for " + ch.getFile().getPath());
                    } else {
                        SDFSLogger.getLog()
                                .debug("no length to " + sp.getFpos() + " " + request.getChunk().getLen() + " " + sp.len
                                        +
                                        " " + ch.getFile().length() + " for " + ch.getFile().getPath());
                    }
                }
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception er) {
                SDFSLogger.getLog().error("unable to write sparse data chunk", er);
                b.setError("unable to write sparse data chunk");
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            }
        }

    }

    @Override
    public void listReplLogs(VolumeEventListenRequest request, StreamObserver<VolumeEvent> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            VolumeEvent.Builder b = VolumeEvent.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                RocksIterator iter = Main.volume.getRSerivce().getIterator(request.getStartSequence());
                for (iter.seek(Longs.toByteArray(request.getStartSequence())); iter.isValid(); iter.next()) {
                    byte[] val = iter.value();
                    VolumeEvent.Builder b = VolumeEvent.newBuilder();
                    b.mergeFrom(val);
                    responseObserver.onNext(b.build());
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                VolumeEvent.Builder b = VolumeEvent.newBuilder();
                SDFSLogger.getLog().warn("unable to create repl listener", e);
                b.setError("unable to create repl listener");
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void addReplicaSource(AddReplicaSourceRequest request,
            StreamObserver<AddReplicaSourceResponse> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
            AddReplicaSourceResponse.Builder b = AddReplicaSourceResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                Main.volume.addReplicationClient(request.getUrl().toLowerCase(), request.getRvolumeID(),
                        request.getMtls());
                AddReplicaSourceResponse.Builder b = AddReplicaSourceResponse.newBuilder();
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();

            } catch (ReplicationClientExistsException e) {
                SDFSLogger.getLog().warn("Unable to add replication source", e);
                AddReplicaSourceResponse.Builder b = AddReplicaSourceResponse.newBuilder();
                b.setError("Unable to add replication source");
                b.setErrorCode(errorCodes.EEXIST);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                SDFSLogger.getLog().warn("Unable to add replication source", e);
                AddReplicaSourceResponse.Builder b = AddReplicaSourceResponse.newBuilder();
                b.setError("Unable to add replication source");
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void removeReplicaSource(RemoveReplicaSourceRequest request,
            StreamObserver<RemoveReplicaSourceResponse> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_WRITE)) {
            RemoveReplicaSourceResponse.Builder b = RemoveReplicaSourceResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                Main.volume.removeReplicationClient(request.getUrl().toLowerCase(), request.getRvolumeID());
                RemoveReplicaSourceResponse.Builder b = RemoveReplicaSourceResponse.newBuilder();
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();

            } catch (ReplicationClientNotExistsException e) {
                SDFSLogger.getLog().warn("Unable to remove replication source", e);
                RemoveReplicaSourceResponse.Builder b = RemoveReplicaSourceResponse.newBuilder();
                b.setError("Unable to remove replication source");
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                SDFSLogger.getLog().warn("Unable to remove replication source", e);
                RemoveReplicaSourceResponse.Builder b = RemoveReplicaSourceResponse.newBuilder();
                b.setError("Unable to remove replication source");
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void subscribeToVolume(VolumeEventListenRequest request, StreamObserver<VolumeEvent> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            VolumeEvent.Builder b = VolumeEvent.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            ReplEventListener l = null;
            try {
                l = new ReplEventListener(responseObserver);
                Main.volume.getRSerivce().registerListener(l);
                while (!l.exceptionThrown) {
                    Thread.sleep(1000);
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                VolumeEvent.Builder b = VolumeEvent.newBuilder();
                SDFSLogger.getLog().warn("unable to create repl listener", e);
                b.setError("unable to create repl listener");
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onCompleted();
            } finally {
                if (l != null) {
                    Main.volume.getRSerivce().unregisterListener(l);
                }
            }
        }
    }

    public class ReplEventListener {

        StreamObserver<VolumeEvent> responseObserver;
        boolean evtsent;
        boolean exceptionThrown;

        public ReplEventListener(
                StreamObserver<VolumeEvent> responseObserver) {
            this.responseObserver = responseObserver;

        }

        @Subscribe
        public void nvent(org.opendedup.sdfs.io.events.ReplEvent evt) {

            try {
                responseObserver.onNext(evt.getVolumeEvent());

            } catch (Exception e) {
                VolumeEvent.Builder b = VolumeEvent.newBuilder();
                SDFSLogger.getLog().info("nSent Event");
                b.setError("Unable to marshal event");
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                exceptionThrown = true;
            }
        }
    }

    @Override
    public void hashingInfo(HashingInfoRequest request, StreamObserver<HashingInfoResponse> responseObserver) {
        HashingInfoResponse.Builder b = HashingInfoResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.CONFIG_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                b.setChunkSize(Main.CHUNK_LENGTH);
                if (Main.hashType.equalsIgnoreCase(VARIABLE_MD5)) {
                    b.setHashtype(hashtype.MD5);
                } else if (Main.hashType.equalsIgnoreCase(VARIABLE_SHA256)) {
                    b.setHashtype(hashtype.SHA256);
                } else {
                    b.setHashtype(hashtype.UNSUPPORTED);
                }
                b.setMapVersion(Main.MAPVERSION);
                b.setMaxSegmentSize(HashFunctionPool.maxLen);
                b.setMinSegmentSize(HashFunctionPool.minLen);
                b.setWindowSize(HashFunctionPool.bytesPerWindow);
                b.setPolyNumber(10923124345206883L);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {

                SDFSLogger.getLog().error("unable get hashing info", e);
                b.setError("unable get hashing info");
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            }
        }
    }

    private static class kv {
        long pos;
        int ct = 0;
        byte[] key;
    }

    public static class Shard implements Callable<ChunkEntry> {
        public final byte[] hash;
        public byte[] hashloc;
        public byte[] ck;
        public int apos;

        public Shard(byte[] hash) {
            this.hash = hash;
        }

        @Override
        public ChunkEntry call() {
            try {
                if (Arrays.equals(hash, WritableCacheBuffer.bk)) {
                    this.ck = WritableCacheBuffer.blankBlock;
                } else {
                    this.ck = HCServiceProxy.fetchChunk(hash, hashloc, true);
                }
                ChunkEntry ce = ChunkEntry.newBuilder().setData(ByteString.copyFrom(this.ck))
                        .setHash(ByteString.copyFrom(this.hash)).build();
                return ce;

            } catch (DataArchivedException e) {
                ChunkEntry.Builder b = ChunkEntry.newBuilder();
                SDFSLogger.getLog().warn("Data Archive error", e);
                b.setError("Data Archived");
                b.setErrorCode(errorCodes.EARCHIVEIO);
                return b.build();
            } catch (Throwable e) {
                ChunkEntry.Builder b = ChunkEntry.newBuilder();
                SDFSLogger.getLog().warn("error while getting blocks ", e);
                b.setError("error while getting blocks ");
                b.setErrorCode(errorCodes.EIO);
                return b.build();
            }

        }

    }

}
