package org.opendedup.sdfs.mgmt.grpc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.protobuf.ByteString;

import org.opendedup.collections.InsertRecord;
import org.opendedup.util.CompressionUtils;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.Storage.CheckHashesRequest;
import org.opendedup.grpc.Storage.CheckHashesResponse;
import org.opendedup.grpc.Storage.ChunkEntry;
import org.opendedup.grpc.Storage.ChunkResponse;
import org.opendedup.grpc.Storage.HashLocPairP;
import org.opendedup.grpc.Storage.HashingInfoRequest;
import org.opendedup.grpc.Storage.HashingInfoResponse;
import org.opendedup.grpc.Storage.MetaDataDedupeFileRequest;
import org.opendedup.grpc.Storage.SparseDataChunkP;
import org.opendedup.grpc.Storage.SparseDedupeChunkWriteRequest;
import org.opendedup.grpc.Storage.SparseDedupeChunkWriteResponse;
import org.opendedup.grpc.Storage.SparseDedupeFileRequest;
import org.opendedup.grpc.Storage.WriteChunksRequest;
import org.opendedup.grpc.Storage.WriteChunksResponse;
import org.opendedup.grpc.Storage.hashtype;
import org.opendedup.grpc.StorageServiceGrpc.StorageServiceImplBase;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.mgmt.grpc.FileIOServiceImpl.FileIOError;
import org.opendedup.sdfs.servers.HCServiceProxy;

import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.stub.StreamObserver;

public class StorageServiceImpl extends StorageServiceImplBase {

    public static final String VARIABLE_SHA256 = "VARIABLE_SHA256";
    public static final String VARIABLE_SHA256_160 = "VARIABLE_SHA256_160";
    public static final String VARIABLE_HWY_160 = "VARIABLE_HWY_160";
    public static final String VARIABLE_HWY_128 = "VARIABLE_HWY_128";
    public static final String VARIABLE_HWY_256 = "VARIABLE_HWY_256";
    public static final String VARIABLE_MD5 = "VARIABLE_MD5";

    ExecutorService threadpool = Executors.newFixedThreadPool(Main.writeThreads * 4);
    ListeningExecutorService service = MoreExecutors.listeningDecorator(threadpool);

    public StorageServiceImpl() {

    }

    @Override
    public void getMetaDataDedupeFile(MetaDataDedupeFileRequest request,
            StreamObserver<ChunkResponse> responseObserver) {

        InputStream is = null;
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            ChunkResponse.Builder b = ChunkResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {

            try {
                File f = FileIOServiceImpl.resolvePath(request.getFilePath());

                is = new FileInputStream(f);
                byte[] buffer = new byte[32 * 1024];
                int length;
                while ((length = is.read(buffer)) > 0) {
                    ChunkResponse.Builder b = ChunkResponse.newBuilder();
                    b.setData(ByteString.copyFrom(ByteBuffer.wrap(buffer), length));
                    b.setLen(length);
                    responseObserver.onNext(b.build());
                }
                responseObserver.onCompleted();
            } catch (FileIOError e) {
                ChunkResponse.Builder b = ChunkResponse.newBuilder();
                b.setError(e.message);
                b.setErrorCode(e.code);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                ChunkResponse.Builder b = ChunkResponse.newBuilder();
                SDFSLogger.getLog().error("unable to get chunk for " + request.getFilePath(), e);
                b.setError("unable to get chunk for " + request.getFilePath());
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            } finally {
                try {
                    is.close();
                } catch (IOException e) {

                }
            }
        }
    }

    @Override
    public void getSparseDedupeFile(SparseDedupeFileRequest request, StreamObserver<ChunkResponse> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            ChunkResponse.Builder b = ChunkResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            InputStream is = null;
            try {

                File f = LongByteArrayMap.getFile(request.getGuid());
                if (!f.exists()) {
                    ChunkResponse.Builder b = ChunkResponse.newBuilder();
                    b.setError("Guild " + request.getGuid() + " does not exists");
                    b.setErrorCode(errorCodes.ENOENT);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                }

                is = new FileInputStream(f);
                byte[] buffer = new byte[32 * 1024];
                int length;
                while ((length = is.read(buffer)) > 0) {
                    ChunkResponse.Builder b = ChunkResponse.newBuilder();
                    b.setData(ByteString.copyFrom(ByteBuffer.wrap(buffer), length));
                    b.setLen(length);
                    responseObserver.onNext(b.build());
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                ChunkResponse.Builder b = ChunkResponse.newBuilder();
                SDFSLogger.getLog().error("unable to get chunk for " + request.getGuid(), e);
                b.setError("unable to get chunk for " + request.getGuid());
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            } finally {
                try {
                    is.close();
                } catch (IOException e) {

                }
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
                    responses.add(future.get());
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
                            return new InsertRecord(false, -1,0).toProtoBuf();
                        }
                    });
                    futures.add(lf);
                }
                for (ListenableFuture<org.opendedup.grpc.Storage.InsertRecord> lf : futures) {
                    responses.add(lf.get());
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
                ArrayList<HashLocPairP> misses = new ArrayList<HashLocPairP>();
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
                    future.get();
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
            } catch(HashWriteException e){
                SDFSLogger.getLog().error("unable to write sparse data chunk. Missed " + e.misses.size() + " hashes");
                b.setError("unable to write sparse data chunk");
                for(int i =0;i<e.misses.size();i++) {
                    b.setMissedAr(i, e.misses.get(i));
                }
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            catch (Exception e) {
                SDFSLogger.getLog().error("unable to write sparse data chunk", e);
                b.setError("unable to write sparse data chunk");
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

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

    private static class HashWriteException extends Exception {
        ArrayList<HashLocPairP> misses;
        public HashWriteException(ArrayList<HashLocPairP> misses) {
            this.misses = misses;

        }
    }

}
