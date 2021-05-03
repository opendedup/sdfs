package org.opendedup.sdfs.mgmt.grpc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

import org.opendedup.collections.InsertRecord;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.Storage.CheckHashesRequest;
import org.opendedup.grpc.Storage.CheckHashesResponse;
import org.opendedup.grpc.Storage.ChunkEntry;
import org.opendedup.grpc.Storage.ChunkResponse;
import org.opendedup.grpc.Storage.HashingInfoRequest;
import org.opendedup.grpc.Storage.HashingInfoResponse;
import org.opendedup.grpc.Storage.MetaDataDedupeFileRequest;
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
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.mgmt.grpc.FileIOServiceImpl.FileIOError;
import org.opendedup.sdfs.servers.HCServiceProxy;

import io.grpc.stub.StreamObserver;

public class StorageServiceImpl extends StorageServiceImplBase {

    public static final String VARIABLE_SHA256 = "VARIABLE_SHA256";
	public static final String VARIABLE_SHA256_160 = "VARIABLE_SHA256_160";
	public static final String VARIABLE_HWY_160 = "VARIABLE_HWY_160";
	public static final String VARIABLE_HWY_128 = "VARIABLE_HWY_128";
	public static final String VARIABLE_HWY_256 = "VARIABLE_HWY_256";
	public static final String VARIABLE_MD5 = "VARIABLE_MD5";

    public StorageServiceImpl() {

    }

    @Override
    public void getMetaDataDedupeFile(MetaDataDedupeFileRequest request,
            StreamObserver<ChunkResponse> responseObserver) {
        InputStream is = null;

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

    @Override
    public void getSparseDedupeFile(SparseDedupeFileRequest request, StreamObserver<ChunkResponse> responseObserver) {
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

    @Override
    public void checkHashes(CheckHashesRequest request, StreamObserver<CheckHashesResponse> responseObserver) {
        CheckHashesResponse.Builder b = CheckHashesResponse.newBuilder();
        try {

            List<ByteString> hashes = request.getHashesList();
            List<Long> responses = new ArrayList<Long>(hashes.size());
            for (ByteString bs : hashes) {
                responses.add(HCServiceProxy.getHashesMap().get(bs.toByteArray()));
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

    @Override
    public void writeChunks(WriteChunksRequest request, StreamObserver<WriteChunksResponse> responseObserver) {
        WriteChunksResponse.Builder b = WriteChunksResponse.newBuilder();
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

            List<org.opendedup.grpc.Storage.InsertRecord> responses = new ArrayList<org.opendedup.grpc.Storage.InsertRecord>(
                    request.getChunksCount());
            for (ChunkEntry ent : request.getChunksList()) {
                byte[] chunk = ent.getData().toByteArray();
                ChunkData cm = new ChunkData(ent.getHash().toByteArray(), chunk.length, chunk,
                        ch.getDedupFile().getGUID());
                InsertRecord ir = HCServiceProxy.getHashesMap().put(cm, true);
                responses.add(ir.toProtoBuf());
            }
            b.addAllInsertRecords(responses);

        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to check hashes ", e);
            b.setError("unable to check hashes");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;

        }
    }

    @Override
    public void writeSparseDataChunk(SparseDedupeChunkWriteRequest request,
            StreamObserver<SparseDedupeChunkWriteResponse> responseObserver) {
        SparseDedupeChunkWriteResponse.Builder b = SparseDedupeChunkWriteResponse.newBuilder();
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
            SparseDataChunk sp = new SparseDataChunk(request.getChunk());
            ch.getDedupFile().updateMap(sp, request.getFileLocation());
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to write sparse data chunk", e);
            b.setError("unable to write sparse data chunk");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;

        }
    }

    @Override
    public void hashingInfo(HashingInfoRequest request, StreamObserver<HashingInfoResponse> responseObserver) {
        HashingInfoResponse.Builder b = HashingInfoResponse.newBuilder();
        try {
            b.setChunkSize(Main.CHUNK_LENGTH);
            if(Main.hashType.equalsIgnoreCase(VARIABLE_MD5)) {
                b.setHashtype(hashtype.MD5);
            }
            else if(Main.hashType.equalsIgnoreCase(VARIABLE_SHA256)) {
                b.setHashtype(hashtype.SHA256);
            }
            else {
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
