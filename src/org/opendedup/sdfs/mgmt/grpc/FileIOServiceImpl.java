package org.opendedup.sdfs.mgmt.grpc;

import org.opendedup.grpc.FileIOServiceGrpc;
import org.opendedup.grpc.dataReadRequest;
import org.opendedup.grpc.dataReadResponse;

import io.grpc.stub.StreamObserver;

import org.apache.commons.io.IOUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.io.events.MFileWritten;

import fuse.FuseException;

import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDedupFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.eventbus.EventBus;
import com.google.protobuf.ByteString;

import org.opendedup.grpc.*;

public class FileIOServiceImpl extends FileIOServiceGrpc.FileIOServiceImplBase {
    AtomicLong nextHandleNo = new AtomicLong(1000);
    ConcurrentHashMap<Long, DedupFileChannel> dedupChannels = new ConcurrentHashMap<Long, DedupFileChannel>();
    public String mountedVolume;
    public String connicalMountedVolume;
    public String mountPoint;
    public static AbstractHashEngine eng = HashFunctionPool.getHashEngine();

    private static EventBus eventBus = new EventBus();

    public static void registerListener(Object obj) {
        eventBus.register(obj);
    }

    public FileIOServiceImpl() throws IOException {
        this.mountedVolume = Main.volume.getPath();
        if (!this.mountedVolume.endsWith("/"))
            this.mountedVolume = this.mountedVolume + "/";
        this.connicalMountedVolume = new File(this.mountedVolume).getCanonicalPath();
        if (!mountPoint.endsWith("/"))
            mountPoint = mountPoint + "/";
        this.mountPoint = Main.volumeMountPoint;
        File f = new File(this.mountedVolume);
        if (!f.exists())
            f.mkdirs();
    }

    private void checkInFS(File f) throws FileIOError {
        try {

            if (!f.getCanonicalPath().startsWith(connicalMountedVolume)) {
                SDFSLogger.getLog()
                        .warn("Path is not in mounted [" + mountedVolume + "]folder " + f.getCanonicalPath());
                throw new FileIOError("data not in path " + f.getCanonicalPath(), errorCodes.EACCES);
            }
        } catch (IOException e) {
            SDFSLogger.getLog().warn("Path is not in mounted folder", e);
            throw new FileIOError("data not in path " + f.getPath(), errorCodes.EACCES);
        }
    }

    private File resolvePath(String path) throws FileIOError {
        String pt = mountedVolume + path.trim();
        File _f = new File(pt);

        if (!_f.exists()) {
            if (SDFSLogger.isDebug())
                SDFSLogger.getLog().debug("No such node");

            _f = null;
            throw new FileIOError("path does not exist [" + path + "]", errorCodes.ENOENT);
        }
        return _f;
    }

    private DedupFileChannel getFileChannel(String path, long handleNo) throws FileIOError {
        DedupFileChannel ch = this.dedupChannels.get(handleNo);
        if (ch == null) {
            File f = this.resolvePath(path);
            try {
                MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
                ch = mf.getDedupFile(false).getChannel(-2);
                if (this.dedupChannels.containsKey(handleNo)) {
                    ch.getDedupFile().unRegisterChannel(ch, -2);
                    ch = this.dedupChannels.get(handleNo);
                } else {
                    this.dedupChannels.put(handleNo, ch);
                }
                // SDFSLogger.getLog().info("Getting attributes for " + f.getPath());

            } catch (Exception e) {
                SDFSLogger.getLog().error(
                        "error processing get file channel for [" + path + "] and " + "file channel [" + handleNo + "]",
                        e);
                throw new FileIOError(
                        "error processing get file channel for [" + path + "] and " + "file channel [" + handleNo + "]",
                        errorCodes.EIO);

            }
        }
        return ch;
    }

    private DedupFileChannel getFileChannel(long handleNo) throws FileIOError {
        DedupFileChannel ch = this.dedupChannels.get(handleNo);
        if (ch == null) {
            SDFSLogger.getLog().debug("unable to read file " + handleNo);
            throw new FileIOError("unable to read file " + handleNo, errorCodes.EBADFD);
        }
        return ch;
    }

    @Override
    public void read(dataReadRequest request, StreamObserver<dataReadResponse> responseObserver) {
        dataReadResponse.Builder b = dataReadResponse.newBuilder();
        if (Main.volume.isOffLine()) {
            b.setError("Volume Offline");
            b.setErrorCode(errorCodes.ENODEV);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        try {
            ByteBuffer buf = ByteBuffer.allocate(request.getLen());
            DedupFileChannel ch = this.getFileChannel((Long) request.getFileHandle());
            int read = ch.read(buf, 0, buf.capacity(), request.getStart());
            if (read == -1)
                read = 0;
            b.setRead(read);
            b.setData(ByteString.copyFrom(buf, read));
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;

        } catch (FileIOError e) {
            b.setError(e.message);
            b.setErrorCode(e.code);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch (DataArchivedException e) {
            SDFSLogger.getLog().warn("Data is archived");
            b.setError("Data is archived");
            b.setErrorCode(errorCodes.ENODATA);
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to read file " + request.getFileHandle(), e);
            b.setError("unable to read file " + request.getFileHandle());
            b.setErrorCode(errorCodes.ENODATA);
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
        return;

    }

    private static class FileIOError extends Exception {
        errorCodes code;
        String message;

        public FileIOError(String message, errorCodes code) {
            super(message);
            this.message = message;
            this.code = code;

        }

        public FileIOError(errorCodes code) {
            super();
            this.code = code;
        }

        public void setErrorCode(errorCodes code) {
            this.code = code;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getMessage() {
            return this.message;
        }

        public errorCodes getErrorCode() {
            return this.code;
        }

    }

}