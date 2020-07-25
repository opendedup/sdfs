package org.opendedup.sdfs.mgmt.grpc;

import io.grpc.stub.StreamObserver;

import org.apache.commons.io.IOUtils;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.io.events.MFileWritten;
import org.opendedup.sdfs.mgmt.CloseFile;
import org.opendedup.sdfs.notification.SDFSEvent;

import fuse.FuseException;
import fuse.FuseFtypeConstants;

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
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.EventBus;
import com.google.protobuf.ByteString;

import org.opendedup.grpc.*;

public class FileIOServiceImpl extends FileIOServiceGrpc.FileIOServiceImplBase {
    AtomicLong nextHandleNo = new AtomicLong(1000);

    private LoadingCache<String, DirectoryStream<Path>> fileListers = CacheBuilder.newBuilder().maximumSize(100)
            .expireAfterAccess(5, TimeUnit.MINUTES).build(new CacheLoader<String, DirectoryStream<Path>>() {

                @Override
                public DirectoryStream<Path> load(String key) throws Exception {
                    throw new IOException("Key Not Found [" + key + "]");
                }

            });
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
        this.mountPoint = Main.volumeMountPoint;
        if (!mountPoint.endsWith("/"))
            mountPoint = mountPoint + "/";
        
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
    public void mkDir(MkDirRequest request, StreamObserver<MkDirResponse> responseObserver) {
        MkDirResponse.Builder b = MkDirResponse.newBuilder();
        File f = new File(this.mountedVolume + request.getPath());
        try {
            this.checkInFS(f);
        } catch (FileIOError e) {
            SDFSLogger.getLog().warn("unable", e);
            b.setError(e.message);
            b.setErrorCode(e.code);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        if (Main.volume.isOffLine()) {
            b.setError("volume offline");
            b.setErrorCode(errorCodes.ENAVAIL);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        if (Main.volume.isFull()) {
            b.setError("volume offline");
            b.setErrorCode(errorCodes.ENOSPC);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        if (f.exists()) {
            b.setError("folder exists");
            b.setErrorCode(errorCodes.EEXIST);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        try {
            MetaFileStore.mkDir(f, -1);
            try {
                eventBus.post(new MFileWritten(MetaFileStore.getMF(f), true));
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to post mfilewritten " + f.getPath(), e);
                b.setError("unable to post mfilewritten " + f.getPath());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        } catch (IOException e) {
            SDFSLogger.getLog().error("error while making dir " + f.getPath(), e);
            b.setError("error while making dir " + f.getPath());
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
    }

    @Override
    public void mkDirAll(MkDirRequest request, StreamObserver<MkDirResponse> responseObserver) {
        MkDirResponse.Builder b = MkDirResponse.newBuilder();
        File f = new File(this.mountedVolume + request.getPath());
        try {
            this.checkInFS(f);
        } catch (FileIOError e) {
            SDFSLogger.getLog().warn("unable", e);
            b.setError(e.message);
            b.setErrorCode(e.code);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        if (Main.volume.isOffLine()) {
            b.setError("volume offline");
            b.setErrorCode(errorCodes.ENAVAIL);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        if (Main.volume.isFull()) {
            b.setError("volume offline");
            b.setErrorCode(errorCodes.ENOSPC);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        if (f.exists()) {
            b.setError("folder exists");
            b.setErrorCode(errorCodes.EEXIST);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        try {
            MetaFileStore.mkDirs(f, -1);
            try {
                eventBus.post(new MFileWritten(MetaFileStore.getMF(f), true));
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to post mfilewritten " + f.getPath(), e);
                b.setError("unable to post mfilewritten " + f.getPath());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        } catch (IOException e) {
            SDFSLogger.getLog().error("error while making dir " + f.getPath(), e);
            b.setError("error while making dir " + f.getPath());
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
    }


    private int getFtype(String path) throws FileIOError {
        // SDFSLogger.getLog().info("Path=" + path);
        String pt = mountedVolume + path;
        File _f = new File(pt);

        if (!Files.exists(Paths.get(_f.getPath()), LinkOption.NOFOLLOW_LINKS)) {
            throw new FileIOError("path not found " + path, errorCodes.ENOENT);
        }
        Path p = Paths.get(_f.getPath());
        try {
            boolean isSymbolicLink = Files.isSymbolicLink(p);
            if (isSymbolicLink)
                return FuseFtypeConstants.TYPE_SYMLINK;
            else if (_f.isDirectory())
                return FuseFtypeConstants.TYPE_DIR;
            else if (_f.isFile()) {
                return FuseFtypeConstants.TYPE_FILE;
            }
        } catch (Exception e) {
            _f = null;
            p = null;
            SDFSLogger.getLog().warn(path + " does not exist", e);
            throw new FileIOError("path not found " + path, errorCodes.ENOENT);
        }
        SDFSLogger.getLog().error("could not determine type for " + path);
        throw new FileIOError("could not determine type for " + path, errorCodes.ENOENT);
    }

    @Override
    public void rmDir(RmDirRequest request, StreamObserver<RmDirResponse> responseObserver) {
        RmDirResponse.Builder b = RmDirResponse.newBuilder();
        try {

            if (this.getFtype(request.getPath()) == FuseFtypeConstants.TYPE_SYMLINK) {

                File f = new File(mountedVolume + request.getPath());

                // SDFSLogger.getLog().info("deleting symlink " + f.getCanonicalPath());
                if (!f.delete()) {
                    f = null;
                    b.setError("unable to delete " + f.getPath());
                    b.setErrorCode(errorCodes.EACCES);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } else {
                File f = resolvePath(request.getPath());
                if (f.getName().equals(".") || f.getName().equals("..")) {
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } else {
                    try {

                        if (MetaFileStore.removeMetaFile(f.getCanonicalPath(), false, false, true)) {
                            responseObserver.onNext(b.build());
                            responseObserver.onCompleted();
                            return;
                        } else {
                            b.setError("unable to delete " + f.getPath());
                            b.setErrorCode(errorCodes.ENOTEMPTY);
                            responseObserver.onNext(b.build());
                            responseObserver.onCompleted();
                            return;
                        }

                    } catch (Exception e) {
                        SDFSLogger.getLog().error("unable to remove " + request.getPath(), e);
                        b.setError("unable to delete " + request.getPath());
                        b.setErrorCode(errorCodes.EACCES);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                        return;
                    }

                }
            }
        } catch (FileIOError e) {
            SDFSLogger.getLog().warn("unable", e);
            b.setError(e.message);
            b.setErrorCode(e.code);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to remove " + request.getPath(), e);
            b.setError("unable to delete " + request.getPath());
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
    }

    @Override
    public void unlink(UnlinkRequest request, StreamObserver<UnlinkResponse> responseObserver) {
        UnlinkResponse.Builder b = UnlinkResponse.newBuilder();
        try {
            String path = request.getPath();
            if (SDFSLogger.isDebug())
                SDFSLogger.getLog().debug("removing " + path);
            if (!Main.safeClose) {
                try {
                    this.getFileChannel(path, -1).getDedupFile().forceClose();
                } catch (IOException e) {
                    SDFSLogger.getLog().error("unable to close file " + path, e);
                }
            }
            if (this.getFtype(path) == FuseFtypeConstants.TYPE_SYMLINK) {
                Path p = new File(mountedVolume + path).toPath();
                // SDFSLogger.getLog().info("deleting symlink " + f.getPath());
                try {
                    MetaDataDedupFile mf = MetaFileStore.getMF(this.resolvePath(path));
                    eventBus.post(new MFileDeleted(mf));
                    Files.delete(p);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } catch (IOException e) {
                    SDFSLogger.getLog().warn("unable to delete symlink " + p);
                    b.setError("unable to delete symlink " + p);
                    b.setErrorCode(errorCodes.ENNOSYS);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
            } else {

                File f = this.resolvePath(path);
                try {
                    MetaFileStore.getMF(f).clearRetentionLock();
                    if (MetaFileStore.removeMetaFile(f.getPath(), true, true, true)) {
                        // SDFSLogger.getLog().info("deleted file " +
                        // f.getPath());
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                        return;
                    } else {
                        SDFSLogger.getLog().warn("unable to delete file " + f.getPath());
                        b.setError("unable to delete symlink " + f.getPath());
                        b.setErrorCode(errorCodes.EACCES);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                        return;
                    }
                } catch (Exception e) {
                    SDFSLogger.getLog().error("unable to delete file " + path, e);
                    b.setError("unable to delete " + f.getPath());
                    b.setErrorCode(errorCodes.EACCES);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
            }
        } catch (FileIOError e) {
            b.setError(e.message);
            b.setErrorCode(e.code);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch (Exception e) {
            SDFSLogger.getLog().error(request.getPath(), e);
            b.setError("unable to delete " + request.getPath());
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
    }

    @Override
    public void write(DataWriteRequest request, StreamObserver<DataWriteResponse> responseObserver) {
        DataWriteResponse.Builder b = DataWriteResponse.newBuilder();
        if (Main.volume.isOffLine()) {
            b.setError("Volume Offline");
            b.setErrorCode(errorCodes.ENODEV);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        try {
            if (Main.volume.isFull()) {
                b.setError("Volume Full");
                b.setErrorCode(errorCodes.ENOSPC);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }

            DedupFileChannel ch = this.getFileChannel(request.getFileHandle());
            ByteBuffer buf = ByteBuffer.allocate(request.getLen());
            request.getData().copyTo(buf);
            buf.position(0);
            try {
                SDFSLogger.getLog().debug(
                        "Writing " + ch.openFile().getPath() + " pos=" + request.getStart() + " len=" + buf.capacity());
                /*
                 * byte[] k = new byte[buf.capacity()]; buf.get(k); buf.position(0);
                 * Files.write(Paths.get("c:/temp/" + ch.openFile().getName()), new
                 * String(offset + "," + buf.capacity() + "," +
                 * StringUtils.byteToHexString(eng.getHash(k)) + "\n") .getBytes(),
                 * StandardOpenOption.APPEND,StandardOpenOption.CREATE);
                 */
                ch.writeFile(buf, buf.capacity(), 0, request.getStart(), true);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to write to file" + request.getFileHandle(), e);
                b.setError("unable to write to file" + request.getFileHandle());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        } catch (FileIOError e) {
            b.setError(e.message);
            b.setErrorCode(e.code);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to write to file" + request.getFileHandle(), e);
            b.setError("unable to write to file" + request.getFileHandle());
            b.setErrorCode(errorCodes.EIO);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }

    }

    @Override
    public void release(FileCloseRequest request, StreamObserver<FileCloseResponse> responseObserver) {
        FileCloseResponse.Builder b = FileCloseResponse.newBuilder();
        try {
            DedupFileChannel ch = this.dedupChannels.remove(request.getFileHandle());

            if (!Main.safeClose)
                return;
            if (ch != null) {
                SDFSLogger.getLog().info("release=" + ch.getFile().getPath());
                ch.getDedupFile().unRegisterChannel(ch, -2);
                CloseFile.close(ch.getFile(), ch.isWrittenTo());
                ch = null;
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } else {
                SDFSLogger.getLog().info("channel not found");
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        } catch (Exception e) {
            b.setError("unable to write to file" + request.getFileHandle());
            b.setErrorCode(errorCodes.EBADFD);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
    }

    @Override
    public void mknod(MkNodRequest request, StreamObserver<MkNodResponse> responseObserver) {
        MkNodResponse.Builder b = MkNodResponse.newBuilder();
        try {
            String path = request.getPath();
            File f = new File(this.mountedVolume + path);

            if (Main.volume.isOffLine()) {
                b.setError("Volume Offline");
                b.setErrorCode(errorCodes.ENODEV);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            if (Main.volume.isFull()) {
                b.setError("Volume Full");
                b.setErrorCode(errorCodes.ENOSPC);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            if (f.exists()) {
                // SDFSLogger.getLog().info("42=");
                f = null;
                b.setError("File exists " + path);
                b.setErrorCode(errorCodes.EEXIST);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } else {
                SDFSLogger.getLog().debug("creating file " + f.getPath());
                MetaDataDedupFile mf = MetaFileStore.getMF(f);
                mf.unmarshal();
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            }
        } catch (Exception e) {
            SDFSLogger.getLog().error("error making " + request.getPath(), e);
            b.setError("error making " + request.getPath());
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
    }

    @Override
    public void open(FileOpenRequest request, StreamObserver<FileOpenResponse> responseObserver) {
        FileOpenResponse.Builder b = FileOpenResponse.newBuilder();
        if (Main.volume.isOffLine()) {
            b.setError("Volume Offline");
            b.setErrorCode(errorCodes.ENODEV);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        try {
            String path = request.getPath();
            long z = this.nextHandleNo.incrementAndGet();
            this.getFileChannel(path, z);
            // SDFSLogger.getLog().info("555=" + path + " z=" + z);
            b.setFileHandle(z);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch (FileIOError e) {
            b.setError(e.message);
            b.setErrorCode(e.code);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to open to file" + request.getPath(), e);
            b.setError("unable to open to file" + request.getPath());
            b.setErrorCode(errorCodes.EIO);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
    }

    @Override
    public void read(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {
        DataReadResponse.Builder b = DataReadResponse.newBuilder();
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
            buf.position(0);
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

    @Override
    public void getFileInfo(FileInfoRequest req, StreamObserver<FileMessageResponse> responseObserver) {
        if (req.getFileName().equals("lastClosedFile")) {
            FileMessageResponse.Builder b = FileMessageResponse.newBuilder();
            try {
                MetaDataDedupFile mf = CloseFile.lastClosedFile;

                b.addResponse(mf.toGRPC(true));
            } catch (Exception e) {
                SDFSLogger.getLog().debug("unable to fulfill request on file " + req.getFileName(), e);
                b.setError("unable to fulfill request on file " + req.getFileName());
                b.setErrorCode(errorCodes.EIO);
            }
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } else {
            FileMessageResponse.Builder b = FileMessageResponse.newBuilder();

            String internalPath = Main.volume.getPath() + File.separator + req.getFileName();
            File f = new File(internalPath);
            if (!f.exists()) {
                b.setError("File not found " + req.getFileName());
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            if (f.isDirectory()) {

                try {
                    String guid = UUID.randomUUID().toString();
                    if (req.getListGuid() != null) {
                        guid = req.getListGuid();
                    }
                    b.setListGuid(guid);
                    Path dir = FileSystems.getDefault().getPath(f.getPath());
                    DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
                    this.fileListers.put(guid, stream);
                    int maxFiles = 1000;
                    int cf = 0;
                    if (req.getNumberOfFiles() > 0) {
                        maxFiles = req.getNumberOfFiles();
                    }
                    b.setMaxNumberOfFiles(maxFiles);
                    for (Path p : stream) {
                        File _mf = p.toFile();
                        MetaDataDedupFile mf = MetaFileStore.getNCMF(_mf);
                        try {
                            b.addResponse(mf.toGRPC(req.getCompact()));
                        } catch (Exception e) {
                            SDFSLogger.getLog().error("unable to load file " + _mf.getPath(), e);
                        }
                        cf++;
                        if (cf == maxFiles) {
                            b.setListGuid(guid);
                            responseObserver.onNext(b.build());
                            responseObserver.onCompleted();
                            return;
                        }
                    }
                    this.fileListers.invalidate(guid);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } catch (Exception e) {
                    SDFSLogger.getLog().error("unable to fulfill request on directory " + req.getFileName(), e);
                    b.setError("unable to fulfill request on directory " + req.getFileName());
                    b.setErrorCode(errorCodes.EIO);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
            } else {

                try {
                    MetaDataDedupFile mf = MetaFileStore.getNCMF(new File(internalPath));
                    b.addResponse(mf.toGRPC(req.getCompact()));
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } catch (Exception e) {
                    SDFSLogger.getLog().error("unable to fulfill request on file " + req.getFileName(), e);
                    b.setError("unable to fulfill request on file " + req.getFileName());
                    b.setErrorCode(errorCodes.EIO);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
            }
        }

    }

    @Override
    public void createCopy(FileSnapshotRequest req, StreamObserver<FileSnapshotResponse> responseObserver) {
        FileSnapshotResponse.Builder b = FileSnapshotResponse.newBuilder();
        File f = new File(Main.volume.getPath() + File.separator + req.getSrc());
        File nf = new File(Main.volume.getPath() + File.separator + req.getDest());
        try {
            if (!f.exists()) {
                b.setError("Path not found [" + req.getSrc() + "]");
                b.setErrorCode(errorCodes.EEXIST);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            if (nf.exists()) {
                b.setError("Path already exists [" + req.getDest() + "]");
                b.setErrorCode(errorCodes.EEXIST);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            SDFSEvent evt = SDFSEvent.snapEvent("Snapshot Intiated for " + req.getSrc() + " to " + req.getDest(), f);
            MetaFileStore.snapshot(f.getPath(), nf.getPath(), false, evt);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch (Exception e) {
            b.setError("Error creating snapshot from " + req.getSrc() + " to " + req.getDest());
            b.setErrorCode(errorCodes.EIO);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
    }

    public void fileExists(FileExistsRequest req,StreamObserver<FileExistsResponse> responseObserver) {
        FileExistsResponse.Builder b = FileExistsResponse.newBuilder();
        try {
            File f = new File(Main.volume.getPath() + File.separator + req.getPath());
            b.setExists(f.exists());
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch(Exception e) {
            b.setError("FileExists error " + req.getPath());
            b.setErrorCode(errorCodes.EIO);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
    }

    private static class FileIOError extends Exception {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        errorCodes code;
        String message;

        public FileIOError(String message, errorCodes code) {
            super(message);
            this.message = message;
            this.code = code;

        }

        public String getMessage() {
            return this.message;
        }

    }

}