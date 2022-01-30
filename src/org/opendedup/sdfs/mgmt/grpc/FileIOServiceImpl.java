package org.opendedup.sdfs.mgmt.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.SparseDataChunk;
import org.opendedup.hashing.AbstractHashEngine;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.sdfs.io.events.MFileWritten;
import org.opendedup.sdfs.mgmt.CloseFile;
import org.opendedup.sdfs.mgmt.GetCloudFile;
import org.opendedup.sdfs.mgmt.MgmtWebServer;
import org.opendedup.sdfs.mgmt.GetCloudMetaFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.OSValidator;

import fuse.FuseFtypeConstants;

import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.RestoreArchive;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.protobuf.ByteString;

import org.opendedup.sdfs.io.FileClosedException;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.opendedup.grpc.FileIOServiceGrpc;
import org.opendedup.grpc.FileInfo.FileAttributes;
import org.opendedup.grpc.FileInfo.FileInfoRequest;
import org.opendedup.grpc.FileInfo.FileMessageResponse;
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.FileInfo.syncaction;
import org.opendedup.grpc.IOService.ChmodRequest;
import org.opendedup.grpc.IOService.ChmodResponse;
import org.opendedup.grpc.IOService.ChownRequest;
import org.opendedup.grpc.IOService.ChownResponse;
import org.opendedup.grpc.IOService.CopyExtentRequest;
import org.opendedup.grpc.IOService.CopyExtentResponse;
import org.opendedup.grpc.IOService.DataReadRequest;
import org.opendedup.grpc.IOService.DataReadResponse;
import org.opendedup.grpc.IOService.DataWriteRequest;
import org.opendedup.grpc.IOService.DataWriteResponse;
import org.opendedup.grpc.IOService.FileCloseRequest;
import org.opendedup.grpc.IOService.FileCloseResponse;
import org.opendedup.grpc.IOService.FileExistsRequest;
import org.opendedup.grpc.IOService.FileExistsResponse;
import org.opendedup.grpc.IOService.FileOpenRequest;
import org.opendedup.grpc.IOService.FileOpenResponse;
import org.opendedup.grpc.IOService.FileRenameRequest;
import org.opendedup.grpc.IOService.FileRenameResponse;
import org.opendedup.grpc.IOService.FileSnapshotRequest;
import org.opendedup.grpc.IOService.FileSnapshotResponse;
import org.opendedup.grpc.IOService.FlushRequest;
import org.opendedup.grpc.IOService.FlushResponse;
import org.opendedup.grpc.IOService.FsyncRequest;
import org.opendedup.grpc.IOService.FsyncResponse;
import org.opendedup.grpc.IOService.GetCloudFileRequest;
import org.opendedup.grpc.IOService.GetCloudFileResponse;
import org.opendedup.grpc.IOService.GetXAttrRequest;
import org.opendedup.grpc.IOService.GetXAttrResponse;
import org.opendedup.grpc.IOService.GetXAttrSizeRequest;
import org.opendedup.grpc.IOService.GetXAttrSizeResponse;
import org.opendedup.grpc.IOService.LinkRequest;
import org.opendedup.grpc.IOService.LinkResponse;
import org.opendedup.grpc.IOService.MkDirRequest;
import org.opendedup.grpc.IOService.MkDirResponse;
import org.opendedup.grpc.IOService.MkNodRequest;
import org.opendedup.grpc.IOService.MkNodResponse;
import org.opendedup.grpc.IOService.RemoveXAttrRequest;
import org.opendedup.grpc.IOService.RemoveXAttrResponse;
import org.opendedup.grpc.IOService.RmDirRequest;
import org.opendedup.grpc.IOService.RmDirResponse;
import org.opendedup.grpc.IOService.SetUserMetaDataRequest;
import org.opendedup.grpc.IOService.SetUserMetaDataResponse;
import org.opendedup.grpc.IOService.SetXAttrRequest;
import org.opendedup.grpc.IOService.SetXAttrResponse;
import org.opendedup.grpc.IOService.Stat;
import org.opendedup.grpc.IOService.StatFS;
import org.opendedup.grpc.IOService.StatFSRequest;
import org.opendedup.grpc.IOService.StatFSResponse;
import org.opendedup.grpc.IOService.StatRequest;
import org.opendedup.grpc.IOService.StatResponse;
import org.opendedup.grpc.IOService.SymLinkRequest;
import org.opendedup.grpc.IOService.SymLinkResponse;
import org.opendedup.grpc.IOService.SyncNotificationSubscription;
import org.opendedup.grpc.IOService.TruncateRequest;
import org.opendedup.grpc.IOService.TruncateResponse;
import org.opendedup.grpc.IOService.UnlinkRequest;
import org.opendedup.grpc.IOService.UnlinkResponse;
import org.opendedup.grpc.IOService.UtimeRequest;
import org.opendedup.grpc.IOService.UtimeResponse;
import org.opendedup.grpc.IOService.SetRetrievalTierRequest;
import org.opendedup.grpc.IOService.SetRetrievalTierResponse;
import org.opendedup.grpc.IOService.GetRetrievalTierRequest;
import org.opendedup.grpc.IOService.GetRetrievalTierResponse;

public class FileIOServiceImpl extends FileIOServiceGrpc.FileIOServiceImplBase {
    AtomicLong nextHandleNo = new AtomicLong(1000);
    private static final int BLOCK_SIZE = 32768;
    private static final int NAME_LENGTH = 2048;

    private LoadingCache<String, Iterator<Path>> fileListers = CacheBuilder.newBuilder().maximumSize(100)
            .expireAfterAccess(5, TimeUnit.MINUTES).build(new CacheLoader<String, Iterator<Path>>() {

                @Override
                public Iterator<Path> load(String key) throws Exception {
                    throw new IOException("Key Not Found [" + key + "]");
                }

            });
    static ConcurrentHashMap<Long, DedupFileChannel> dedupChannels = new ConcurrentHashMap<Long, DedupFileChannel>();
    public static String mountedVolume;
    public static String connicalMountedVolume;
    public static String mountPoint;
    public static AbstractHashEngine eng = HashFunctionPool.getHashEngine();

    private static EventBus eventBus = new EventBus();

    public static void registerListener(Object obj) {
        eventBus.register(obj);
    }

    public FileIOServiceImpl() throws IOException {
        FileIOServiceImpl.mountedVolume = Main.volume.getPath();
        if (!FileIOServiceImpl.mountedVolume.endsWith("/"))
            FileIOServiceImpl.mountedVolume = FileIOServiceImpl.mountedVolume + "/";
        FileIOServiceImpl.connicalMountedVolume = new File(FileIOServiceImpl.mountedVolume).getCanonicalPath();
        FileIOServiceImpl.mountPoint = Main.volumeMountPoint;
        if (!mountPoint.endsWith("/"))
            mountPoint = mountPoint + "/";

        File f = new File(FileIOServiceImpl.mountedVolume);
        if (!f.exists())
            f.mkdirs();
    }

    private static void checkInFS(File f) throws FileIOError {
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

    protected static File resolvePath(String path) throws FileIOError {
        String pt = mountedVolume + path.trim();
        File _f = new File(pt);

        if (!OSValidator.isWindows() && Files.isSymbolicLink(_f.toPath())) {
            try {
                _f = Files.readSymbolicLink(_f.toPath()).toFile();
                pt = mountedVolume + _f.getPath();
                _f = new File(pt);
            } catch (IOException e) {
                SDFSLogger.getLog().debug("error resolving " + mountedVolume + path, e);
                throw new FileIOError("error resolving [" + path + "]", errorCodes.ENOENT);
            }
        }
        try {
            checkInFS(_f);
        } catch (FileIOError e) {
            SDFSLogger.getLog().warn("unable", e);
            throw e;
        }
        if (!_f.exists()) {
            if (SDFSLogger.isDebug())
                SDFSLogger.getLog().debug("No such node");

            _f = null;
            throw new FileIOError("path does not exist [" + path + "]", errorCodes.ENOENT);
        }
        return _f;
    }

    private DedupFileChannel getFileChannel(String path, long handleNo, boolean register) throws FileIOError {
        synchronized (dedupChannels) {
            DedupFileChannel ch = dedupChannels.get(handleNo);
            if (!register) {
                ch = null;
            }
            if (ch == null) {
                File f = FileIOServiceImpl.resolvePath(path);
                try {
                    MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
                    ch = mf.getDedupFile(false).getChannel(-2);
                    if (!register) {
                        return ch;
                    } else if (dedupChannels.containsKey(handleNo)) {
                        ch.getDedupFile().unRegisterChannel(ch, -2);
                        ch = dedupChannels.get(handleNo);
                    } else {
                        dedupChannels.put(handleNo, ch);
                    }
                    // SDFSLogger.getLog().info("Getting attributes for " + f.getPath());

                } catch (Exception e) {
                    SDFSLogger.getLog().error("error processing get file channel for [" + path + "] and "
                            + "file channel [" + handleNo + "]", e);
                    throw new FileIOError("error processing get file channel for [" + path + "] and " + "file channel ["
                            + handleNo + "]", errorCodes.EIO);

                }
            }
            return ch;
        }
    }

    private DedupFileChannel getFileChannel(long handleNo) throws FileIOError {
        DedupFileChannel ch = dedupChannels.get(handleNo);
        if (ch == null) {
            SDFSLogger.getLog().debug("unable to read file " + handleNo);
            throw new FileIOError("unable to read file " + handleNo, errorCodes.EBADFD);
        }
        return ch;
    }

    @Override
    public void mkDir(MkDirRequest request, StreamObserver<MkDirResponse> responseObserver) {
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            MkDirResponse.Builder b = MkDirResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            MkDirResponse.Builder b = MkDirResponse.newBuilder();
            File f = new File(FileIOServiceImpl.mountedVolume + request.getPath());
            try {
                FileIOServiceImpl.checkInFS(f);
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
                MetaFileStore.mkDir(f, request.getMode());
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
    }

    @Override
    public void mkDirAll(MkDirRequest request, StreamObserver<MkDirResponse> responseObserver) {
        MkDirResponse.Builder b = MkDirResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {

            File f = new File(FileIOServiceImpl.mountedVolume + request.getPath());
            try {
                FileIOServiceImpl.checkInFS(f);
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
    }

    @Override
    public void getCloudFile(GetCloudFileRequest request, StreamObserver<GetCloudFileResponse> responseObserver) {
        GetCloudFileResponse.Builder b = GetCloudFileResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                GetCloudFile cf = new GetCloudFile();
                cf.getResult(request.getFile(), request.getDstfile(), request.getOverwrite(), request.getChangeid());
                b.setEventID(cf.fevt.uid);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (NullPointerException e) {
                SDFSLogger.getLog().error("unable to fulfill request", e);
                b.setError("file not found " + request.getFile());
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to fulfill request", e);
                b.setError("unable to fulfill request");
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void getCloudMetaFile(GetCloudFileRequest request, StreamObserver<GetCloudFileResponse> responseObserver) {
        GetCloudFileResponse.Builder b = GetCloudFileResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                GetCloudMetaFile cf = new GetCloudMetaFile();
                cf.getResult(request.getFile(), request.getDstfile(), request.getChangeid());
                b.setEventID(cf.fevt.uid);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to fulfill request", e);
                b.setError("unable to fulfill request");
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
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
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_DELETE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {

                if (this.getFtype(request.getPath()) == FuseFtypeConstants.TYPE_SYMLINK) {

                    File f = new File(mountedVolume + request.getPath());

                    // SDFSLogger.getLog().info("deleting symlink " + f.getCanonicalPath());
                    if (!f.delete()) {

                        b.setError("unable to delete " + f.getPath());
                        b.setErrorCode(errorCodes.EACCES);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                        f = null;
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
    }

    @Override
    public void setUserMetaData(SetUserMetaDataRequest req, StreamObserver<SetUserMetaDataResponse> responseObserver) {
        SetUserMetaDataResponse.Builder b = SetUserMetaDataResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String internalPath = Main.volume.getPath() + File.separator + req.getPath();
            File f = new File(internalPath);
            if (!f.exists()) {
                b.setError("File not found " + req.getPath());
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            try {
                MetaDataDedupFile mf = MetaFileStore.getMF(new File(internalPath));
                List<FileAttributes> attrs = req.getFileAttributesList();
                for (FileAttributes attr : attrs) {
                    mf.addXAttribute(attr.getKey(), attr.getValue());
                }
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to fulfill request on file " + req.getPath(), e);
                b.setError("unable to fulfill request on file " + req.getPath());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    @Override
    public StreamObserver<DataWriteRequest> streamWrite(StreamObserver<DataWriteResponse> responseObserver) {
        return new StreamObserver<DataWriteRequest>() {

            AtomicLong written = new AtomicLong();

            @Override
            public void onNext(DataWriteRequest request) {
                if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
                    SDFSLogger.getLog().error("User is not a member of any group with access");
                    this.onError(new Exception("User is not a member of any group with access"));
                } else {
                    if (Main.volume.isOffLine()) {
                        SDFSLogger.getLog().error("Volume is Offline");
                        this.onError(new Exception("Volume is Offline"));
                    }
                    if (Main.volume.isFull()) {
                        SDFSLogger.getLog().error("Volume Full");
                        this.onError(new Exception("Volume Full"));
                    }
                    try {
                        DedupFileChannel ch = getFileChannel(request.getFileHandle());
                        ByteBuffer buf = request.getData().asReadOnlyByteBuffer();
                        if (request.getCompressed()) {
                            byte[] b = new byte[buf.capacity()];
                            buf.get(b);
                            byte[] chunk = CompressionUtils.decompressLz4(b, request.getLen());
                            buf = ByteBuffer.wrap(chunk);
                        }
                        buf.position(0);
                        ch.writeFile(buf, buf.capacity(), 0, request.getStart(), true);
                        written.addAndGet(buf.capacity());
                    } catch (Exception e) {
                        SDFSLogger.getLog().error("Error while stream writing", e);
                        this.onError(e);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                SDFSLogger.getLog().error("error while writing", t);

            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(DataWriteResponse.newBuilder().setWritten(written.get())
                        .build());
                responseObserver.onCompleted();

            }

        };
    }

    @Override
    public void unlink(UnlinkRequest request, StreamObserver<UnlinkResponse> responseObserver) {
        UnlinkResponse.Builder b = UnlinkResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_DELETE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                String path = request.getPath();
                if (SDFSLogger.isDebug())
                    SDFSLogger.getLog().debug("removing " + path);
                if (!Main.safeClose) {
                    try {
                        this.getFileChannel(path, -1, true).getDedupFile().forceClose();
                    } catch (IOException e) {
                        SDFSLogger.getLog().error("unable to close file " + path, e);
                    }
                }
                if (this.getFtype(path) == FuseFtypeConstants.TYPE_DIR) {
                    b.setError("is a directory " + path);
                    b.setErrorCode(errorCodes.EISDIR);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } else if (this.getFtype(path) == FuseFtypeConstants.TYPE_SYMLINK) {
                    Path p = new File(mountedVolume + path).toPath();
                    // SDFSLogger.getLog().info("deleting symlink " + f.getPath());
                    try {
                        MetaDataDedupFile mf = MetaFileStore.getMF(FileIOServiceImpl.resolvePath(path));
                        SDFSLogger.getLog().info("Unlink::chattr set non-Immutable file: " + path);
                        ImmuteLinuxFDFileFile(path, false);
                        eventBus.post(new MFileDeleted(mf));
                        Files.delete(p);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                        return;
                    } catch (IOException e) {
                        SDFSLogger.getLog().warn("unable to delete symlink " + p);
                        b.setError("unable to delete symlink " + p);
                        b.setErrorCode(errorCodes.ENOSYS);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                        return;
                    }
                } else {
                    File f = FileIOServiceImpl.resolvePath(path);
                    try {
                        SDFSLogger.getLog().info("Unlink::chattr set non-Immutable file: " + f.getPath());
                        ImmuteLinuxFDFileFile(f.getPath(), false);
                        MetaFileStore.getMF(f).clearRetentionLock();
                        if (MetaFileStore.removeMetaFile(f.getPath(), false, false, true)) {
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
    }

    @Override
    public void write(DataWriteRequest request, StreamObserver<DataWriteResponse> responseObserver) {
        DataWriteResponse.Builder b = DataWriteResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
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
                ByteBuffer buf = request.getData().asReadOnlyByteBuffer();
                if (request.getCompressed()) {
                    byte[] bf = new byte[buf.capacity()];
                    buf.get(bf);
                    byte[] chunk = CompressionUtils.decompressLz4(bf, request.getLen());
                    buf = ByteBuffer.wrap(chunk);
                }
                buf.position(0);
                try {
                    SDFSLogger.getLog().debug("Writing " + ch.openFile().getPath() + " pos=" + request.getStart()
                            + " len=" + buf.capacity());
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
    }

    @Override
    public void release(FileCloseRequest request, StreamObserver<FileCloseResponse> responseObserver) {
        FileCloseResponse.Builder b = FileCloseResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)
                || !AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                DedupFileChannel ch = null;
                synchronized (dedupChannels) {
                    ch = dedupChannels.remove(request.getFileHandle());
                }
                if (!Main.safeClose)
                    return;
                if (ch != null) {
                    ImmuteLinuxFDFileFile(ch.getPath(), false);
                    ch.getDedupFile().unRegisterChannel(ch, -2);
                    CloseFile.close(ch.getFile(), ch.isWrittenTo());
                    ImmuteLinuxFDFileFile(ch.getPath(), true);
                    ch = null;
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } else {
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
    }

    public void ImmuteLinuxFDFileFile(String filePath, Boolean isImmutable) {
        if (OSValidator.isUnix()) {
            String strCommand = "";
            if (filePath != null && !filePath.isEmpty()) {

                if (isImmutable) {
                    SDFSLogger.getLog().info("chattr set Immutable, file: " + filePath);
                    SDFSLogger.getLog().info("Command::sudo chattr +i -V " + filePath);
                    strCommand = "sudo chattr +i -V " + filePath;
                } else {
                    SDFSLogger.getLog().info("chattr set non-Immutable, file: " + filePath);
                    SDFSLogger.getLog().info("COmmand::sudo chattr -i -V " + filePath);
                    strCommand = "sudo chattr -i -V " + filePath;
                }
                MgmtWebServer.executeLinuxCmd(strCommand);
            }
        }
    }

    @Override
    public void mknod(MkNodRequest request, StreamObserver<MkNodResponse> responseObserver) {
        MkNodResponse.Builder b = MkNodResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            try {
                String path = request.getPath();
                File f = new File(FileIOServiceImpl.mountedVolume + path);

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
                    try {
                        mf.setMode(request.getMode());
                    } finally {
                        f = null;
                    }
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
    }

    @Override
    public void open(FileOpenRequest request, StreamObserver<FileOpenResponse> responseObserver) {
        FileOpenResponse.Builder b = FileOpenResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)
                || !AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
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
                this.getFileChannel(path, z, true);
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
    }

    @Override
    public void read(DataReadRequest request, StreamObserver<DataReadResponse> responseObserver) {
        DataReadResponse.Builder b = DataReadResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            if (Main.volume.isOffLine()) {
                b.setError("Volume Offline");
                b.setErrorCode(errorCodes.ENODEV);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            try {
                ByteBuffer buf = ByteBuffer.wrap(new byte[request.getLen()]);
                DedupFileChannel ch = this.getFileChannel((Long) request.getFileHandle());
                int read = ch.read(buf, 0, buf.capacity(), request.getStart());
                if (read == -1)
                    read = 0;

                b.setRead(read);
                if (request.getCompress() && read > 1) {
                    byte[] chunk = CompressionUtils.compressLz4(buf.array());
                    if (read < chunk.length) {
                        buf = ByteBuffer.wrap(chunk);
                    }
                }
                buf.position(0);
                b.setData(ByteString.copyFrom(buf, read));
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                HCServiceProxy.set_move_blob(false);
                return;
            } catch (FileIOError e) {
                b.setError(e.message);
                b.setErrorCode(e.code);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (DataArchivedException e) {
                SDFSLogger.getLog().info("Readin 4");
                SDFSLogger.getLog().warn("Data is archived");
                b.setError("Data is archived");
                b.setErrorCode(errorCodes.ENODATA);
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to read file " + request.getFileHandle(), e);
                b.setError("unable to read file " + request.getFileHandle());
                b.setErrorCode(errorCodes.ENODATA);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }

    }

    @Override
    public void stat(FileInfoRequest req, StreamObserver<FileMessageResponse> responseObserver) {
        FileMessageResponse.Builder b = FileMessageResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String internalPath = Main.volume.getPath() + File.separator + req.getFileName();
            File f = new File(internalPath);
            SDFSLogger.getLog().debug("looking for " + f.getPath());
            if (!f.exists()) {
                SDFSLogger.getLog().info("File not found " + req.getFileName());
                b.setError("File not found " + req.getFileName());
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            try {
                SDFSLogger.getLog().debug("found " + f.getPath());
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

    @Override
    public void chmod(ChmodRequest req, StreamObserver<ChmodResponse> responseObserver) {
        ChmodResponse.Builder b = ChmodResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String internalPath = Main.volume.getPath() + File.separator + req.getPath();
            File f = new File(internalPath);
            String path = req.getPath();
            int mode = req.getMode();
            if (!f.exists()) {
                SDFSLogger.getLog().info("File not found " + req.getPath());
                b.setError("File not found " + req.getPath());
                b.setErrorCode(errorCodes.ENOENT);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } else {
                try {
                    int ftype = this.getFtype(path);
                    if (ftype == FuseFtypeConstants.TYPE_SYMLINK || ftype == FuseFtypeConstants.TYPE_DIR) {
                        Path p = Paths.get(f.getCanonicalPath());

                        try {
                            if (!OSValidator.isWindows())
                                Files.setAttribute(p, "unix:mode", Integer.valueOf(mode), LinkOption.NOFOLLOW_LINKS);
                            responseObserver.onNext(b.build());
                            responseObserver.onCompleted();
                        } catch (IOException e) {
                            SDFSLogger.getLog().warn("access denied for " + path, e);
                            b.setError("access denied for " + path);
                            b.setErrorCode(errorCodes.EACCES);
                            responseObserver.onNext(b.build());
                            responseObserver.onCompleted();
                        } finally {
                            path = null;
                        }
                    } else {

                        try {
                            MetaDataDedupFile mf = MetaFileStore.getMF(f);
                            mf.setMode(mode);
                            mf.unmarshal();
                            responseObserver.onNext(b.build());
                            responseObserver.onCompleted();
                        } catch (Exception e) {
                            SDFSLogger.getLog().warn("access denied for " + path, e);
                            b.setError("access denied for " + path);
                            b.setErrorCode(errorCodes.EACCES);
                            responseObserver.onNext(b.build());
                            responseObserver.onCompleted();
                        } finally {
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
                    SDFSLogger.getLog().error("unable to chmod " + req.getPath(), e);
                    b.setError("unable to chmod " + req.getPath());
                    b.setErrorCode(errorCodes.EIO);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }

            }
        }

    }

    @Override
    public void chown(ChownRequest req, StreamObserver<ChownResponse> responseObserver) {
        ChownResponse.Builder b = ChownResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = req.getPath();
            int uid = req.getUid();
            int gid = req.getGid();
            try {
                String pt = mountedVolume + path.trim();
                File f = new File(pt);
                if (!Files.isSymbolicLink(f.toPath())) {
                    f = resolvePath(path);
                }
                int ftype = this.getFtype(path);
                if (ftype == FuseFtypeConstants.TYPE_SYMLINK || ftype == FuseFtypeConstants.TYPE_DIR) {
                    Path p = Paths.get(f.getCanonicalPath());
                    try {
                        SDFSLogger.getLog().info("setting uid " + uid + "and gid " + gid);
                        if (!OSValidator.isWindows()) {
                            Files.setAttribute(p, "unix:uid", Integer.valueOf(uid), LinkOption.NOFOLLOW_LINKS);
                            Files.setAttribute(p, "unix:gid", Integer.valueOf(gid), LinkOption.NOFOLLOW_LINKS);
                        }
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    } catch (IOException e) {
                        SDFSLogger.getLog().warn("access denied for " + path, e);
                        b.setError("access denied for " + path);
                        b.setErrorCode(errorCodes.EACCES);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    } finally {
                        path = null;
                    }
                } else {

                    try {
                        MetaDataDedupFile mf = MetaFileStore.getMF(f);
                        mf.setOwner_id(uid);
                        mf.setGroup_id(gid);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    } catch (Exception e) {
                        SDFSLogger.getLog().warn("access denied for " + path, e);
                        b.setError("access denied for " + path);
                        b.setErrorCode(errorCodes.EACCES);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    } finally {

                    }
                }
            } catch (FileIOError e) {
                SDFSLogger.getLog().error(path, e);
                b.setError(e.message);
                b.setErrorCode(e.code);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error(path, e);
                b.setError("unable to chown " + req.getPath());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        }

    }

    @Override
    public void flush(FlushRequest req, StreamObserver<FlushResponse> responseObserver) {
        FlushResponse.Builder b = FlushResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = req.getPath();
            long fh = req.getFd();
            try {
                if (Main.volume.isOffLine()) {
                    b.setError("volume offline");
                    b.setErrorCode(errorCodes.ENAVAIL);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                DedupFileChannel ch = this.getFileChannel(path, (Long) fh, true);
                ch.force(true);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to sync file [" + path + "]", e);
                b.setError("unable to sync file [" + path + "]");
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    @Override
    public void fsync(FsyncRequest request, StreamObserver<FsyncResponse> responseObserver) {
        FsyncResponse.Builder b = FsyncResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = request.getPath();
            long fh = request.getFh();
            if (Main.volume.isOffLine()) {
                b.setError("volume offline");
                b.setErrorCode(errorCodes.ENAVAIL);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            try {
                if (Main.safeSync) {
                    DedupFileChannel ch = this.getFileChannel(path, (Long) fh, true);
                    ch.force(true);
                }
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to fsync file [" + path + "]", e);
                b.setError("unable to fsync file [" + path + "]");
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    @Override
    public void getAttr(StatRequest request, StreamObserver<StatResponse> responseObserver) {
        StatResponse.Builder b = StatResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            Stat.Builder sb = Stat.newBuilder();
            String path = request.getPath();
            try {
                int ftype = this.getFtype(path);
                // SDFSLogger.getLog().info("1 " + path + " " + ftype);
                if (ftype == FuseFtypeConstants.TYPE_SYMLINK) {
                    // SDFSLogger.getLog().info("poop " + path);
                    Path p = null;
                    BasicFileAttributes attrs = null;
                    try {
                        p = Paths.get(FileIOServiceImpl.mountedVolume + path);
                        int uid = 0;
                        int gid = 0;
                        int mode = 0000;
                        try {
                            if (!OSValidator.isWindows()) {
                                uid = (Integer) Files.getAttribute(p, "unix:uid", LinkOption.NOFOLLOW_LINKS);
                                gid = (Integer) Files.getAttribute(p, "unix:gid", LinkOption.NOFOLLOW_LINKS);
                                mode = (Integer) Files.getAttribute(p, "unix:mode", LinkOption.NOFOLLOW_LINKS);
                            }
                        } catch (Exception e) {
                            SDFSLogger.getLog().error("unable to parse sylink " + path, e);
                        }
                        sb.setUid(uid);
                        sb.setGid(gid);
                        sb.setMode(mode);

                        long fileLength = 0;
                        attrs = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                        fileLength = attrs.size();
                        sb.setUid(uid);
                        sb.setGid(gid);
                        sb.setMode(mode);
                        sb.setAtime(attrs.lastAccessTime().toMillis());
                        sb.setMtim(attrs.lastModifiedTime().toMillis());
                        sb.setCtim(attrs.creationTime().toMillis());
                        sb.setBlksize(BLOCK_SIZE);
                        sb.setDev(p.hashCode());
                        sb.setBlocks((fileLength * NAME_LENGTH + BLOCK_SIZE - 1) / BLOCK_SIZE);
                        sb.setIno(p.hashCode());
                        sb.setNlink(0);
                        sb.setSize(fileLength);
                        sb.setRdev(1);
                        b.setStat(sb.build());
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                        return;

                    } catch (Exception e) {
                        SDFSLogger.getLog().error("unable to parse symlink " + path, e);
                        b.setError("unable to parse symlink [" + path + "]");
                        b.setErrorCode(errorCodes.EACCES);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                        return;
                    }
                } else {
                    File f = resolvePath(path);
                    Path p = null;
                    try {

                        p = Paths.get(f.getCanonicalPath());
                        if (ftype == FuseFtypeConstants.TYPE_DIR) {
                            int uid = 0;
                            int gid = 0;
                            int mode = 0;
                            if (!OSValidator.isWindows()) {
                                uid = (Integer) Files.getAttribute(p, "unix:uid");
                                gid = (Integer) Files.getAttribute(p, "unix:gid");
                                mode = (Integer) Files.getAttribute(p, "unix:mode");
                            }
                            MetaDataDedupFile mf = MetaFileStore.getFolder(f);

                            long fileLength = f.length();
                            if (!OSValidator.isWindows()) {
                                sb.setUid(uid);
                                sb.setGid(gid);
                                sb.setMode(mode);
                            }
                            sb.setAtime(mf.getLastAccessed());
                            sb.setMtim(mf.lastModified());
                            sb.setCtim(0);
                            sb.setBlksize(BLOCK_SIZE);
                            sb.setDev(mf.getHashCode());
                            sb.setBlocks((fileLength * NAME_LENGTH + BLOCK_SIZE - 1) / BLOCK_SIZE);
                            sb.setIno(mf.getHashCode());
                            sb.setNlink(0);
                            sb.setSize(fileLength);
                            sb.setRdev(1);
                            b.setStat(sb.build());
                            responseObserver.onNext(b.build());
                            responseObserver.onCompleted();
                            return;

                        } else {
                            MetaDataDedupFile mf = MetaFileStore.getMF(f);
                            int uid = mf.getOwner_id();
                            int gid = mf.getGroup_id();
                            int mode = mf.getMode();

                            long fileLength = mf.length();
                            long actualBytes = (mf.getIOMonitor().getActualBytesWritten() * 2) / 1024;
                            if (actualBytes == 0 && mf.getIOMonitor().getActualBytesWritten() > 0)
                                actualBytes = (Main.CHUNK_LENGTH * 2) / 1024;
                            sb.setUid(uid);
                            sb.setGid(gid);
                            sb.setMode(mode);
                            sb.setAtime(mf.getLastAccessed());
                            sb.setMtim(mf.lastModified());
                            sb.setCtim(0);
                            sb.setBlksize(BLOCK_SIZE);
                            sb.setDev(mf.getHashCode());
                            sb.setBlocks(actualBytes);
                            sb.setIno(mf.getHashCode());
                            sb.setNlink(0);
                            sb.setSize(fileLength);
                            sb.setRdev(1);
                            b.setStat(sb.build());
                            responseObserver.onNext(b.build());
                            responseObserver.onCompleted();
                            return;

                        }
                    } catch (Exception e) {
                        SDFSLogger.getLog().error(
                                "unable to parse attributes " + path + " at physical path " + f.getCanonicalPath(), e);
                        b.setError("unable to parse attributes " + path);
                        b.setErrorCode(errorCodes.EACCES);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                        return;
                    } finally {
                        f = null;
                        p = null;
                    }
                }
            } catch (FileIOError e) {
                b.setError(e.message);
                b.setErrorCode(e.code);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                b.setError("unable to parse attributes " + path);
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }

    }

    @Override
    public void readLink(LinkRequest request, StreamObserver<LinkResponse> responseObserver) {
        LinkResponse.Builder b = LinkResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = request.getPath();
            try {
                Path p = Paths.get(FileIOServiceImpl.mountedVolume + path);
                String lpath = Files.readSymbolicLink(p).toString();
                if (new File(lpath).getPath().startsWith(FileIOServiceImpl.mountedVolume))
                    lpath = FileIOServiceImpl.mountPoint + lpath.substring(FileIOServiceImpl.mountedVolume.length());
                b.setLinkPath(lpath);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                b.setError("unable to parse attributes " + path);
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    @Override
    public void symLink(SymLinkRequest request, StreamObserver<SymLinkResponse> responseObserver) {
        SymLinkResponse.Builder b = SymLinkResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String from = request.getFrom();
            String to = request.getTo();
            try {
                File src = null;
                File fr = new File(mountedVolume + from);
                if (fr.getCanonicalPath().startsWith(FileIOServiceImpl.mountPoint)) {
                    from = from.substring(mountPoint.length());
                    FileIOServiceImpl.resolvePath(from);
                    src = new File(mountedVolume + from);
                } else if (!Main.allowExternalSymlinks) {
                    b.setError("external symlinks are not allowed " + from + " to " + to);
                    b.setErrorCode(errorCodes.EACCES);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } else {
                    src = new File(from);
                }
                File dst = new File(mountedVolume + to);
                try {
                    FileIOServiceImpl.checkInFS(dst);
                } catch (FileIOError e) {
                    b.setError(e.message);
                    b.setErrorCode(e.code);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                if (dst.exists()) {
                    b.setError("file exists " + dst.getPath());
                    b.setErrorCode(errorCodes.EEXIST);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                Path srcP = Paths.get(src.getCanonicalPath());
                Path dstP = Paths.get(dst.getCanonicalPath());
                // SDFSLogger.getLog().info(
                // "symlink " + src.getCanonicalPath() + " to " + dst.getCanonicalPath());
                try {
                    Files.createSymbolicLink(dstP, srcP);
                    eventBus.post(new MFileWritten(MetaFileStore.getMF(dst.getCanonicalPath()), true));
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } catch (IOException e) {
                    b.setError("error linking " + from + " to " + to);
                    b.setErrorCode(errorCodes.EACCES);
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
                SDFSLogger.getLog().error(from, e);
                b.setError("error linking " + from + " to " + to);
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    @Override
    public void truncate(TruncateRequest request, StreamObserver<TruncateResponse> responseObserver) {
        TruncateResponse.Builder b = TruncateResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = request.getPath();
            long size = request.getLength();
            try {
                DedupFileChannel ch = this.getFileChannel(path, -1, false);
                ch.truncateFile(size);

                ch.getDedupFile().unRegisterChannel(ch, -1);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (FileIOError e) {
                SDFSLogger.getLog().error(path, e);
                b.setError(e.message);
                b.setErrorCode(e.code);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error(path, e);
                b.setError("error truncating " + path);
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    @Override
    public void utime(UtimeRequest request, StreamObserver<UtimeResponse> responseObserver) {
        UtimeResponse.Builder b = UtimeResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = request.getPath();
            long atime = request.getAtime();
            long mtime = request.getMtime();
            try {
                File f = FileIOServiceImpl.resolvePath(path);
                if (f.isFile()) {
                    MetaDataDedupFile mf = MetaFileStore.getMF(f);
                    mf.setLastAccessed(atime);
                    mf.setLastModified(mtime);
                } else {
                    Path p = f.toPath();
                    Files.setLastModifiedTime(p, FileTime.fromMillis(mtime));
                    MetaDataDedupFile mf = MetaFileStore.getMF(f);
                    if (mf.isFile())
                        mf.setDirty(true);
                    try {
                        eventBus.post(new MFileWritten(mf, true));
                    } catch (Exception e) {
                        SDFSLogger.getLog().error("unable to post mfilewritten " + path, e);
                    }
                }
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
                SDFSLogger.getLog().error(path, e);
                b.setError("error in utime " + path);
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }

    }

    @Override
    public void getXAttr(GetXAttrRequest request, StreamObserver<GetXAttrResponse> responseObserver) {
        GetXAttrResponse.Builder b = GetXAttrResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = request.getPath();
            String name = request.getAttr();
            try {
                FileIOServiceImpl.resolvePath(path);
                int ftype = this.getFtype(path);
                if (ftype != FuseFtypeConstants.TYPE_SYMLINK) {

                    File f = FileIOServiceImpl.resolvePath(path);
                    MetaDataDedupFile mf = MetaFileStore.getMF(f);
                    String st = mf.getXAttribute(name);
                    if (st != null)
                        b.setValue(st);
                    else {
                        b.setError("not entity " + name + " in " + path);
                        b.setErrorCode(errorCodes.ENODATA);

                    }
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();

                } else {
                    b.setError("cannot get extended attributes for symlink " + path);
                    b.setErrorCode(errorCodes.ENOENT);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                }
            } catch (FileIOError e) {
                b.setError(e.message);
                b.setErrorCode(e.code);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error(path, e);
                b.setError("error in utime " + path);
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }

    }

    @Override
    public void getXAttrSize(GetXAttrSizeRequest request, StreamObserver<GetXAttrSizeResponse> responseObserver) {
        GetXAttrSizeResponse.Builder b = GetXAttrSizeResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = request.getPath();
            String name = request.getAttr();
            try {
                FileIOServiceImpl.resolvePath(path);
                int ftype = this.getFtype(path);
                if (ftype != FuseFtypeConstants.TYPE_SYMLINK) {

                    File f = FileIOServiceImpl.resolvePath(path);
                    MetaDataDedupFile mf = MetaFileStore.getMF(f);
                    String st = mf.getXAttribute(name);
                    if (st != null)
                        b.setLength(st.getBytes().length);
                    else {
                        b.setError("not entity " + name + " in " + path);
                        b.setErrorCode(errorCodes.ENODATA);

                    }
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();

                } else {
                    b.setError("cannot get extended attributes for symlink " + path);
                    b.setErrorCode(errorCodes.ENOENT);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                }
            } catch (FileIOError e) {
                b.setError(e.message);
                b.setErrorCode(e.code);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error(path, e);
                b.setError("error in utime " + path);
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    @Override
    public void setXAttr(SetXAttrRequest request, StreamObserver<SetXAttrResponse> responseObserver) {
        SetXAttrResponse.Builder b = SetXAttrResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = request.getPath();
            String name = request.getAttr();
            String value = request.getValue();
            try {
                File f = FileIOServiceImpl.resolvePath(path);
                MetaDataDedupFile mf = MetaFileStore.getMF(f);
                mf.addXAttribute(name, value);
                if (mf.isFile())
                    mf.setDirty(true);
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
                SDFSLogger.getLog().error(path, e);
                b.setError("error in setxattr " + path);
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    @Override
    public void removeXAttr(RemoveXAttrRequest request, StreamObserver<RemoveXAttrResponse> responseObserver) {
        RemoveXAttrResponse.Builder b = RemoveXAttrResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String path = request.getPath();
            String name = request.getAttr();
            try {
                File f = FileIOServiceImpl.resolvePath(path);
                if (!f.exists()) {
                    b.setError("cannot get extended attributes for " + path);
                    b.setErrorCode(errorCodes.ENOENT);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                MetaDataDedupFile mf = MetaFileStore.getMF(f);
                mf.removeXAttribute(name);
                if (mf.isFile())
                    mf.setDirty(true);
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
                SDFSLogger.getLog().error(path, e);
                b.setError("error in setxattr " + path);
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    @Override
    public void getFileInfo(FileInfoRequest req, StreamObserver<FileMessageResponse> responseObserver) {
        FileMessageResponse.Builder b = FileMessageResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            if (req.getFileName().equals("lastClosedFile")) {

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
                        String guid = null;
                        Path dir = FileSystems.getDefault().getPath(f.getPath());
                        Iterator<Path> stream = null;
                        if (req.getListGuid().isEmpty() || req.getListGuid().trim().length() == 0) {
                            guid = UUID.randomUUID().toString();
                            stream = Files.newDirectoryStream(dir).iterator();
                            this.fileListers.put(guid, stream);
                        } else {
                            guid = req.getListGuid();
                            stream = this.fileListers.get(guid);
                        }
                        int maxFiles = 1000;
                        int cf = 0;
                        if (req.getNumberOfFiles() > 0) {
                            maxFiles = req.getNumberOfFiles();
                        }
                        b.setMaxNumberOfFiles(maxFiles);
                        while (stream.hasNext()) {
                            Path p = stream.next();
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

    }

    @Override
    public void rename(FileRenameRequest req, StreamObserver<FileRenameResponse> responseObserver) {
        FileRenameResponse.Builder b = FileRenameResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
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
                try {
                    FileIOServiceImpl.checkInFS(nf);
                } catch (FileIOError e) {
                    b.setError(e.getMessage());
                    b.setErrorCode(e.code);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
                MetaFileStore.rename(f.getCanonicalPath(), nf.getCanonicalPath());
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error("unable to rename " + req.getSrc() + " to " + req.getDest(), e);
                b.setError("unable to rename " + req.getSrc() + " to " + req.getDest());
                b.setErrorCode(errorCodes.EACCES);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            } finally {
                f = null;
            }
        }
    }

    private static LoadingCache<String, DedupFileChannel> writeChannels = CacheBuilder.newBuilder()
            .maximumSize(Main.maxOpenFiles * 2).concurrencyLevel(64).expireAfterAccess(120, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<String, DedupFileChannel>() {
                public void onRemoval(RemovalNotification<String, DedupFileChannel> removal) {
                    DedupFileChannel ck = removal.getValue();
                    ck.getDedupFile().unRegisterChannel(ck, -1);
                    // flushingBuffers.put(pos, ck);
                }
            }).build(new CacheLoader<String, DedupFileChannel>() {
                public DedupFileChannel load(String f) throws IOException, FileClosedException {
                    SparseDedupFile sdf = (SparseDedupFile) MetaFileStore.getMF(f).getDedupFile(true);
                    return sdf.getChannel(-1);
                }

            });

    @Override
    public void copyExtent(CopyExtentRequest req, StreamObserver<CopyExtentResponse> responseObserver) {
        CopyExtentResponse.Builder b = CopyExtentResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            String srcfile = req.getSrcFile();
            String dstfile = req.getDstFile();

            long sstart = req.getSrcStart();
            long dstart = req.getDstStart();
            long len = req.getLength();
            File f = new File(Main.volume.getPath() + File.separator + srcfile);
            File nf = new File(Main.volume.getPath() + File.separator + dstfile);
            if (!f.exists()) {
                b.setErrorCode(errorCodes.ENODEV).setError("Path not found [" + srcfile + "]");
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;

            }
            if (!nf.exists()) {
                b.setErrorCode(errorCodes.ENODEV).setError("Path not found [" + dstfile + "]");
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            MetaDataDedupFile smf = MetaFileStore.getMF(f);
            MetaDataDedupFile dmf = MetaFileStore.getMF(nf);
            if (smf.length() < len)
                len = smf.length();
            SparseDedupFile sdf = null;
            SparseDedupFile ddf = null;
            try {
                sdf = (SparseDedupFile) smf.getDedupFile(true);
                ddf = (SparseDedupFile) dmf.getDedupFile(true);
                sdf.sync(true);
                ddf.sync(true);
            } catch (Exception e) {
                SDFSLogger.getLog().error("error while setting dedupe for files", e);
                b.setErrorCode(errorCodes.EIO).setError("error while setting dedupe for files");
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
            ddf.setReconstructed(true);
            long _spos = -1;
            long _dpos = -1;
            try {
                long written = 0;
                _spos = this.getChuckPosition(sstart);
                _dpos = this.getChuckPosition(dstart);
                Lock l = ddf.getWriteLock();
                l.lock();
                writeChannels.get(f.getPath());
                writeChannels.get(nf.getPath());
                try {
                    while (written < len) {
                        long _sstart = written + sstart;
                        long _dstart = written + dstart;
                        _spos = this.getChuckPosition(_sstart);
                        _dpos = this.getChuckPosition(_dstart);
                        long _rem = len - written;
                        int _so = (int) (_sstart - _spos);
                        int _do = (int) (_dstart - _dpos);
                        boolean insdone = false;
                        DedupFileChannel ch = null;
                        int tries = 0;
                        while (!insdone) {
                            try {
                                ch = sdf.getChannel(-1);
                                SparseDataChunk sdc = sdf.getSparseDataChunk(_spos);
                                /*
                                 * if(sdc.getFingers().size() == 0) { int _nlen = 4 *1024; if(_nlen > _rem) {
                                 * _nlen = (int)_rem; } dc.writeFile(ByteBuffer.allocate(_nlen), _nlen, 0,
                                 * _dpos, true); //ddf.mf.getIOMonitor().addVirtualBytesWritten(p.nlen, true);
                                 * //ddf.mf.getIOMonitor().addDulicateData(p.nlen, true);
                                 * //ddf.mf.setLastModified(System.currentTimeMillis()); written += _nlen;
                                 * if(written >= len) insdone = true; } else {
                                 */
                                WritableCacheBuffer ddc = (WritableCacheBuffer) ddf.getWriteBuffer(_dpos);
                                ddc.writeAccelBuffer();
                                HashLocPair p = sdc.getWL(_so);

                                if (p.nlen > _rem) {
                                    p.nlen = (int) _rem;
                                }
                                p.pos = _do;
                                int ep = p.pos + p.nlen;
                                if (ep > Main.CHUNK_LENGTH) {
                                    p.nlen = Main.CHUNK_LENGTH - p.pos;
                                }
                                try {
                                    ddc.copyExtent(p);
                                } catch (DataArchivedException e) {
                                    if (Main.checkArchiveOnRead) {
                                        SDFSLogger.getLog().warn("Archived data found in " + sdf.getMetaFile().getPath()
                                                + " at " + _spos
                                                + ". Recovering data from archive. This may take up to 4 hours");
                                        RestoreArchive.recoverArchives(smf);
                                        this.copyExtent(req, responseObserver);
                                    } else
                                        throw e;
                                }
                                dmf.getIOMonitor().addVirtualBytesWritten(p.nlen, true);
                                dmf.getIOMonitor().addDulicateData(p.nlen, true);
                                dmf.setLastModified(System.currentTimeMillis());
                                written += p.nlen;
                                insdone = true;

                                // }
                            } catch (org.opendedup.sdfs.io.FileClosedException e) {
                                if (tries > 100) {
                                    SDFSLogger.getLog().warn("tried to open file 100 ties and failed " + smf.getPath());
                                    b.setErrorCode(errorCodes.EIO)
                                            .setError("tried to open file 100 ties and failed " + smf.getPath());
                                    responseObserver.onNext(b.build());
                                    responseObserver.onCompleted();
                                    return;
                                }
                                insdone = false;
                            } catch (Exception e) {
                                throw e;
                            } finally {
                                if (ch != null) {
                                    try {
                                        sdf.unRegisterChannel(ch, -1);
                                    } catch (Exception e) {

                                    }
                                }
                            }
                        }

                    }
                } finally {
                    ddf.sync(true);
                    long el = written + dstart;
                    if (el > dmf.length()) {
                        dmf.setLength(el, false);
                    }
                    l.unlock();
                }
                b.setWritten(written);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                SDFSLogger.getLog().error("error in copy extent src=" + srcfile + " dst=" + dstfile + " sstart="
                        + sstart + " dstart=" + dstart + " len=" + len + " spos" + _spos + " dpos=" + _dpos, e);
                b.setErrorCode(errorCodes.EIO)
                        .setError("error in copy extent src=" + srcfile + " dst=" + dstfile + " sstart=" + sstart
                                + " dstart=" + dstart + " len=" + len + " spos" + _spos + " dpos=" + _dpos);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }

    }

    private long getChuckPosition(long location) {
        long place = location / Main.CHUNK_LENGTH;
        place = place * Main.CHUNK_LENGTH;
        return place;
    }

    @Override
    public void createCopy(FileSnapshotRequest req, StreamObserver<FileSnapshotResponse> responseObserver) {
        FileSnapshotResponse.Builder b = FileSnapshotResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {

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
                SDFSEvent evt = SDFSEvent.snapEvent("Snapshot Intiated for " + req.getSrc() + " to " + req.getDest(),
                        f);
                MetaFileStore.snapshot(f.getPath(), nf.getPath(), false, evt);
                b.setEventID(evt.uid);
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
    }

    public void fileExists(FileExistsRequest req, StreamObserver<FileExistsResponse> responseObserver) {
        FileExistsResponse.Builder b = FileExistsResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {

            try {
                File f = new File(Main.volume.getPath() + File.separator + req.getPath());
                b.setExists(f.exists());
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            } catch (Exception e) {
                b.setError("FileExists error " + req.getPath());
                b.setErrorCode(errorCodes.EIO);
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
                return;
            }
        }
    }

    public void setRetrievalTier(SetRetrievalTierRequest req,
            StreamObserver<SetRetrievalTierResponse> responseObserver) {
        SetRetrievalTierResponse.Builder b = SetRetrievalTierResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            synchronized (this) {
                try {
                    String tierType = req.getTierType();
                    if (!tierType.isEmpty()) {
                        SDFSLogger.getLog().info("Tier type: " + tierType);
                        if (tierType.equalsIgnoreCase("expedited") || tierType.equalsIgnoreCase("standard")
                                || tierType.equalsIgnoreCase("bulk") || tierType.equalsIgnoreCase("high")) {
                            Main.retrievalTier = tierType;
                            SDFSLogger.getLog().info("Retrieval tier is set to: " + Main.retrievalTier);
                        } else {
                            b.setError("SetRetrievalTier error, not a proper retrieval tier: " + req.getTierType());
                            b.setErrorCode(errorCodes.EIO);
                        }
                    } else {
                        b.setError("SetRetrievalTier error, retrieval tier is empty " + req.getTierType());
                        b.setErrorCode(errorCodes.EIO);
                    }
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } catch (Exception e) {
                    b.setError("SetRetrievalTier error " + req.getTierType());
                    b.setErrorCode(errorCodes.EIO);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
            }
        }
    }

    public void getRetrievalTier(GetRetrievalTierRequest req,
            StreamObserver<GetRetrievalTierResponse> responseObserver) {
        GetRetrievalTierResponse.Builder b = GetRetrievalTierResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.FILE_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            synchronized (this) {
                try {
                    String tierType = Main.retrievalTier;
                    if (tierType.equals("")) {
                        b.setError("GetRetrievalTier error, retrieval tier is not set explicitly");
                        b.setErrorCode(errorCodes.EIO);
                    } else {
                        b.setTierType("Tier type: " + tierType);
                    }
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                } catch (Exception e) {
                    b.setTierType("");
                    b.setError("GetRetrievalTier error");
                    b.setErrorCode(errorCodes.EIO);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                    return;
                }
            }
        }
    }

    protected static class FileIOError extends Exception {
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

    @Override
    public void statFS(StatFSRequest request, StreamObserver<StatFSResponse> responseObserver) {
        StatFSResponse.Builder b = StatFSResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }
        StatFS.Builder bs = StatFS.newBuilder();

        try {
            long blocks = Main.volume.getTotalBlocks();
            long used = Main.volume.getUsedBlocks();
            if (used > blocks)
                used = blocks;
            bs.setBsize(Main.volume.getBlockSize()).setBlocks(blocks).setBfree(blocks - used).setNamelen(NAME_LENGTH)
                    .setType(0).setFsid(Main.volume.getSerialNumber());
            b.setStat(bs);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to stat", e);
            b.setError("unable to stat");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        } finally {

        }
    }

    @Override
    public void fileNotification(SyncNotificationSubscription request,
            StreamObserver<FileMessageResponse> responseObserver) {
        final ServerCallStreamObserver<FileMessageResponse> callStreamObserver = (ServerCallStreamObserver<FileMessageResponse>) responseObserver;
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.METADATA_READ)) {
            FileMessageResponse.Builder b = FileMessageResponse.newBuilder();
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
            return;
        }

        SDFSLogger.getLog().info("Received Request " + request.getUid());
        SyncFileListener dListener = new SyncFileListener(callStreamObserver);
        while (!dListener.closed && !callStreamObserver.isCancelled()) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }

        }
        dListener.close();

    }

    public class SyncFileListener {

        StreamObserver<FileMessageResponse> responseObserver;
        boolean closed = false;

        public SyncFileListener(StreamObserver<FileMessageResponse> responseObserver) {
            this.responseObserver = responseObserver;
            FileReplicationService.registerEvents(this);
            MetaDataDedupFile.registerListener(this);
            MetaFileStore.registerListener(this);
        }

        @Subscribe
        public void syncEvent(org.opendedup.sdfs.io.events.MFileDownloaded _evt) {

            synchronized (responseObserver) {
                FileMessageResponse.Builder b = FileMessageResponse.newBuilder();
                try {
                    b.setAction(syncaction.DOWNLOAD);
                    b.addResponse(_evt.mf.toGRPC(true));

                } catch (Exception e) {
                    SDFSLogger.getLog().error("error creating file message", e);
                    b.setError("Unable to create file message");
                    b.setErrorCode(errorCodes.EIO);
                }
                try {
                    responseObserver.onNext(b.build());

                    SDFSLogger.getLog().info("Sent message");
                } catch (Exception e) {
                    SDFSLogger.getLog().error("Unable to send message", e);
                    this.close();
                }
            }

        }

        @Subscribe
        public void syncEvent(org.opendedup.sdfs.io.events.MFileWritten _evt) {
            synchronized (responseObserver) {
                FileMessageResponse.Builder b = FileMessageResponse.newBuilder();
                try {
                    b.setAction(syncaction.WRITE);
                    b.addResponse(_evt.mf.toGRPC(true));

                } catch (Exception e) {
                    SDFSLogger.getLog().error("error creating file message", e);
                    b.setError("Unable to create file message");
                    b.setErrorCode(errorCodes.EIO);
                }
                try {
                    responseObserver.onNext(b.build());

                    SDFSLogger.getLog().info("Sent message");
                } catch (Exception e) {
                    SDFSLogger.getLog().error("Unable to send message", e);
                    this.close();
                }
            }
        }

        @Subscribe
        public void syncEvent(org.opendedup.sdfs.io.events.MFileDeleted _evt) {
            synchronized (responseObserver) {
                FileMessageResponse.Builder b = FileMessageResponse.newBuilder();
                try {
                    b.setAction(syncaction.DELETE);
                    b.addResponse(_evt.mf.toGRPC(true));

                } catch (Exception e) {
                    SDFSLogger.getLog().error("error creating file message", e);
                    b.setError("Unable to create file message");
                    b.setErrorCode(errorCodes.EIO);
                }
                try {
                    responseObserver.onNext(b.build());

                    SDFSLogger.getLog().info("Sent message");
                } catch (Exception e) {
                    SDFSLogger.getLog().error("Unable to send message", e);
                    this.close();
                }
            }
        }

        @Subscribe
        public void syncEvent(org.opendedup.sdfs.io.events.MFileUploaded _evt) {
            synchronized (responseObserver) {
                FileMessageResponse.Builder b = FileMessageResponse.newBuilder();
                try {
                    b.setAction(syncaction.UPLOAD);
                    b.addResponse(_evt.mf.toGRPC(true));

                } catch (Exception e) {
                    SDFSLogger.getLog().error("error creating file message", e);
                    b.setError("Unable to create file message");
                    b.setErrorCode(errorCodes.EIO);
                }
                try {
                    responseObserver.onNext(b.build());

                    SDFSLogger.getLog().info("Sent message");
                } catch (Exception e) {
                    SDFSLogger.getLog().error("Unable to send message", e);
                    this.close();
                }
            }
        }

        public void close() {
            synchronized (responseObserver) {
                if (!this.closed) {
                    FileReplicationService.unregisterEvents(this);
                    MetaDataDedupFile.unregisterListener(this);
                    MetaFileStore.unregisterListener(this);
                    responseObserver.onCompleted();
                    this.closed = true;
                    SDFSLogger.getLog().info("Closed Listener");
                }
            }
        }

    }
}