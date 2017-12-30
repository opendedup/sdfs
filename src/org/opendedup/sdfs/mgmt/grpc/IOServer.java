package org.opendedup.sdfs.mgmt.grpc;

import io.grpc.Server;

import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import fuse.FuseException;

import org.opendedup.io.ReadBuffer.ReadRequest;
import org.opendedup.io.ReadBuffer.ReadResponse;
import org.opendedup.io.CloseFileGrpc;
import org.opendedup.io.OpenFileGrpc;
import org.opendedup.io.FileHandle.CloseRequest;
import org.opendedup.io.FileHandle.CloseResponse;
import org.opendedup.io.FileHandle.OpenRequest;
import org.opendedup.io.FileHandle.OpenResponse;
import org.opendedup.io.ReadFileGrpc;
import org.opendedup.io.WriteBuffer.WriteRequest;
import org.opendedup.io.WriteBuffer.WriteResponse;
import org.opendedup.io.WriteFileGrpc;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.mgmt.MgmtWebServer;

public class IOServer {
	private static final Logger logger = Logger.getLogger(IOServer.class.getName());

	private Server server;

	public void start() throws IOException {
		/* The port on which the server should run */
		int port = 50051;
		server = ServerBuilder.forPort(port).addService(new ReaderImpl()).build().start();
		logger.info("Server started, listening on " + port);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				// Use stderr here since the logger may have been reset by its JVM shutdown
				// hook.
				System.err.println("*** shutting down gRPC server since JVM is shutting down");
				IOServer.this.stop();
				System.err.println("*** server shut down");
			}
		});
	}

	private void stop() {
		if (server != null) {
			server.shutdown();
		}
	}

	/**
	 * Await termination on the main thread since the grpc library uses daemon
	 * threads.
	 */
	private void blockUntilShutdown() throws InterruptedException {
		if (server != null) {
			server.awaitTermination();
		}
	}

	/**
	 * Main launches the server from the command line.
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		final IOServer server = new IOServer();
		server.start();
		server.blockUntilShutdown();
	}

	static class ReaderImpl extends ReadFileGrpc.ReadFileImplBase {

		@Override
		public void read(ReadRequest req, StreamObserver<ReadResponse> responseObserver) {
			
			ReadResponse reply = null;
			try {
				ByteBuffer bf = ByteBuffer.wrap(new byte[req.getLength()]);
				int read = MgmtWebServer.io.read(req.getFileHandle(), bf, req.getStart());
				reply = ReadResponse.newBuilder().setRead(read).setReadBytes(ByteString.copyFrom(bf.array())).build();

			} catch (FuseException e) {
				reply = ReadResponse.newBuilder().setRead(e.getErrno() * -1).build();
			} catch (Exception e) {
				SDFSLogger.getLog().error("error reading", e);
				reply = ReadResponse.newBuilder().setRead(-1).build();
			}
			responseObserver.onNext(reply);
			responseObserver.onCompleted();

		}
	}

	static class OpenFileImpl extends OpenFileGrpc.OpenFileImplBase {
		@Override
		public void open(OpenRequest req, StreamObserver<OpenResponse> responseObserver) {
			OpenResponse resp = null;
			try {
				String pth = req.getFile();
				resp = OpenResponse.newBuilder().setFileHandle(MgmtWebServer.io.open(pth)).build();
			}catch(FuseException e) {
				resp = OpenResponse.newBuilder().setFileHandle(e.getErrno() * -1).build();
			}catch(Exception e) {
				SDFSLogger.getLog().error("error opend", e);
				resp = OpenResponse.newBuilder().setFileHandle(-1).build();
			}
			responseObserver.onNext(resp);
			responseObserver.onCompleted();
			
		}
	}
	
	static class CloseFileImpl extends CloseFileGrpc.CloseFileImplBase {
		@Override
		public void close(CloseRequest req, StreamObserver<CloseResponse> responseObserver) {
			CloseResponse resp = null;
			try {
				MgmtWebServer.io.release(req.getFileHandle());
				resp = CloseResponse.newBuilder().setFileHandle(req.getFileHandle()).build();
			} catch(FuseException e) {
				resp = CloseResponse.newBuilder().setFileHandle(e.getErrno() *-1).build();
			} catch(Exception e) {
				SDFSLogger.getLog().error("error opend", e);
				resp = CloseResponse.newBuilder().setFileHandle(-1).build();
			}
			responseObserver.onNext(resp);
			responseObserver.onCompleted();
		}
	}

	static class WriterImpl extends WriteFileGrpc.WriteFileImplBase {

		@Override
		public void write(WriteRequest req, StreamObserver<WriteResponse> responseObserver) {
			
			WriteResponse reply = null;
			try {
				ByteBuffer bf = ByteBuffer.wrap(req.toByteArray());
				MgmtWebServer.io.write(req.getFileHandle(), bf, req.getStart());
				reply = WriteResponse.newBuilder().setWritten(bf.capacity()).build();
			} catch (FuseException e) {
				reply = WriteResponse.newBuilder().setWritten(e.getErrno() * -1).build();
			} catch (Exception e) {
				SDFSLogger.getLog().error("error writing", e);
				reply = WriteResponse.newBuilder().setWritten(-1).build();
			}
			responseObserver.onNext(reply);
			responseObserver.onCompleted();

		}
	}

}
