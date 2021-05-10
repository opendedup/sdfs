package org.opendedup.sdfs.mgmt.grpc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.SDFSCli.AddUserRequest;
import org.opendedup.grpc.SDFSCli.AddUserResponse;
import org.opendedup.grpc.SDFSCli.DeleteUserRequest;
import org.opendedup.grpc.SDFSCli.DeleteUserResponse;
import org.opendedup.grpc.SDFSCli.ListUsersRequest;
import org.opendedup.grpc.SDFSCli.ListUsersResponse;
import org.opendedup.grpc.SDFSCli.SdfsUser;
import org.opendedup.grpc.SDFSCli.SdfsUsers;
import org.opendedup.grpc.SDFSCli.SetPermissionsRequest;
import org.opendedup.grpc.SDFSCli.SetPermissionsResponse;
import org.opendedup.grpc.SDFSCli.SetUserPasswordRequest;
import org.opendedup.grpc.SDFSCli.SetUserPasswordResponse;
import org.opendedup.grpc.SdfsUserServiceGrpc.SdfsUserServiceImplBase;
import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.OSValidator;

import io.grpc.stub.StreamObserver;

public class SdfsUserServiceImpl extends SdfsUserServiceImplBase {

    static private SdfsUsers users = null;

    static {
        try {
            if (Main.permissionsFile == null) {
                Main.permissionsFile = OSValidator.getProgramBasePath() + "volumes" + File.separator
                        + Main.volume.getName() + File.separator + "permssions" + File.separator
                        + "volume_permissions.pb";
            }
            File f = new File(Main.permissionsFile);
            f.getParentFile().mkdirs();
            if (f.exists()) {
                users = SdfsUsers.parseFrom(new FileInputStream(f));
            } else {
                users = SdfsUsers.newBuilder().build();
                users.writeTo(new FileOutputStream(f));
            }
        } catch (Exception e) {
            SDFSLogger.getLog().error("unable to load user db", e);
            System.exit(2);
        }
    }

    public SdfsUserServiceImpl() throws FileNotFoundException, IOException {

    }

    @Override
    public void addUser(AddUserRequest request, StreamObserver<AddUserResponse> responseObserver) {
        AddUserResponse.Builder b = AddUserResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.AUTH_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            synchronized (users) {
                if (request.getUser().equalsIgnoreCase("admin")) {
                    b.setError("User admin already exists and cannot be modified this way");
                    b.setErrorCode(errorCodes.EINVAL);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                } else if (users.containsUsers(request.getUser())) {
                    b.setError("User already exists :" + request.getUser());
                    b.setErrorCode(errorCodes.EEXIST);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                } else if (!request.getPassword().matches(Main.PasswordPattern)) {
                    b.setError("User password complexity failed. User password must pass the following checks: "
                            + "a digit must occur at least once, a lower case letter must occur at least once "
                            + ", an upper case letter must occur at least once, a special character must occur at least once"
                            + ", no whitespace allowed in the entire string , at least 8 characters");
                    b.setErrorCode(errorCodes.EINVAL);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                } else if (request.getUser().length() == 0) {
                    b.setError("user name must be set");
                    b.setErrorCode(errorCodes.EINVAL);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();

                } else {
                    try {
                        String salt = HashFunctions.getRandomString(24);

                        String hashedPassword = HashFunctions.getSHAHash(request.getPassword().getBytes(),
                                salt.getBytes());
                        SdfsUser.Builder ub = SdfsUser.newBuilder();
                        ub.setDescription(request.getDescription());
                        ub.setPasswordHash(hashedPassword);
                        ub.setSalt(salt);
                        ub.setUser(request.getUser());
                        ub.setPermissions(request.getPermissions());
                        SdfsUser u = ub.build();
                        paddUser(u);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();

                    } catch (Exception e) {
                        SDFSLogger.getLog().error("unable to persist user " + request.getUser(), e);
                        b.setError("unable to persist user " + request.getUser());
                        b.setErrorCode(errorCodes.EIO);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    }

                }
            }

        }

    }

    @Override
    public void deleteUser(DeleteUserRequest request, StreamObserver<DeleteUserResponse> responseObserver) {
        DeleteUserResponse.Builder b = DeleteUserResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.AUTH_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            synchronized (users) {
                if (request.getUser().equalsIgnoreCase("admin")) {
                    b.setError("User admin already exists and cannot be modified this way");
                    b.setErrorCode(errorCodes.EINVAL);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                } else if (request.getUser().length() == 0) {
                    b.setError("user name must be set");
                    b.setErrorCode(errorCodes.EINVAL);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();

                } else {
                    try {
                        premoveUser(request.getUser());
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    } catch (Exception e) {
                        SDFSLogger.getLog().error("unable to remove user " + request.getUser(), e);
                        b.setError("unable to remove user " + request.getUser());
                        b.setErrorCode(errorCodes.EIO);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    }

                }
            }
        }

    }

    @Override
    public void listUsers(ListUsersRequest request, StreamObserver<ListUsersResponse> responseObserver) {
        ListUsersResponse.Builder b = ListUsersResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.AUTH_READ)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            synchronized (users) {
                b.addAllUsers(users.getUsersMap().values());
                responseObserver.onNext(b.build());
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public void setSdfsPassword(SetUserPasswordRequest request,
            StreamObserver<SetUserPasswordResponse> responseObserver) {
        SetUserPasswordResponse.Builder b = SetUserPasswordResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.AUTH_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            synchronized (users) {
                if (request.getUser().equalsIgnoreCase("admin")) {
                    b.setError("User admin cannot be modified this way");
                    b.setErrorCode(errorCodes.EINVAL);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                } else if (!users.containsUsers(request.getUser())) {
                    b.setError("User does not exists :" + request.getUser());
                    b.setErrorCode(errorCodes.EEXIST);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                } else if (!request.getPassword().matches(Main.PasswordPattern)) {
                    b.setError("User password complexity failed. User password must pass the following checks: "
                            + "a digit must occur at least once, a lower case letter must occur at least once "
                            + ", an upper case letter must occur at least once, a special character must occur at least once"
                            + ", no whitespace allowed in the entire string , at least 8 characters");
                    b.setErrorCode(errorCodes.EINVAL);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                } else {
                    try {
                        SdfsUser.Builder ub = SdfsUser.newBuilder();
                        ub.mergeFrom(users.getUsersOrThrow(request.getUser()));
                        String salt = HashFunctions.getRandomString(24);

                        String hashedPassword = HashFunctions.getSHAHash(request.getPassword().getBytes(),
                                salt.getBytes());

                        ub.setPasswordHash(hashedPassword);
                        paddUser(ub.build());
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    } catch (Exception e) {
                        SDFSLogger.getLog().error("unable to persist user " + request.getUser(), e);
                        b.setError("unable to persist user " + request.getUser());
                        b.setErrorCode(errorCodes.EIO);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    }

                }
            }
        }
    }

    @Override
    public void setSdfsPermissions(SetPermissionsRequest request,
            StreamObserver<SetPermissionsResponse> responseObserver) {
        SetPermissionsResponse.Builder b = SetPermissionsResponse.newBuilder();
        if (!AuthUtils.validateUser(AuthUtils.ACTIONS.AUTH_WRITE)) {
            b.setError("User is not a member of any group with access");
            b.setErrorCode(errorCodes.EACCES);
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } else {
            synchronized (users) {
                if (request.getUser().equalsIgnoreCase("admin")) {
                    b.setError("User admin cannot be modified this way");
                    b.setErrorCode(errorCodes.EINVAL);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                } else if (!users.containsUsers(request.getUser())) {
                    b.setError("User does not exists :" + request.getUser());
                    b.setErrorCode(errorCodes.EEXIST);
                    responseObserver.onNext(b.build());
                    responseObserver.onCompleted();
                } else {
                    try {
                        SdfsUser.Builder ub = SdfsUser.newBuilder();
                        ub.mergeFrom(users.getUsersOrThrow(request.getUser()));

                        ub.setPermissions(request.getPermissions());
                        paddUser(ub.build());
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    } catch (Exception e) {
                        SDFSLogger.getLog().error("unable to persist user " + request.getUser(), e);
                        b.setError("unable to persist user " + request.getUser());
                        b.setErrorCode(errorCodes.EIO);
                        responseObserver.onNext(b.build());
                        responseObserver.onCompleted();
                    }
                }
            }
        }
    }

    public static void setLastLogin(String userName) throws IOException {
        synchronized (users) {
            if (users.containsUsers(userName)) {
                SdfsUser.Builder ub = SdfsUser.newBuilder();
                ub.mergeFrom(users.getUsersOrThrow(userName));

                ub.setLastLogin(System.currentTimeMillis());
                paddUser(ub.build());
            }
        }
    }

    public static void setLastFailedLogin(String userName) throws IOException {
        synchronized (users) {
            if (users.containsUsers(userName)) {
                SdfsUser.Builder ub = SdfsUser.newBuilder();
                ub.mergeFrom(users.getUsersOrThrow(userName));

                ub.setLastFailedLogin(System.currentTimeMillis());
                paddUser(ub.build());
            }
        }
    }

    private static void paddUser(SdfsUser user) throws IOException {
        SdfsUsers.Builder b = SdfsUsers.newBuilder();
        b.mergeFrom(users);
        b.putUsers(user.getUser(), user);
        SdfsUsers _users = b.build();

        serializeUsers(_users);
        users = _users;
    }

    private static void premoveUser(String userName) throws IOException {
        SdfsUsers.Builder b = SdfsUsers.newBuilder();
        b.mergeFrom(users);
        b.removeUsers(userName);
        SdfsUsers _users = b.build();

        serializeUsers(_users);
        users = _users;
    }

    private static void serializeUsers(SdfsUsers u) throws IOException {
        try {
            File f = new File(Main.permissionsFile);
            u.writeTo(new FileOutputStream(f));
        } catch (Error e) {
            SDFSLogger.getLog().error("unable to serialize users", e);
            throw new IOException(e);
        }
    }

    public static SdfsUser getUser(String userName) {
        synchronized (users) {
            if (!users.containsUsers(userName)) {
                return null;
            }
            return users.getUsersOrThrow(userName);
        }
    }

}
