package org.opendedup.sdfs.mgmt.grpc;

import org.opendedup.sdfs.Main;

public class AuthUtils {

    public static enum ACTIONS {
        METADATA_READ, METADATA_WRITE, FILE_READ, FILE_WRITE, FILE_DELETE, VOLUME_READ, CONFIG_READ, CONFIG_WRITE,
        EVENT_READ, AUTH_READ, AUTH_WRITE,ENCRYPTION_READ,ENCRYPTION_WRITE
    }

    public static boolean validateUser(ACTIONS action) {
        if (!Main.sdfsCliRequireAuth) {
            return true;
        }
        JWebToken user = IOServer.AuthorizationInterceptor.USER_IDENTITY.get();
        return user.hasPermission(action);

    }

}
