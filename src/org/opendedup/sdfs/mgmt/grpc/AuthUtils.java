package org.opendedup.sdfs.mgmt.grpc;

import org.opendedup.sdfs.Main;

public class AuthUtils {
    public static enum ACTIONS {
        METADATA_READ,
        METADATA_WRITE,
        FILE_READ,
        FILE_WRITE,
        FILE_DELETE,
        VOLUME_READ,
        CONFIG_READ,
        CONFIG_WRITE,
        EVENT_READ,
    }

    public static boolean validateUser(ACTIONS action) {
        if(!Main.sdfsCliRequireAuth) {
            return true;
        }
        JWebToken user = IOServer.AuthorizationInterceptor.USER_IDENTITY.get();
        if(user.hasGroup("admin")) {
            return true;
        }
        switch(action) {
            case METADATA_READ:
                return user.hasGroup("sdfs-metadata-read");
            case METADATA_WRITE:
                return user.hasGroup("sdfs-metadata-write");
            case FILE_READ:
                return user.hasGroup("sdfs-file-read");
            case FILE_WRITE:
                return user.hasGroup("sdfs-file-write");
            case FILE_DELETE:
                return user.hasGroup("sdfs-file-delete");
            case VOLUME_READ:
                return user.hasGroup("sdfs-volume-read");
            case CONFIG_READ:
                return user.hasGroup("sdfs-config-read");
            case CONFIG_WRITE:
                return user.hasGroup("sdfs-config-write");
            case EVENT_READ:
                return user.hasGroup("sdfs-event-read");
            default:
                return false;
        }

    }
    
}
