package fuse;

import fuse.compat.Filesystem1;
import fuse.compat.Filesystem1ToFilesystem2Adapter;
import fuse.compat.Filesystem2;
import fuse.compat.Filesystem2ToFilesystem3Adapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Factory object which wraps FileSystem(1,2,3) objects
 * inside the correct FuseFS adapters
 */
public class FuseFSFactory {
    /**
     * Wraps a filesystem1 object inside a FuseFS adapter.
     *
     * @param filesystem1 The filesystem to adapt.
     * @param log         The logger that should be used by the adapter.
     * @return The new FuseFS adapter.
     */
    public static FuseFS adapt(Filesystem1 filesystem1, Log log) {
        return adapt(new Filesystem1ToFilesystem2Adapter(filesystem1), log);
    }

    /**
     * Wraps a filesystem2 object inside a FuseFS adapter.
     *
     * @param filesystem2 The filesystem to adapt.
     * @param log         The logger that should be used by the adapter.
     * @return The new FuseFS adapter.
     */
    public static FuseFS adapt(Filesystem2 filesystem2, Log log) {
        return adapt(new Filesystem2ToFilesystem3Adapter(filesystem2), log);
    }

    /**
     * Wraps a filesystem3 object inside a FuseFS adapter.
     *
     * @param filesystem3 The filesystem to adapt.
     * @param log         The logger that should be used by the adapter.
     * @return The new FuseFS adapter.
     */
    public static FuseFS adapt(Filesystem3 filesystem3, Log log) {
        return new Filesystem3ToFuseFSAdapter(filesystem3, log);
    }

    /**
     * Detects the type of the filesystem and wraps it.
     * <p/>
     * This method primarily exists to support JNI code.
     * So that the logic of figuring out the class of a
     * particular filesystem can be encoded in java rather
     * than in C.
     * <p/>
     * This method uses a logger for the filesystem object that
     * was passed to it.
     *
     * @param filesystem The filsystem (1,2 or 3) object to be wrapped.
     * @return The new FuseFS adapter
     *         or null if the Filesystem type is not recognized.
     */
    public static FuseFS adapt(Object filesystem) {
        Log log = LogFactory.getLog(filesystem.getClass());

        return adapt(filesystem, log);
    }

    /**
     * Detects the type of the filesystem and wraps it.
     * <p/>
     * This method primarily exists to support JNI code.
     * So that the logic of figuring out the class of a
     * particular filesystem can be encoded in java rather
     * than in C.
     *
     * @param filesystem The filsystem (1,2 or 3) object to be wrapped.
     * @param log        The logger that should be used by the adapter.
     * @return The new FuseFS adapter
     *         or null if the Filesystem type is not recognized.
     */
    public static FuseFS adapt(Object filesystem, Log log) {
        if (filesystem instanceof Filesystem3) {
            return adapt((Filesystem3) filesystem, log);
        }

        if (filesystem instanceof Filesystem2) {
            return adapt((Filesystem2) filesystem, log);
        }

        if (filesystem instanceof Filesystem1) {
            return adapt((Filesystem1) filesystem, log);
        }

        return null;
    }
}
