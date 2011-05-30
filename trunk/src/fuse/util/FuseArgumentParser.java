package fuse.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple argument parser to provide the correct arguments to the underlying fuse system.
 */
public class FuseArgumentParser {

    private String mountPoint;
    private String source;
    private boolean foreground;
    private List<String> remaining = new ArrayList<String>();

    public FuseArgumentParser(String[] args) {
        boolean seenOption = false;

        for (String arg : args) {
            if (seenOption == true) {
                remaining.add(arg);
                seenOption = false;
            } else if ("-o".equals(arg)) {
                remaining.add(arg);
                seenOption = true;
            } else if ("-f".equals(arg)) {
                foreground = true;
            } else if (mountPoint == null) {
                mountPoint = arg;
            } else if (source == null) {
                source = arg;
            } else {
                remaining.add(arg);
            }
        }
    }

    /**
     * Returns the to primary parameters needed for the fuse C code.
     *
     * @return the mount point and -f flag.
     */
    public String[] getFuseArgs() {
        if (foreground) {
            return new String[]{mountPoint, "-f"};
        }

        return new String[]{mountPoint};
    }

    public String getMountPoint() {
        return mountPoint;
    }

    /**
     * Most file systems have a primary source (device name)
     *
     * @return the source location.
     */
    public String getSource() {
        return source;
    }

    /**
     * Corresponds to the -f flag to put the fuse file system into the foreground.
     *
     * @return true if filesystem should run in the foreground.
     */
    public boolean isForeground() {
        return foreground;
    }

    /**
     * Returns any remaining parameters.
     *
     * @return the additional parameters.
     */
    public List<String> getRemaining() {
        return remaining;
    }
}
