package fuse;

import fuse.util.Struct;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Looks up a password entry by username or userid.
 * <p/>
 * This is necessary in order to map remote users to local user when the remote system
 * has different uid's to the local system or when the remote system does not provide uid's.
 */

public class PasswordEntry extends Struct {
    /**
     * Short username (jsmith) not real name (John Smith).
     */
    public String username;

    /**
     * Numeric user id.
     */
    public int uid;

    /**
     * Primary group (gid) of the user.
     */
    public int gid;

    /**
     * Home directory based on /etc/passwd.
     */
    public String homeDirectory;

    /**
     * Login shell, based on /etc/passwd.
     */
    public String shell;

    public PasswordEntry(Charset cs,
                         ByteBuffer username, int uid, int gid, ByteBuffer homeDirectory, ByteBuffer shell) {
        this.username = cs.decode(username).toString();
        this.uid = uid;
        this.gid = gid;
        this.homeDirectory = cs.decode(homeDirectory).toString();
        this.shell = cs.decode(shell).toString();
    }

    /**
     * Lookup a users password entry based on the given username.
     *
     * @param cs       character set used to map strings.
     * @param username the username to lookup
     * @return the password entry or null if an error (not found) occurs.
     */
    public static native PasswordEntry lookupByUsername(Charset cs, String username);

    /**
     * Lookup a users password entry based on the given uid.
     *
     * @param cs  character set used to map strings.
     * @param uid the user id to lookup
     * @return the password entry or null if an error (not found) occurs.
     */
    public static native PasswordEntry lookupByUid(Charset cs, int uid);

    /**
     * Append attributes for debugging purposes.
     *
     * @param buff       the buffer to append to.
     * @param isPrefixed are their other attributes before this?
     * @return true.
     */
    protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed) {
        buff.append(super.appendAttributes(buff, isPrefixed) ? ", " : " ");

        buff.append("username=").append(username)
            .append(", uid=").append(uid)
            .append(", gid=").append(gid)
            .append(", homeDirectory=").append(homeDirectory)
            .append(", shell=").append(shell);

        return true;
    }
}
