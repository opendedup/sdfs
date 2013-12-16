/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package org.opendedup.buse.driver;

/**
 * This is an enumeration of error return values
 */
public interface Errno {
	//
	// generated from <errno.h>

	public static final int EPERM = 1; /* Operation not permitted */
	public static final int ENOENT = 2; /* No such file or directory */
	public static final int ESRCH = 3; /* No such process */
	public static final int EINTR = 4; /* Interrupted system call */
	public static final int EIO = 5; /* I/O error */
	public static final int ENXIO = 6; /* No such device or address */
	public static final int E2BIG = 7; /* Arg list too long */
	public static final int ENOEXEC = 8; /* Exec format error */
	public static final int EBADF = 9; /* Bad file number */
	public static final int ECHILD = 10; /* No child processes */
	public static final int EAGAIN = 11; /* Try again */
	public static final int ENOMEM = 12; /* Out of memory */
	public static final int EACCES = 13; /* Permission denied */
	public static final int EFAULT = 14; /* Bad address */
	public static final int ENOTBLK = 15; /* Block device required */
	public static final int EBUSY = 16; /* Device or resource busy */
	public static final int EEXIST = 17; /* File exists */
	public static final int EXDEV = 18; /* Cross-device link */
	public static final int ENODEV = 19; /* No such device */
	public static final int ENOTDIR = 20; /* Not a directory */
	public static final int EISDIR = 21; /* Is a directory */
	public static final int EINVAL = 22; /* Invalid argument */
	public static final int ENFILE = 23; /* File table overflow */
	public static final int EMFILE = 24; /* Too many open files */
	public static final int ENOTTY = 25; /* Not a typewriter */
	public static final int ETXTBSY = 26; /* Text file busy */
	public static final int EFBIG = 27; /* File too large */
	public static final int ENOSPC = 28; /* No space left on device */
	public static final int ESPIPE = 29; /* Illegal seek */
	public static final int EROFS = 30; /* Read-only file system */
	public static final int EMLINK = 31; /* Too many links */
	public static final int EPIPE = 32; /* Broken pipe */
	public static final int EDOM = 33; /* Math argument out of domain of func */
	public static final int ERANGE = 34; /* Math result not representable */
	public static final int EDEADLK = 35; /* Resource deadlock would occur */
	public static final int ENAMETOOLONG = 36; /* File name too long */
	public static final int ENOLCK = 37; /* No record locks available */
	public static final int ENOSYS = 38; /* Function not implemented */
	public static final int ENOTEMPTY = 39; /* Directory not empty */
	public static final int ELOOP = 40; /* Too many symbolic links encountered */
	public static final int EWOULDBLOCK = EAGAIN; /* Operation would block */
	public static final int ENOMSG = 42; /* No message of desired type */
	public static final int EIDRM = 43; /* Identifier removed */
	public static final int ECHRNG = 44; /* Channel number out of range */
	public static final int EL2NSYNC = 45; /* Level 2 not synchronized */
	public static final int EL3HLT = 46; /* Level 3 halted */
	public static final int EL3RST = 47; /* Level 3 reset */
	public static final int ELNRNG = 48; /* Link number out of range */
	public static final int EUNATCH = 49; /* Protocol driver not attached */
	public static final int ENOCSI = 50; /* No CSI structure available */
	public static final int EL2HLT = 51; /* Level 2 halted */
	public static final int EBADE = 52; /* Invalid exchange */
	public static final int EBADR = 53; /* Invalid request descriptor */
	public static final int EXFULL = 54; /* Exchange full */
	public static final int ENOANO = 55; /* No anode */
	public static final int EBADRQC = 56; /* Invalid request code */
	public static final int EBADSLT = 57; /* Invalid slot */
	public static final int EDEADLOCK = EDEADLK;
	public static final int EBFONT = 59; /* Bad font file format */
	public static final int ENOSTR = 60; /* Device not a stream */
	public static final int ENODATA = 61; /* No data available */
	public static final int ETIME = 62; /* Timer expired */
	public static final int ENOSR = 63; /* Out of streams resources */
	public static final int ENONET = 64; /* Machine is not on the network */
	public static final int ENOPKG = 65; /* Package not installed */
	public static final int EREMOTE = 66; /* Object is remote */
	public static final int ENOLINK = 67; /* Link has been severed */
	public static final int EADV = 68; /* Advertise error */
	public static final int ESRMNT = 69; /* Srmount error */
	public static final int ECOMM = 70; /* Communication error on send */
	public static final int EPROTO = 71; /* Protocol error */
	public static final int EMULTIHOP = 72; /* Multihop attempted */
	public static final int EDOTDOT = 73; /* RFS specific error */
	public static final int EBADMSG = 74; /* Not a data message */
	public static final int EOVERFLOW = 75; /*
											 * Value too large for defined data
											 * type
											 */
	public static final int ENOTUNIQ = 76; /* Name not unique on network */
	public static final int EBADFD = 77; /* File descriptor in bad state */
	public static final int EREMCHG = 78; /* Remote address changed */
	public static final int ELIBACC = 79; /*
										 * Can not access a needed shared
										 * library
										 */
	public static final int ELIBBAD = 80; /* Accessing a corrupted shared library */
	public static final int ELIBSCN = 81; /* .lib section in a.out corrupted */
	public static final int ELIBMAX = 82; /*
										 * Attempting to link in too many shared
										 * libraries
										 */
	public static final int ELIBEXEC = 83; /*
											 * Cannot exec a shared library
											 * directly
											 */
	public static final int EILSEQ = 84; /* Illegal byte sequence */
	public static final int ERESTART = 85; /*
											 * Interrupted system call should be
											 * restarted
											 */
	public static final int ESTRPIPE = 86; /* Streams pipe error */
	public static final int EUSERS = 87; /* Too many users */
	public static final int ENOTSOCK = 88; /* Socket operation on non-socket */
	public static final int EDESTADDRREQ = 89; /* Destination address required */
	public static final int EMSGSIZE = 90; /* Message too long */
	public static final int EPROTOTYPE = 91; /* Protocol wrong type for socket */
	public static final int ENOPROTOOPT = 92; /* Protocol not available */
	public static final int EPROTONOSUPPORT = 93; /* Protocol not supported */
	public static final int ESOCKTNOSUPPORT = 94; /* Socket type not supported */
	public static final int EOPNOTSUPP = 95; /*
											 * Operation not supported on
											 * transport endpoint
											 */
	public static final int EPFNOSUPPORT = 96; /* Protocol family not supported */
	public static final int EAFNOSUPPORT = 97; /*
												 * Address family not supported
												 * by protocol
												 */
	public static final int EADDRINUSE = 98; /* Address already in use */
	public static final int EADDRNOTAVAIL = 99; /*
												 * Cannot assign requested
												 * address
												 */
	public static final int ENETDOWN = 100; /* Network is down */
	public static final int ENETUNREACH = 101; /* Network is unreachable */
	public static final int ENETRESET = 102; /*
											 * Network dropped connection
											 * because of reset
											 */
	public static final int ECONNABORTED = 103; /*
												 * Software caused connection
												 * abort
												 */
	public static final int ECONNRESET = 104; /* Connection reset by peer */
	public static final int ENOBUFS = 105; /* No buffer space available */
	public static final int EISCONN = 106; /*
											 * Transport endpoint is already
											 * connected
											 */
	public static final int ENOTCONN = 107; /*
											 * Transport endpoint is not
											 * connected
											 */
	public static final int ESHUTDOWN = 108; /*
											 * Cannot send after transport
											 * endpoint shutdown
											 */
	public static final int ETOOMANYREFS = 109; /*
												 * Too many references: cannot
												 * splice
												 */
	public static final int ETIMEDOUT = 110; /* Connection timed out */
	public static final int ECONNREFUSED = 111; /* Connection refused */
	public static final int EHOSTDOWN = 112; /* Host is down */
	public static final int EHOSTUNREACH = 113; /* No route to host */
	public static final int EALREADY = 114; /* Operation already in progress */
	public static final int EINPROGRESS = 115; /* Operation now in progress */
	public static final int ESTALE = 116; /* Stale NFS file handle */
	public static final int EUCLEAN = 117; /* Structure needs cleaning */
	public static final int ENOTNAM = 118; /* Not a XENIX named type file */
	public static final int ENAVAIL = 119; /* No XENIX semaphores available */
	public static final int EISNAM = 120; /* Is a named type file */
	public static final int EREMOTEIO = 121; /* Remote I/O error */
	public static final int EDQUOT = 122; /* Quota exceeded */
	public static final int ENOMEDIUM = 123; /* No medium found */
	public static final int EMEDIUMTYPE = 124; /* Wrong medium type */

	// extended attributes support needs these...

	public static final int ENOATTR = ENODATA; /* No such attribute */
	public static final int ENOTSUPP = 524; /* Operation is not supported */
}
