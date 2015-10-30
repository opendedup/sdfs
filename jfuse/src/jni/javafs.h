/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */


#ifndef _JAVAFS_H_
#define _JAVAFS_H_

#define FUSE_USE_VERSION 22
#include <fuse.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/statfs.h>

#include <jni.h>


// #define DEBUG 1

#define MAX_GETDIR_NAME_LENGTH 1024


#ifdef TRACE
#undef TRACE
#endif
#ifdef WARN
#undef WARN
#endif
#ifdef ERROR
#undef ERROR
#endif

#ifdef __cplusplus

#include <iostream>

#ifdef DEBUG
#define TRACE(x) 	cout<<std::hex<<"["<<getpid()<<"]("<<__func__<<")"<<x<<"\n"
#define WARN(x)		cerr<<std::hex<<"["<<getpid()<<"]("<<__func__<<")"<<x<<"\n"
#define ERROR(x)	cerr<<std::hex<<"["<<getpid()<<"]("<<__func__<<")"<<x<<"\n"
#else
#define TRACE(x...)	do{}while(0)
#define WARN(x...)	do{}while(0)
#define ERROR(x...)	cerr<<x<<"\n"
#endif

#else

#include <stdio.h>

#ifdef DEBUG
#define TRACE(x...)	do{fprintf(stdout, "[%x](%s) ", getpid(), __func__); fprintf(stdout, x); fprintf(stdout, "\n");}while(0)
#define WARN(x...)	do{fprintf(stdout, "[%x](%s) ", getpid(), __func__); fprintf(stdout, x); fprintf(stdout, "\n");}while(0)
#define ERROR(x...)	do{fprintf(stderr, "[%x](%s) ", getpid(), __func__); fprintf(stdout, x); fprintf(stdout, "\n");}while(0)
#else
#define TRACE(x...)	do{}while(0)
#define WARN(x...)	do{}while(0)
#define ERROR(x...)	do{fprintf(stderr, x); fprintf(stderr, "\n");}while(0)
#endif

#endif


#ifdef __cplusplus
extern "C" {
#endif

#include "javafs_bindings.h"

#define IFTODT(mode)   (((mode) & 0170000) >> 12)

#ifdef __cplusplus
} /* end of extern "C" { */
#endif


#endif

