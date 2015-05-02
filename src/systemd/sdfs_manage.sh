#!/bin/bash
FSTAB=/etc/sdfs/fstab
PID=$BASHPID
################################################################################
#
# Start up checks
#
################################################################################
if [ ! -f "$FSTAB" ]; then
    echo "Missing fstab, nothing be mount"
    exit 1
fi

if [ -z "$2" ]; then
    echo "Specify pool for mount"
    exit 1
fi

if [ -z "$1" ]; then
    echo "{mount|umount} <pool name>"
fi

if ! grep "$2" "$FSTAB" &> /dev/null; then
    echo "Describe mount point and pool in $FSTAB"
    echo "Like a <pool name> <full path to mount point>"
    exit 1
fi

SUPPORTED_COMMAND=false

for i in mount umount; do
    if [ "$i" == "$1" ]; then
        SUPPORTED_COMMAND=true
        break
    fi
done

if ! $SUPPORTED_COMMAND; then
    echo "Command $1 not supported"
    echo "Specify {mount|umount} <pool name>"
fi


################################################################################
#
# Check fstab
#
################################################################################
# For avoid problems if user has change fstab file
BACKUP_FSTAB=/run/sdfs/"$2".fstab
if [ ! -f "$BACKUP_FSTAB" ]; then
    install -D "$FSTAB" "$BACKUP_FSTAB"
fi
FSTAB=$BACKUP_FSTAB

MOUNT_ENTRY="$(grep -v ^# $FSTAB | grep "$2" | tail -n 1)"

if [ -z "$MOUNT_ENTRY" ]; then
    echo "Can't find $1 in $FSTAB"
fi

check_path(){
    if [ ! -d "$2" ]; then
        echo "mount point $2 does not exist, check $FSTAB"
        exit 1
    fi
}

if check_path $MOUNT_ENTRY; then
    echo $MOUNT_ENTRY > "$BACKUP_FSTAB"
fi

################################################################################
#
# Command processing
#
################################################################################

PID_FILE=/run/sdfs/"$2".pid

mount_sdfs_wrapper(){
    if [ -z "$2" ]; then
        echo "Error in fstab for pool $1 mount point not specified"
        exit 1
    fi
    mount.sdfs "$1" "$2" &
    echo "export JAVA_PID=$!" > "$PID_FILE"
    while ! mountpoint -q "$2"; do
        sleep 1
    done
}

if [ "$1" == "mount" ]; then
    mount_sdfs_wrapper $MOUNT_ENTRY
    systemd-notify --ready --pid="$PID"
    wait
fi

umount_sdfs_wrapper(){
    if mountpoint -q "$2"; then
        umount "$2"
    fi
    . $PID_FILE
    while kill -0 "$JAVA_PID" &> /dev/null; do
        sleep 1
    done
}

if [ "$1" == "umount" ]; then
    [ -f "$BACKUP_FSTAB" ] && rm "$BACKUP_FSTAB"
    umount_sdfs_wrapper $MOUNT_ENTRY
fi

exit 0
