#!/bin/bash
export FSTAB=/etc/sdfs/fstab
export PID="$BASHPID"
export COMMAND="$1"
export POOL_NAME="$2"
export MOUNT_POINT=""
export LOCK_FILE=/run/sdfs/"$POOL_NAME".lock

startup_checks(){
    if [ ! -f "$FSTAB" ]; then
        echo "Missing $FSTAB, nothing can be mount"; exit 1
    elif [ -z "$POOL_NAME" ]; then
        echo "Specify pool for mount"; exit 1
    elif ! grep "$POOL_NAME" "$FSTAB" &> /dev/null; then
        echo "Describe mount point and pool in $FSTAB"
        echo "Like a <pool name> <full path to mount point>"
        exit 1
    fi

    SUPPORTED_COMMAND=false
    for i in mount umount; do
        if [ "$i" == "$COMMAND" ]; then
            SUPPORTED_COMMAND=true
            break
        fi
    done
    if ! $SUPPORTED_COMMAND; then
        echo "Command <${COMMAND}> not supported"
        echo "Specify {mount|umount} <pool name>"
        exit 1
    fi
}
startup_checks

mount_point_get(){
    MOUNT_POINT="$2"
}

# Sigint, sigterm wrapper, for safe stopping java machine
trap "$0 umount $POOL_NAME" SIGINT SIGTERM

check_fstab(){
    MOUNT_ENTRY="$(grep -v ^# $FSTAB | grep "$POOL_NAME" | tail -n 1)"
    if [ -z "$MOUNT_ENTRY" ]; then
        echo "Can't find $POOL_NAME in $FSTAB"
        exit 1
    else
        mount_point_get $MOUNT_ENTRY
        if [ ! -d "$MOUNT_POINT" ]; then
            echo "mount point $MOUNT_POINT does not exist, check $FSTAB"
            exit 1
        fi
    fi
}

if [ "$1" == "mount" ]; then
    check_fstab
    mount.sdfs "$POOL_NAME" "$MOUNT_POINT" &
    MAIN_PID=$!
    {
        # store info about java engine pid and mount point for it
        echo "export MAIN_PID=$MAIN_PID"
        echo "export MOUNT_POINT=$MOUNT_POINT"
    } > "$LOCK_FILE"
    while ! mountpoint -q "$MOUNT_POINT"; do
        sleep 1
        if ! kill -0 "$MAIN_PID"; then
            echo "Engine crashed before mount are completed"
            exit 1
        fi
    done
    systemd-notify --ready --pid="$PID"
    # Wait before engine has successfully stoped
    wait
elif [ "$1" == "umount" ]; then
    if [ ! -f "$LOCK_FILE" ]; then
        echo "$POOL_NAME can't maintain umount for it"
        exit 1
    fi
    . "$LOCK_FILE"
    if mountpoint -q "$MOUNT_POINT"; then
        umount "$MOUNT_POINT"
    fi
    while kill -0 "$MAIN_PID"; do
        sleep 1
    done
fi

[ -f "$LOCK_FILE" ] && rm "$LOCK_FILE"

exit 0
