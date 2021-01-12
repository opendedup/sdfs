#!/bin/bash
RUNCMD="mkfs.sdfs --volume-name=pool0 --volume-capacity=${CAPACITY} --sdfscli-listen-addr=0.0.0.0"

if [ -n "${TYPE}" ]; then
    RUNCMD="mkfs.sdfs --volume-name=pool0 --volume-capacity=${CAPACITY} --sdfscli-listen-addr=0.0.0.0"
    if [ "${TYPE}" == 'AWS' ]; then
        RUNCMD+=" --aws-enabled=true  --cloud-bucket-name ${BUCKET_NAME}"
    elif [ "${TYPE}" == 'AZURE' ]; then
        RUNCMD+=" --azure-enabled=true  --cloud-bucket-name ${BUCKET_NAME}"
    elif [ "${TYPE}" == 'GOOGLE' ]; then
        RUNCMD+=" --google-enabled=true  --cloud-bucket-name ${BUCKET_NAME}"
    elif [ "${TYPE}" == 'BACKBLAZE' ]; then
        RUNCMD+=" --backblaze-enabled  --cloud-bucket-name ${BUCKET_NAME}"
    fi
    if [ -n "${URL}" ]; then
        RUNCMD+=" --cloud-url ${URL}"
    fi
    if [ -n "${BACKUP_VOLUME}" ]; then
        RUNCMD+=" --backup-volume"
    fi
    if [ -n "${GCS_CREDS_FILE}" ]; then
        RUNCMD+=" --gcs-creds-file ${GCS_CREDS_FILE}"
    elif [ -n "${ACCESS_KEY}" ]; then
        RUNCMD+=" --cloud-access-key ${ACCESS_KEY} --cloud-secret-key ${SECRET_KEY}"
    elif [ -n "${AWS_AIM}" ]; then
        RUNCMD+=" --aws-aim"
    fi
    if [ -n "${PUBSUB_PROJECT}" ]; then
        RUNCMD+=" --pubsub-project ${PUBSUB_PROJECT} --enable-global-syncronization"
    fi
    if [ -n "${PUBSUB_CREDS_FILE}" ]; then
        RUNCMD+=" --pubsub-authfile ${PUBSUB_CREDS_FILE}"
    elif [ -n "${GCS_CREDS_FILE}" ]; then
        RUNCMD+=" --pubsub-authfile ${GCS_CREDS_FILE}"
    fi
fi

ADDLCMD=""

if [ ! -f /etc/sdfs/pool0-volume-cfg.xml ]; then
    echo ${RUNCMD}
    ${RUNCMD}
    ADDLCMD+=" -w"
fi

if [ -n "${DISABLE_TLS}" ]; then
        ADDLCMD+=" -s"
fi
echo ${ADDLCMD}
rm -rf /var/run/pool0.pid
startsdfs -q -n -v pool0 ${ADDLCMD}