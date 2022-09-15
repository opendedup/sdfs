package org.opendedup.sdfs.filestore;

import org.opendedup.sdfs.notification.SDFSEvent;

public class SDFSDeleteEntry {
    public int ct;
    public SDFSEvent evt;
    public SDFSDeleteEntry(int ct, SDFSEvent evt) {
        this.ct = ct;
        this.evt = evt;
    }
    
}