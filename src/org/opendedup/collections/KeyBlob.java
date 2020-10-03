package org.opendedup.collections;

import java.io.Serializable;

public class KeyBlob implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -2753966297671970793L;
    /**
     * 
     */
    public byte[] key;

    public KeyBlob(byte[] key) {
        this.key = key;
    }

    public byte[] getKey() {
        return this.key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }
}