package org.opendedup.sdfs.filestore.cloud.gcp;

import java.io.IOException;

import com.amazonaws.auth.AWSSessionCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

public class GCPSessionCredentials implements AWSSessionCredentials {
    private final GoogleCredentials credentials;

    public GCPSessionCredentials(GoogleCredentials credentials) throws IOException {
        if(credentials != null) {
        this.credentials = credentials;
        } else {
         this.credentials = ServiceAccountCredentials.getApplicationDefault();
        }
    }
    private String getGCPToken() {
        try {
            this.credentials.refreshIfExpired();
        } catch (IOException ioex) {
            return "";
        }
        return credentials.getAccessToken().getTokenValue();
    }
    public String getAWSAccessKeyId() {
        return getGCPToken();
    }

    public String getAWSSecretKey() {
        return "";
    }

    public String getSessionToken() {
        return "";
    }
}