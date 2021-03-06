package org.opendedup.sdfs.filestore.cloud.gcp;

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;

public class CustomGCPSigner extends AWS4Signer {
    @Override
    public void sign(SignableRequest<?> request, AWSCredentials credentials) {
        request.addHeader("Authorization", "Bearer " + credentials.getAWSAccessKeyId());
    }
    
}