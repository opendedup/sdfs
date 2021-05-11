package org.opendedup.sdfs.mgmt.grpc;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.google.protobuf.InvalidProtocolBufferException;

import org.json.JSONException;
import org.json.JSONObject;
import org.opendedup.grpc.SDFSCli.SdfsPermissions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

/**
 *
 * @author user
 */
public class JWebToken {

    private static final String SECRET_KEY = Main.sdfsPasswordSalt;
    private static final String ISSUER = "opendedup.org";
    private static final String JWT_HEADER = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
    private JSONObject payload = new JSONObject();
    private String signature;
    private String encodedHeader;
    private SdfsPermissions permissions;

    private JWebToken() {
        encodedHeader = encode(new JSONObject(JWT_HEADER));
    }

    public JWebToken(JSONObject payload) {
        this(payload.getString("sub"), payload.getString("aud"), payload.getLong("exp"));
    }

    public JWebToken(String sub, String aud, long expires) {
        this();
        payload.put("sub", sub);
        payload.put("aud", aud);
        payload.put("exp", expires);
        payload.put("iat", LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
        payload.put("iss", ISSUER);
        payload.put("jti", UUID.randomUUID().toString()); //how do we use this?
        signature = hmacSha256(encodedHeader + "." + encode(payload), SECRET_KEY);
    }

    /**
     * For verification
     *
     * @param token
     * @throws java.security.NoSuchAlgorithmException
     * @throws InvalidProtocolBufferException
     */
    public JWebToken(String token) throws NoSuchAlgorithmException, InvalidProtocolBufferException {
        this();
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid Token format");
        }
        if (encodedHeader.equals(parts[0])) {
            encodedHeader = parts[0];
        } else {
            throw new NoSuchAlgorithmException("JWT Header is Incorrect: " + parts[0]);
        }

        payload = new JSONObject(decode(parts[1]));
        if (payload.isEmpty()) {
            throw new JSONException("Payload is Empty: ");
        }
        if (!payload.has("exp")) {
            throw new JSONException("Payload doesn't contain expiry " + payload);
        }
        if(payload.has("aud")) {
            byte [] pb = Base64.getDecoder().decode(payload.getString("aud"));
            permissions = SdfsPermissions.parseFrom(pb);
        }
        signature = parts[2];
    }

    public boolean hasPermission(AuthUtils.ACTIONS action) {
        if (permissions.getADMIN()) {
            return true;
        }
        switch(action) {
            case METADATA_READ:
                return permissions.getMETADATAREAD();
            case METADATA_WRITE:
                return permissions.getMETADATAWRITE();
            case FILE_READ:
                return permissions.getFILEREAD();
            case FILE_WRITE:
                return permissions.getFILEWRITE();
            case FILE_DELETE:
                return permissions.getFILEDELETE();
            case VOLUME_READ:
                return permissions.getVOLUMEREAD();
            case CONFIG_READ:
                return permissions.getCONFIGREAD();
            case CONFIG_WRITE:
                return permissions.getCONFIGWRITE();
            case EVENT_READ:
                return permissions.getEVENTREAD();
            case AUTH_READ:
                return permissions.getAUTHREAD();
            case AUTH_WRITE:
                return permissions.getAUTHWRITE();
            default:
                return false;
        }
        
    }

    @Override
    public String toString() {
        return encodedHeader + "." + encode(payload) + "." + signature;
    }

    public boolean isValid() {
        return payload.getLong("exp") > (LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)) //token not expired
                && signature.equals(hmacSha256(encodedHeader + "." + encode(payload), SECRET_KEY)); //signature matched
    }

    public String getSubject() {
        return payload.getString("sub");
    }

    public SdfsPermissions getAudience() {
        
        return this.permissions;
    }

    private static String encode(JSONObject obj) {
        return encode(obj.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String encode(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static String decode(String encodedString) {
        return new String(Base64.getUrlDecoder().decode(encodedString));
    }

    /**
     * Sign with HMAC SHA256 (HS256)
     *
     * @param data
     * @return
     * @throws Exception
     */
    private String hmacSha256(String data, String secret) {
        try {

            //MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = secret.getBytes(StandardCharsets.UTF_8);//digest.digest(secret.getBytes(StandardCharsets.UTF_8));

            Mac sha256Hmac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(hash, "HmacSHA256");
            sha256Hmac.init(secretKey);

            byte[] signedBytes = sha256Hmac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return encode(signedBytes);
        } catch (NoSuchAlgorithmException | InvalidKeyException ex) {
            SDFSLogger.getLog().error(ex.getMessage(), ex);
            return null;
        }
    }

}