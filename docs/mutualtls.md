# SDFS TLS

SDFS supports TLS with the GRPC API and is used by default unless otherwise specified.

## Default Configuration
By Default SDFS uses TLS v1.3 for all GRPC API traffic using a self signed certificate generated on initialization. The keys are stored in /opt/sdfs/volumes/<volume-name>/keys/ at follows:
* tls_key.key - A pkcs8 private key
* tls_key.pem - A public cert for the generated private key

These keypaths can be changed by setting environmental variables during mount of the volume. The following environmental variables are supported:

| Envronmental Variable | Description | Default |
|-----------------------|-------------|---------|
| SDFS_PRIVATE_KEY | The absolute path to the SDFS private key | /opt/sdfs/volumes/<volume-name>/keys/tls_key.key|
| SDFS_CERT_CHAIN | The absolute pathe to the SDFS public cert | /opt/sdfs/volumes/<volume-name>/keys/tls_key.pem|
| SDFS_SIGNER_CHAIN | The optional signer public cert of the public cert signer. This file is required for mutual tls| /opt/sdfs/volumes/<volume-name>/keys/signer_key.crt|


## Disable TLS

To disable TLS during mkfs.sdfs creation.

```bash
mkfs.sdfs --volume-name=pool0 --volume-capacity=100GB --sdfscli-disable-ssl
```
To disable TLS after creation edit the xml config located in /etc/sdfs/<volume-name>-volume-cfg.xml and set the use-ssl attribute to false inside the sdfscli xml tag.

```xml
<sdfscli enable="true" enable-auth="false" enable-mutual-tls-auth="false" listen-address="localhost" password="dc4e4ebbc818fe7aca46d3528d4ae68e87767a2319a2ac8fc43161844ad17593" port="6442" salt="WVvu8u" use-ssl="false"/>
```


## Mutual TLS Configuration

Mutual TLS can also be configured for SDFS. With mutual TLS both the client and the server authenticate to eachother using a certificated signed by the same CA. To use mutual TLS use the following steps

### Requirements
* SDFS Installed
* Certstrap - https://github.com/square/certstrap
* Openssl

#### Configuring the SDFS Server

1. Create a volume with mutual tls enabled
```bash
mkfs.sdfs --volume-name=pool0 --volume-capactiy=100GB --sdfscli-require-mutual-tls
```

To enable mutual TLS after creation after creation edit the xml config located in /etc/sdfs/<volume-name>-volume-cfg.xml and set the enable-mutual-tls-auth attribute to true inside the sdfscli xml tag.
```xml
<sdfscli enable="true" enable-auth="false" enable-mutual-tls-auth="true" listen-address="localhost" password="dc4e4ebbc818fe7aca46d3528d4ae68e87767a2319a2ac8fc43161844ad17593" port="6442" salt="WVvu8u" use-ssl="false"/>
```

2. Create a certificate chain for the SDFS Server
```bash
# Create a Signer Certificate
certstrap init --common-name="signer_key"
# Create the key for the SDFS Server
certstrap request-cert --common-name tls_key
# Sign the SDFS Server Certificate with the Signer Key
certstrap sign tls_key --CA signer_key
# Convert the Key to pkcs8
openssl pkcs8 -topk8 -nocrypt -in out/tls_key.key -out out/tls_key_pkcs8.key
```

3. Copy the key to the volume key directory
```bash
mkdir -p /opt/sdfs/volumes/<volume-name>/keys/
cp out/signer_key.crt /opt/sdfs/volumes/<volume-name>/keys/
cp out/tls_key.crt /opt/sdfs/volumes/<volume-name>/keys/tls_key.pem
cp out/tls_key_pkcs8.key /opt/sdfs/volumes/<volume-name>/keys/tls_key.key
```

4. Start the volume
```bash
startsdfscli -v pool0 -n
```

#### Configuring sdfscli
config -trust-all -root-ca keys/signer_key.crt -mtls-key keys/scooby.key -mtls-cert keys/scooby.crt -mtls -dse

1. Create a signed certificate with the same signer cert at the server cert. Make sure you are in the same directory where you created the signer cert and it is located in a subdirectory as out/signer_key.key and out/signer_key.crt
```bash
# Create the key for the SDFS Client
certstrap request-cert --common-name client
# Sign the SDFS Client Certificate with the Signer Key
certstrap sign tls_key --CA signer_key
```

2. Copy the key to the default location.
```bash
mkdir -p $HOME/.sdfs/keys/
cp out/signer_key.crt $HOME/.sdfs/keys/ca.crt
cp out/client.key $HOME/.sdfs/keys/client.key
cp out/client.crt $HOME/.sdfs/keys/client.crt
```

3. Test the connection
```bash
sdfscli -trust-all -dse -mtls
```