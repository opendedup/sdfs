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
* Certstrap -

openssl pkcs8 -topk8 -nocrypt -in pkcs1_key_file -out pkcs8_key.pem

1. Create a volume
