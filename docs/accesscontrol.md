# Access Control for SDFS

SDFS has access controls per api call for SDFS. Access Control is administered through the sdfscli on a per volume basis.

## Built in user
SDFS Comes with one built in user, admin. This user is admistered through the sdfscli and has access to all sdfs apis. By default the built in user does not require a password. To enable a password do the following

### For new volumes
```bash
mkfs.sdfs --volume-name=pool0 --volume-capacity=100GB --sdfscli-password apassword --sdfscli-require-auth
```

### For existing volumes
1. Run sdfscli config -password apassword123 -trust-all
2. Unmount the volume using sdfscli --shutdown -trust-all
3. edit the xml config in /etc/sdfs or c:\program files\sdfs\etc\ and change **enable-auth** to true.
```xml
<sdfscli enable="true" enable-auth="true" enable-mutual-tls-auth="false" listen-address="localhost" password="dc4e4ebbc818fe7aca46d3528d4ae68e87767a2319a2ac8fc43161844ad17593" port="6442" salt="WVvu8u" use-ssl="false"/>
```
4. Remount the volume
5. Test with sdfscli config -dse -u admin -p apassword123 -trust-all

## Additional Users
SDFS Access controls allow for additional users with specific access to api calls. This can be used to limit administrative access to a volume by specific users or systems. Access controls are applied to all files and functions on the volumes.

###
 Types of Access

|Access Flag|Description|
|-----------|-----------|
|METADATA_READ| Read File Metadata|
|METADATA_WRITE| Write File Metadata|
|FILE_READ| Read  Files|
|FILE_WRITE| Write Files|
|FILE_DELETE| Delete Files|
|VOLUME_READ| Read Volume Metadata|
|CONFIG_READ| Read Volume Config|
|CONFIG_WRITE| Write Volume Config|
|EVENT_READ| Read Events|
|AUTH_READ| List and Read User ACLs|
|AUTH_WRITE| Write User ACLS and create/delete users|

### Adding A User

```bash
sdfscli user -add user1 -user-password iL0ve#ranges -u admin -p apassword -trust-all
```

### Setting User Permissions

```bash
sdfscli user -set-permissions user1 -permission FILE_READ -permission FILE_WRITE -permission VOLUME_READ -permission METADATA_READ -permission METADATA_WRITE -u admin -p apassword -trust-all
```

### List Users

```bash
sdfscli user -list -u admin -p apassword -trust-all
```
### Authenticate as the created user
```bash
sdfscli file -list . -u user1 -p iL0ve\#ranges -trust-all
```

### DB Location

The SDFS Access Control Database is located at /opt/sdfs/volumes/\<volume-name\>/permissions/volume_permissions.pb on linux and c:\program files\sdfs\volumes\\<volume-name\>\permissions\volume_permissions.pb on windows 

## API Calls Required Permission

|API Call| Required Permission|
|-----|----|
|AddUser| AUTH_WRITE|
|DeleteUser | AUTH_WRITE|
|listUsers | AUTH_READ|
|setSdfsPassword| AUTH_WRITE|
|setSdfsPermissions| AUTH_WRITE|
|mkDir|FILE_WRITE|
|mkDirAll| FILE_WRITE|
|getCloudFile| FILE_WRITE|
|getCloudMetaFile | FILE_WRITE|
|rmDir|FILE_DELETE|
|setUserMetaData|METADATA_WRITE|
|unlink|FILE_DELETE|
|write|FILE_WRITE|
|release|FILE_WRITE or FILE_READ|
|mknod|FILE_WRITE|
|open|FILE_WRITE or FILE_READ|
|read|FILE_READ|
|stat|METADATA_READ|
|chmod|FILE_WRITE|
|chown|FILE_WRITE|
|flush|FILE_WRITE|
|fsync|FILE_WRITE|
|getAttr|METADATA_READ|
|readLink|FILE_READ|
|symLink|FILE_WRITE|
|truncate|FILE_WRITE|
|utime|FILE_WRITE|
|getXAttr|METADATA_READ|
|getXAttrSize|METADATA_READ|
|setXAttr|METADATA_WRITE|
|removeXAttr|METADATA_WRITE|
|getFileInfo|METADATA_READ|
|rename|FILE_WRITE|
|copyExtent|FILE_WRITE|
|createCopy|FILE_WRITE|
|fileExists|METADATA_READ|
|statFS|METADATA_READ|
|fileNotification|METADATA_READ|
|getEvent|EVENT_READ|
|subscribeEvent|EVENT_READ|
|listEvents|EVENT_READ|
|getMetaDataDedupeFile|FILE_READ|
|getSparseDedupeFile|FILE_READ|
|checkHashes|FILE_WRITE|
|writeChunks|FILE_WRITE|
|writeSparseDataChunk|FILE_WRITE|
|hashingInfo|CONFIG_READ|
|shutdownVolume|CONFIG_WRITE|
|getConnectedVolumes|VOLUME_READ|
|setVolumeCapacity|CONFIG_WRITE|
|getGCSchedule|CONFIG_READ|
|setPassword|Admin Only|
|setReadSpeed|CONFIG_WRITE|
|setWriteSpeed|CONFIG_WRITE|
|syncFromCloudVolume|FILE_WRITE|
|syncCloudVolume|FILE_WRITE|
|setCacheSize|CONFIG_WRITE|
|systemInfo|CONFIG_READ|
|setMaxAge|CONFIG_WRITE|
|dSEInfo|CONFIG_READ|
|cleanStore|CONFIG_WRITE|
|deleteCloudVolume|CONFIG_WRITE|