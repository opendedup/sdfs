sudo chown -R root:root ./deb
sudo dpkg -b deb
mv deb.deb sdfs-3.0.1_amd64.deb
alien -r sdfs-3.0.1_amd64.deb
cp sdfs-3.0.1_amd64.deb ../../datish-iso/ubuntu-14.04.3-server-amd64/pool/extra/
sudo chown -R samsilverberg:samsilverberg ./deb
