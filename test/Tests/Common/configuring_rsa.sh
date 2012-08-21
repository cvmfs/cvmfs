openssl x509 -fingerprint -sha1 -in /tmp/cvmfs_test.crt | grep "SHA1 Fingerprint" | sed 's/SHA1 Fingerprint=//' > /tmp/whitelist.test.unsigned
echo `date -u "+%Y%m%d%H%M%S"` > /tmp/whitelist.test.signed
echo "E`date -u --date='next month' "+%Y%m%d%H%M%S"`" >> /tmp/whitelist.test.signed
echo "N$1" >> /tmp/whitelist.test.signed
cat /tmp/whitelist.test.unsigned >> /tmp/whitelist.test.signed
sha1=`openssl sha1 < /tmp/whitelist.test.signed | tr -d " " | sed 's/(stdin)=//' | head -c40`
echo "--" >> /tmp/whitelist.test.signed
echo $sha1 >> /tmp/whitelist.test.signed
echo $sha1 | head -c 40 > /tmp/whitelist.test.sha1
openssl genrsa -out /tmp/cvmfs_master.key 2048
openssl rsa -in /tmp/cvmfs_master.key -pubout -out /tmp/cvmfs_master.pub
openssl rsautl -inkey /tmp/cvmfs_master.key -sign -in /tmp/whitelist.test.sha1 -out /tmp/whitelist.test.signature
cat /tmp/whitelist.test.signature >> /tmp/whitelist.test.signed
