#! /bin/bash

DEPLOY_DIR="/usr/lib/mastermind"

echo "Clean old version of combainer:"

/etc/init.d/cocaine-server stop

for app in 'mastermind';
do 
	rm -rf /var/lib/cocaine/apps/$app
	rm -rf /var/spool/cocaine/$app
	rm -rf /var/cache/cocaine/apps/$app
done

echo "Deploy New Combainer:"
cocaine-tool upload -m $DEPLOY_DIR/cocaine-app/mastermind.manifest -p $DEPLOY_DIR/cocaine-app/mastermind.tar.gz -n mastermind -c /etc/cocaine/cocaine.conf --verbos

chown cocaine -R /usr/lib/mastermind


/etc/init.d/cocaine-server restart
