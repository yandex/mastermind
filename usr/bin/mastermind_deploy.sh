#! /bin/bash

DEPLOY_DIR="/usr/lib/mastermind"

echo "Clean old version of combainer:"

for app in 'mastermind';
do 
	rm -rf /var/lib/cocaine/apps/$app
	rm -rf /var/spool/cocaine/$app
	rm -rf /var/cache/cocaine/apps/$app
done

echo "Deploy New Combainer:"
cocaine-tool app upload --manifest $DEPLOY_DIR/cocaine-app/mastermind.manifest --package $DEPLOY_DIR/cocaine-app/mastermind.tar.gz -n mastermind

mkdir /var/log/mastermind
chown cocaine -R /usr/lib/mastermind
chown cocaine -R /var/log/mastermind


/etc/init.d/cocaine-runtime restart
