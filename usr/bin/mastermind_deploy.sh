#! /bin/bash

DEPLOY_DIR="/usr/lib/mastermind"

APP_NAME=$1

echo "Clean old version of Mastermind:"

for app in 'mastermind';
do 
	rm -rf /var/lib/cocaine/apps/$app
	rm -rf /var/spool/cocaine/$app
	rm -rf /var/cache/cocaine/apps/$app
	rm -f /var/cache/cocaine/manifests/$app
done

echo "Deploy New Mastermind:"
cocaine-tool app upload --manifest $DEPLOY_DIR/cocaine-app/mastermind.manifest --package $DEPLOY_DIR/cocaine-app/mastermind.tar.gz -n $APP_NAME
cocaine-tool profile upload -n $APP_NAME --profile $DEPLOY_DIR/cocaine-app/mastermind.profile
cocaine-tool runlist add-app -n default --app $APP_NAME --profile mastermind --force

mkdir /var/log/mastermind
chown cocaine -R /usr/lib/mastermind
chown cocaine -R /var/log/mastermind


/etc/init.d/cocaine-runtime restart
