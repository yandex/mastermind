#! /bin/bash

DEPLOY_DIR=$1
APP_NAME=$2
MANIFEST=$3
PROFILE=$4

echo "Cleaning old version of application $APP_NAME..."

rm -rf /var/lib/cocaine/apps/$APP_NAME
rm -f /var/lib/cocaine/manifests/$APP_NAME
rm -rf /var/spool/cocaine/$APP_NAME
rm -rf /var/cache/cocaine/apps/$APP_NAME
rm -f /var/cache/cocaine/manifests/$APP_NAME

echo "Deploying new application $APP_NAME"
cocaine-tool app upload --manifest $DEPLOY_DIR/cocaine-app/$MANIFEST --package $DEPLOY_DIR/cocaine-app/mastermind.tar.gz -n $APP_NAME
/usr/bin/mastermind_upload_profile $APP_NAME --fallback-profile $DEPLOY_DIR/cocaine-app/$PROFILE
