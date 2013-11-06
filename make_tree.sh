#! /bin/sh

CURDIR=$PWD

#---------- mastermind code
cp -R $1/src/mastermind $CURDIR/debian/tmp/usr/bin/
cd $1/src/cocaine-app/
tar cvf $CURDIR/debian/tmp/usr/lib/mastermind/cocaine-app/mastermind.tar.gz *.py cache_transport/*.py
cd $CURDIR
cp $1/src/cocaine-app/*manifest $CURDIR/debian/tmp/usr/lib/mastermind/cocaine-app/
cp $1/src/cocaine-app/mastermind.profile $CURDIR/debian/tmp/usr/lib/mastermind/cocaine-app/
