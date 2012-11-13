#! /bin/sh

CURDIR=$PWD

#---------- mastermind code
cp -R $1/src/mastermind $CURDIR/debian/mastermind/usr/bin/
cd $1/src/cocaine-app/
tar cvf $CURDIR/debian/mastermind/usr/lib/mastermind/cocaine-app/mastermind.tar.gz *.py 
cd $CURDIR
cp $1/src/cocaine-app/*manifest $CURDIR/debian/mastermind/usr/lib/mastermind/cocaine-app/
