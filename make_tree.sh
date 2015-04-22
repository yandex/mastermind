#! /bin/sh

CURDIR=$PWD

#---------- mastermind code
cp -R $1/src/mastermind $CURDIR/debian/tmp/usr/bin/
cd $1/src/cocaine-app/
find . -type f -iname \*.py -print0 | tar czvf $CURDIR/debian/tmp/usr/lib/mastermind/cocaine-app/mastermind.tar.gz --null -T -
cd $CURDIR
cp $1/src/cocaine-app/*.manifest $CURDIR/debian/tmp/usr/lib/mastermind/cocaine-app/
cp $1/src/cocaine-app/*.profile $CURDIR/debian/tmp/usr/lib/mastermind/cocaine-app/
