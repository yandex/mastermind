#! /bin/sh

CURDIR=$PWD

#---------- mastermind code
mkdir -p $CURDIR/debian/tmp
find . -type f -name \*.py -print0 | tar czvf $CURDIR/debian/tmp/mastermind.tar.gz --null -T -
