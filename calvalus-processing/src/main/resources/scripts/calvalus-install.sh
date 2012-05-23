#!/bin/bash

archive=$1
targetdir=$2/`dirname $3`
package=`basename $3`

mkdir -p $targetdir
cd $targetdir

# rename existing directory
if [ -e $package ]
then
  now=`date '+%Y-%m-%dT%H:%M:%S'`
  mv $package $package-$now
fi

tar xf $archive

exit 0
