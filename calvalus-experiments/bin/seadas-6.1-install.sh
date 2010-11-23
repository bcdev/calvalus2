#!/bin/bash

archive=$1
targetdir=$2
package=$3

mkdir -p $targetdir
cd $targetdir
tar xf $archive

exit 0
