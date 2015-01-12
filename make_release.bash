#!/bin/bash

gitrepo=`pwd`
chmod -R u+w ../femto_release
rm -Rf ../femto_release
mkdir ../femto_release
pushd ../femto_release
git clone $gitrepo
cd femto
# instead of using make dist, we use git archive.
#sh autogen.sh
#./configure
##make distcheck does a VPATH/out-of-tree-build not working with RE2
#make dist
VERSION=`head -n 1 ChangeLog | cut -d ' ' -f 1`
ARCHIVE_NAME="femto-$VERSION"
git archive --prefix=$ARCHIVE_NAME/ -o $ARCHIVE_NAME.tar.gz HEAD
mkdir dist-build
cd dist-build
tar xvzf ../femto*.tar.gz
cd femto-*
mkdir build
cd build
cmake ..
if [ $? -ne 0 ]; then
  echo "Cmake failed"
  exit 1
fi
make
if [ $? -ne 0 ]; then
  echo "Cmake make failed"
  exit 1
fi
make check
if [ $? -ne 0 ]; then
  echo "Cmake make check failed"
  exit 1
fi
cd ..
./configure
if [ $? -ne 0 ]; then
  echo "autotools configure failed"
  exit 1
fi
make
if [ $? -ne 0 ]; then
  echo "autotools make failed"
  exit 1
fi
make check
if [ $? -ne 0 ]; then
  echo "autotools make check failed"
  exit 1
fi
popd
