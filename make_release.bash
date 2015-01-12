#!/bin/bash

gitrepo=`pwd`
chmod -R u+w ../femto_release
rm -Rf ../femto_release
mkdir ../femto_release
pushd ../femto_release
git clone $gitrepo
cd femto
sh autogen.sh
./configure
#make distcheck does a VPATH/out-of-tree-build not working with RE2
make dist
mkdir dist-build
cd dist-build
tar xvzf ../femto*.tar.gz
cd femto-*
mkdir build
cmake ..
if [ $? -ne 0 ]; then
  echo "Cmake failed"
fi
make
if [ $? -ne 0 ]; then
  echo "Cmake make failed"
fi
make check
if [ $? -ne 0 ]; then
  echo "Cmake make check failed"
fi
cd ..
./configure
if [ $? -ne 0 ]; then
  echo "autotools configure failed"
fi
make
if [ $? -ne 0 ]; then
  echo "autotools make failed"
fi
make check
if [ $? -ne 0 ]; then
  echo "autotools make check failed"
fi
popd
