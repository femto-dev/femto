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
./configure
make
make check
popd
