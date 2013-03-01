#!/bin/sh
echo Deleting possible symbolic links
rm -f INSTALL depcomp install-sh missing mkinstalldirs ylwrap libtool ltmain.sh config.guess config.h config.sub configure aclocal.m4
echo Running autoreconf --force --install
autoreconf --force --install
automake --add-missing
echo You now need to run configure
