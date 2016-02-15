EXTRA_DIST = README femto-js CMakeLists.txt

if MOD_FEMTO
if HAS_APXS

AM_CPPFLAGS = -I/usr/include/apr-1 -I/usr/include/apr-1.0 -I$(srcdir)/../main -I`${APXS} -q INCLUDEDIR` -D_LARGEFILE64_SOURCE -D_GNU_SOURCE

pkglib_LTLIBRARIES = libmod_femto.la
libmod_femto_la_SOURCES = mod_femto.c
libmod_femto_la_LIBADD = ../main/libfemto.la ../utils/libfemto_util.la
libmod_femto_la_LDFLAGS = -module

apxs-install:
	${APXS} -i -a -n femto libmod_femto.la

reload:
	apachectl stop
	${APXS} -i -a -n femto libmod_femto.la
	apachectl start

install-js:
	rm -Rf /usr/local/apache2/htdocs/femto-js.old
	mv /usr/local/apache2/htdocs/femto-js /usr/local/apache2/htdocs/femto-js.old
	rsync -av femto-js/ /usr/local/apache2/htdocs/femto-js/

endif
endif
