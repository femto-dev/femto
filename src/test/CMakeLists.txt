configure_file(test.pl test.pl COPYONLY)
add_test(test.pl test.pl)



#original Makefile.am contents follow:

#check_SCRIPTS = test.pl
#
#EXTRA_DIST = test.pl
#
#TESTS = $(check_SCRIPTS)
#
#clean-local:
#	rm -Rf testdir
#
