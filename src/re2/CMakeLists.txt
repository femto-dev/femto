
add_custom_target(
   make_re2
   COMMAND make && cp obj/libre2.a ${CMAKE_BINARY_DIR}/src/re2
   WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/re2/re2
)


add_library(re2 STATIC IMPORTED)
#set(RE2_LIB_DIR ${CMAKE_SOURCE_DIR}/src/re2/re2/obj/ PARENT_SCOPE)
#set(RE2_LIB ${CMAKE_SOURCE_DIR}/src/re2/re2/obj/libre2.a PARENT_SCOPE)
set(RE2_LIB  ${CMAKE_BINARY_DIR}/src/re2/libre2.a PARENT_SCOPE)
#configure_file(${CMAKE_SOURCE_DIR}/src/re2/re2/obj/libre2.a  libre2.a COPYONLY)

# Doesn't seem to work.
set_target_properties(re2 PROPERTIES IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/src/re2)
#/libre2.a)
#set_target_properties(re2 PROPERTIES IMPORTED_LOCATION (${CMAKE_BINARY_DIR}/res/ )

add_dependencies(re2 make_re2)

#install(TARGETS re2 ${INSTALL_TARGETS_DEFAULT_ARGS})

#
#femto_search_LDADD = $(srcdir)/../re2/re2/obj/libre2.a $(LDADD)
#add_library(re2 IMPORTED)
#target_link_libraries(re2 re2/obj/libre2.a)
#add_dependencies(re2 make_re2)


########### install files ###############




#original Makefile.am contents follow:

#all-local:
#	cd re2 && $(MAKE) $(AM_MAKEFLAGS) all
#check-local:
#	cd re2 && $(MAKE) $(AM_MAKEFLAGS) test
#clean-local:
#	cd re2 && $(MAKE) $(AM_MAKEFLAGS) clean
#
## Assuming the package knows how to install itself
##install-data-local:
##	cd re2 && $(MAKE) $(AM_MAKEFLAGS) install-data
##install-exec-local:
##	cd re2 && $(MAKE) $(AM_MAKEFLAGS) install-exec
##uninstall-local:
##	cd re2 && $(MAKE) $(AM_MAKEFLAGS) uninstall
#
## Distribute files from here.
#EXTRA_DIST = re2
#
