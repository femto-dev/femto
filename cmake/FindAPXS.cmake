
if(NOT DEFINED APACHE_MODULE_DIR)
   find_program(APXS_BIN NAMES apxs apxs2
             PATH_SUFFIXES httpd apache apache2
   )

   if(APXS_BIN)
      EXEC_PROGRAM(${APXS_BIN}
         ARGS -q LIBEXECDIR
         OUTPUT_VARIABLE APACHE_MODULE_DIR )
   endif(APXS_BIN)
endif(NOT DEFINED APACHE_MODULE_DIR)

