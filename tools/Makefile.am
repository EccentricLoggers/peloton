## Makefile.am -- Process this file with automake to produce Makefile.in

## Common flags
include $(top_srcdir)/common.am

######################################################################
# SUBDIRS
######################################################################

bin_PROGRAMS = initdb pg_ctl psql \
			   createdb createuser dropdb dropuser

pg_common_cxxflags = $(AM_CXXFLAGS) \
					 -Wno-write-strings -Wno-sign-compare \
					 -Wno-unused-parameter -Wno-missing-field-initializers \
					 -Wno-array-bounds -Wno-clobbered -Wno-strict-overflow

pg_common_ldadd = $(top_builddir)/src/libpelotonpg.la \
				  $(top_builddir)/src/libpeloton.la

pg_common_ldflags =	\
					-L$(top_builddir)/src/.libs	-lpq

######################################################################
# INITDB
######################################################################

initdb_SOURCES = \
				 common/fe_memutils.cpp \
				 pg_initdb/findtimezone.cpp \
				 pg_initdb/initdb.cpp

initdb_CPPFLAGS = \
				  -DFRONTEND \
				  -I$(top_srcdir)/src/postgres/include \
				  -I$(top_srcdir)/src/postgres/timezone

initdb_LDFLAGS = $(pg_common_ldflags)
initdb_LDADD = $(pg_common_ldadd)
initdb_CXXFLAGS = $(pg_common_cxxflags)

######################################################################
# PSQL
######################################################################

psql_SOURCES = \
			   common/dumputils.cpp \
			   common/fe_memutils.cpp \
			   common/keywords.cpp \
			   pg_psql/command.cpp \
			   pg_psql/common.cpp \
			   pg_psql/copy.cpp \
			   pg_psql/describe.cpp \
			   pg_psql/help.cpp \
			   pg_psql/input.cpp \
			   pg_psql/large_obj.cpp \
			   pg_psql/mainloop.cpp \
			   pg_psql/mbprint.cpp \
			   pg_psql/print.cpp \
			   pg_psql/prompt.cpp \
			   pg_psql/sql_help.cpp \
			   pg_psql/startup.cpp \
			   pg_psql/stringutils.cpp \
			   pg_psql/tab-complete.cpp \
			   pg_psql/variables.cpp

psql_CPPFLAGS = \
				-DFRONTEND \
				-I. \
				-I$(top_srcdir)/tools/common \
				-I$(top_srcdir)/src/postgres/include \
				-I$(top_srcdir)/src/postgres/interfaces/libpq

psql_LDFLAGS = $(pg_common_ldflags)
psql_LDADD = $(pg_common_ldadd)
psql_CXXFLAGS = $(pg_common_cxxflags)

######################################################################
# PSQL & SCRIPTS
######################################################################

pg_scripts_CPPFLAGS = \
					  -DFRONTEND \
					  -I$(top_srcdir)/tools/common \
					  -I$(top_srcdir)/src/postgres/include \
					  -I$(top_srcdir)/src/postgres/interfaces/libpq

pg_scripts_SOURCES = \
					 common/dumputils.cpp \
					 common/fe_memutils.cpp \
					 common/keywords.cpp

pg_scripts_CXXFLAGS = $(pg_common_cxxflags)


pg_ctl_SOURCES = \
				 $(pg_scripts_SOURCES) \
				 scripts/pg_ctl.cpp

pg_ctl_CPPFLAGS = $(pg_scripts_CPPFLAGS)

pg_ctl_LDFLAGS = $(pg_common_ldflags)
pg_ctl_LDADD = $(pg_common_ldadd)
pg_ctl_CXXFLAGS = $(pg_common_cxxflags)


createdb_SOURCES = \
				   $(pg_scripts_SOURCES) \
				   scripts/common.cpp \
				   scripts/createdb.cpp

createdb_CPPFLAGS = $(pg_scripts_CPPFLAGS)

createdb_LDFLAGS = $(pg_common_ldflags)
createdb_LDADD = $(pg_common_ldadd)
createdb_CXXFLAGS = $(pg_common_cxxflags)


createuser_SOURCES = \
					 $(pg_scripts_SOURCES) \
					 scripts/common.cpp \
					 scripts/createuser.cpp

createuser_CPPFLAGS = $(pg_scripts_CPPFLAGS)

createuser_LDFLAGS = $(pg_common_ldflags)
createuser_LDADD = $(pg_common_ldadd)
createuser_CXXFLAGS = $(pg_common_cxxflags)


dropdb_SOURCES = \
				 $(pg_scripts_SOURCES) \
				 scripts/common.cpp \
				 scripts/dropdb.cpp

dropdb_CPPFLAGS = $(pg_scripts_CPPFLAGS)

dropdb_LDFLAGS = $(pg_common_ldflags)
dropdb_LDADD = $(pg_common_ldadd)
dropdb_CXXFLAGS = $(pg_common_cxxflags)


dropuser_SOURCES = \
				   $(pg_scripts_SOURCES) \
				   scripts/common.cpp \
				   scripts/dropuser.cpp

dropuser_CPPFLAGS = $(pg_scripts_CPPFLAGS)

dropuser_LDFLAGS = $(pg_common_ldflags)
dropuser_LDADD = $(pg_common_ldadd)
dropuser_CXXFLAGS = $(pg_common_cxxflags)

######################################################################
# PELOTON CONFIGURATION
######################################################################

install-data-hook:
	cp -rf $(top_srcdir)/tools/share/. $(datadir)
