# src/test/modules/toytable/Makefile

MODULE_big = toytable
OBJS = toytableam.o $(WIN32RES)
PGFILEDESC = "A dummy implementantation of the table AM API"

EXTENSION = toytable
DATA = toytable--1.0.sql

REGRESS = toytable

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/toytable
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

OBJS = toytableam.o

include $(top_srcdir)/src/backend/common.mk
