# contrib/copy_jsonlines/Makefile

MODULE_big = copy_jsonlines
OBJS = \
	$(WIN32RES) \
	copy_jsonlines.o

EXTENSION = copy_jsonlines
DATA = copy_jsonlines--1.0.sql
PGFILEDESC = "copy_jsonlines - JSON Lines text format support for COPY command"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/copy_jsonlines
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
