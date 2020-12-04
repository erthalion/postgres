# src/bin/pg_undo_dump/Makefile

PGFILEDESC = "pg_undo_dump - decode and display UNDO log"
PGAPPICON=win32

subdir = src/bin/pg_undo_dump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

RMGRDESCSOURCES = smgrdesc.c
RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))

OBJS = \
	$(RMGRDESCOBJS) \
	$(WIN32RES) \
	pg_undo_dump.o \
	rmgrdesc.o

all: pg_undo_dump

pg_undo_dump: $(OBJS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_undo_dump$(X) '$(DESTDIR)$(bindir)/pg_undo_dump$(X)'

$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_undo_dump$(X)'

clean distclean maintainer-clean:
	rm -f pg_undo_dump$(X) $(OBJS) $(RMGRDESCSOURCES)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
