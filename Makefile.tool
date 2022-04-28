include common.mk

PLUGIN_DIR=tools/*
SUBDIRS := $(wildcard ${PLUGIN_DIR})

.PHONY: force

$(SUBDIRS): force
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir ; \
	done \

all : $(SUBDIRS)

clean:
	for dir in $(SUBDIRS); \
		do $(MAKE) -C $$dir clean ;\
	done \
