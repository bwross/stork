# Configuration
# =============
# Build information
APPNAME = 'Stork Scheduler'
VERSION = '0.0.2 (still alpha)'

# =============
PROJECT = stork
#PACKAGES = stork stork/scheduler stork/util stork/module stork/stat stork/cred

CLASSPATH = '.:lib/EXTRACTED/:build'
JFLAGS = -J-Xmx512m -g -cp $(CLASSPATH) -verbose -Xlint:unchecked
JC = javac
JAVA = java
JAR = jar -J-Xmx512m

.PHONY: all install clean dist-clean init release classes
.SUFFIXES: .java .class

# Recursive wildcard function from jgc.org.
rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) \
	$(filter $(subst *,%,$2),$d))

#JAVASRCS = $(wildcard $(PACKAGES:%=%/*.java))
JAVASRCS = $(call rwildcard,$(PROJECT),*.java)
CLASSES = $(JAVASRCS:%.java=build/%.class)

TO_BUILD = # Generated by "build/%.class" rule.
JC_CMD = # Set only if we need to compile something.

all: lib/EXTRACTED $(CLASSES) | build
	$(JC_CMD) $(TO_BUILD)
	@$(MAKE) --no-print-directory build/build_tag 
	@$(MAKE) --no-print-directory $(PROJECT).jar 

build:
	mkdir -p build

$(PROJECT).jar: $(CLASSES)
	$(JAR) cf $(PROJECT).jar -C build . -C lib/EXTRACTED .
	cp $(PROJECT).jar bin/

build/%.class: %.java | build
	$(eval TO_BUILD += $<)
	$(eval JC_CMD = $(JC) $(JFLAGS) -d build)

classes: $(TO_BUILD) | build

release: $(PROJECT).tar.gz

src-release: $(PROJECT)-src.tar.gz

$(PROJECT).tar.gz: $(PROJECT).jar
	cp $(PROJECT).jar bin/
	tar czf $(PROJECT).tar.gz bin libexec --exclude='*/CVS' \
		--transform 's,^,$(PROJECT)/,'

$(PROJECT)-src.tar.gz: dist-clean
	tar czf $(PROJECT)-src.tar.gz * --exclude='*/CVS'

# FIXME: This is a bad hack.
lib/EXTRACTED:
	cd lib && ./extract.sh

build/build_tag: $(CLASSES) | build
	@echo generating build tag
	@echo appname = $(APPNAME) >  build/build_tag
	@echo version = $(VERSION) >> build/build_tag
	@echo buildtime = `date`   >> build/build_tag

clean:
	$(RM) -rf build $(PROJECT).jar $(PROJECT).tar.gz

dist-clean: clean
	$(RM) -rf lib/EXTRACTED
