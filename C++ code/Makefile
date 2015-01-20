
ARCH      := INTEL

ifeq ($(ARCH),INTEL)
CC          := gcc-4.8
CFLAGS      := 	-O3 -g -DINTEL -fomit-frame-pointer -march=x86-64 -fgnu-tm -mrtm
LDFLAGS     :=-fgnu-tm -litm -mrtm
endif

ifeq ($(ARCH),PPC)
CC          := cc_r
CFLAGS      := -O3 -DPPC -q64 -w
LDFLAGS     := -lpthread -q64
ASFLAGS     := -a64
endif

ifeq ($(ARCH),IA64)
CC          := gcc
CFLAGS      := -O3 -DIA64 -fomit-frame-pointer
LDFLAGS     := -lpthread
endif

ifeq ($(ARCH),MIPS)
CC          := gcc
CFLAGS      := -O3 -DMIPS -fomit-frame-pointer
LDFLAGS     := -lpthread
endif
ifeq ($(ARCH),SPARC)
CC          := /opt/SUNWspro/bin/cc
CFLAGS      := -g -xO1 -DSPARC=1 sparc_mcas.il  -m32 -mt
LDFLAGS     := -DSPARC sparc_mcas.il -xarch=native -lrt
endif


ifeq ($(ARCH),SPARC___)
CC          := /opt/SUNWspro/bin/cc
CFLAGS      := -xO3 -DSPARC sparc_mcas.il -xarch=v9b
LDFLAGS     := -DSPARC sparc_mcas.il -xarch=v9b -lthread -lrt
endif

ifeq ($(ARCH),ALPHA)
CC          := cc
CFLAGS      := -accept vaxc_keywords -O3 -DALPHA
CFLAGS      += -fomit-frame-pointer -DWEAK_MEM_ORDER
LDFLAGS     := -lpthread 
endif

TARGETS    :=  leap_FG_lock_multi leap_rtm_multi leap_global_lock_multi leap_tm_lock_multi

all: $(TARGETS) replay

clean:
	rm -f $(TARGETS) *~ core *.o *.a

leap_rtm_multi : leap_rtm_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS) -lpthread

leap_global_lock_multi: leap_global_lock_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)

leap_FG_lock_multi: leap_FG_lock_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)

leap_tm_lock_multi: leap_tm_lock_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)
	
trie.o: trie.c
	$(CC) $(CFLAGS) -c -o $@ $<


