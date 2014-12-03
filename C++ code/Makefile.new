
ARCH      := INTEL
DEBUGGING := -DNDEBUG

ifeq ($(ARCH),INTEL)
#CC          := gcc
#CC          := gcc-4.8#/home/pollux/home/hillelav/leaplist/gcc-4.7/install/bin/gcc
#CC          := gcc
CC           := g++
#CC           := /home/adi/gcc4.8/install/bin/gcc

CFLAGS      := -O3 -m64 -DNDEBUG -DINTEL64 -D_REENTRANT -lrt -pthread 

LDFLAGS     := -O3 -m64 -DNDEBUG -DINTEL64 -D_REENTRANT -lrt -pthread
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

CFLAGS      += $(DEBUGGING)
COMMON_DEPS += Makefile $(wildcard *.h)

GC_HARNESS_TARGETS := skip_lock_perlist skip_lock_pernode skip_lock_perpointer
GC_HARNESS_TARGETS += skip_cas skip_mcas

GC_HARNESS_TARGETS += bst_lock_fraser bst_lock_manber bst_lock_kung
GC_HARNESS_TARGETS += bst_mcas

GC_HARNESS_TARGETS += rb_lock_concurrentwriters rb_lock_serialisedwriters
GC_HARNESS_TARGETS += rb_lock_mutex

TARGETS    := $(GC_HARNESS_TARGETS)
TARGETS    += rb_stm_fraser rb_stm_herlihy rb_stm_lock
TARGETS    += trie_test skip_stm_fraser skip_stm_herlihy skip_stm_lock leap_stm leap_no_stm skip_stm_gcc leap_light leap_tm_lock_multi leap_rwlock_multi leap_tm_lock leap_stm_multi
GC_HARNESS_TARGETS += leap_cas

#TARGETS    := leap_tm_lock_multi leap_rwlock_multi leap_stm_multi leap_cop_multi 

all: $(TARGETS) replay

clean:
	rm -f $(TARGETS) replay *~ core *.o *.a

replay: %: %.c $(COMMON_DEPS)
	$(CC) $(CFLAGS) -c -o $(patsubst %.c,%.o,$<) $<
	$(CC) -o $@ $(patsubst %.c,%.o,$<) $(LDFLAGS)

#tree_mcas.o: tree_mcas.c mcas.c $(COMMON_DEPS)
#	$(CC) $(CFLAGS) -c -o $@ $<
#skip_lock_perpointer.o: skip_lock.c $(COMMON_DEPS)
#	$(CC) $(CFLAGS) -DTINY_MTX -c -o $@ $<
#skip_lock_pernode.o: skip_lock.c $(COMMON_DEPS)
#	$(CC) $(CFLAGS) -c -o $@ $<
#skip_lock_perlist.o: skip_lock.c $(COMMON_DEPS)
#	$(CC) $(CFLAGS) -DFAT_MTX -c -o $@ $<
#skip_mcas.o: skip_mcas.c mcas.c $(COMMON_DEPS)
#	$(CC) $(CFLAGS) -c -o $@ $<

%.o: %.c $(COMMON_DEPS)
	$(CC) $(CFLAGS) -c -o $@ $<

#skip_stm_lock: skip_stm.o stm_lock.o set_harness.o ptst.o gc.o
#	$(CC) -o $@ $^ $(LDFLAGS)
#skip_stm_fraser: skip_stm.o stm_fraser.o set_harness.o ptst.o gc.o
#	$(CC) -o $@ $^ $(LDFLAGS)
#skip_stm_herlihy: skip_stm.o stm_herlihy.o set_harness.o ptst.o gc.o
#	$(CC) -o $@ $^ $(LDFLAGS)

#leap_stm: leap_stm.o stm_herlihy.o set_harness.o ptst.o gc.o trie.o
#	$(CC) -o $@ $^ $(LDFLAGS)

leap_stm_multi: leap_stm_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)

leap_rwlock_multi: leap_rwlock_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)

leap_tm_lock_multi: leap_tm_lock_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)

leap_cop_multi: leap_cop_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)

#leap_tm_lock: leap_tm_lock.o stm_herlihy.o set_harness.o ptst.o gc.o trie.o
#	$(CC) -o $@ $^ $(LDFLAGS)

#leap_light: leap_light.o stm_herlihy.o set_harness.o ptst.o gc.o trie.o
#	$(CC) -o $@ $^ $(LDFLAGS)

#leap_no_stm: leap_no_stm.o stm_herlihy.o set_harness.o ptst.o gc.o trie.o
#	$(CC) -o $@ $^ $(LDFLAGS)

#skip_stm_gcc: skip_stm_gcc.o set_harness.o ptst.o gc.o
#	$(CC) -o $@ $^ $(LDFLAGS)

#rb_stm_lock: rb_stm.o stm_lock.o set_harness.o ptst.o gc.o
#	$(CC) -o $@ $^ $(LDFLAGS)
#rb_stm_fraser: rb_stm.o stm_fraser.o set_harness.o ptst.o gc.o
#	$(CC) -o $@ $^ $(LDFLAGS)
#rb_stm_herlihy: rb_stm.o stm_herlihy.o set_harness.o ptst.o gc.o
#	$(CC) -o $@ $^ $(LDFLAGS)

$(GC_HARNESS_TARGETS): %: %.o set_harness.o ptst.o gc.o
	$(CC) -o $@ $^ $(LDFLAGS)

trie.o: trie.c
	$(CC) $(CFLAGS) -c -o $@ $<

trie_test : set_harness.o trie.o ptst.o gc.o trie_test.c
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)


test_gcc_stm_perf : test_gcc_stm_perf.c
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)


test_gcc_stm : test_gcc_stm.c
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

test_gcc_stm_adi : test_gcc_stm_adi.c
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

test_gcc_stm_adi2 : test_gcc_stm_adi2.c
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)



