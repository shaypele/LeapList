
CC          := gcc-4.8
#Compiled for an intel proccessor with transactions support
CFLAGS      := 	-O3 -g -DINTEL -fomit-frame-pointer -march=x86-64 -fgnu-tm -mrtm
LDFLAGS     :=-fgnu-tm -litm -mrtm


TARGETS    :=  leap_global_lock_multi leap_FG_lock_multi leap_software_trans_multi leap_hardware_trans_multi  

all: $(TARGETS)

clean:
	rm -f $(TARGETS) *~ core *.o *.a

leap_hardware_trans_multi : leap_hardware_trans_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)

leap_global_lock_multi: leap_global_lock_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)

leap_FG_lock_multi: leap_FG_lock_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)

leap_software_trans_multi: leap_software_trans_multi.o set_harness.o ptst.o gc.o trie.o
	$(CC) -o $@ $^ $(LDFLAGS)
	
trie.o: trie.c
	$(CC) $(CFLAGS) -c -o $@ $<


