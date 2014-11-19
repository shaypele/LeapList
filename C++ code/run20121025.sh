#/bin/bash

./run_script_threads.pl pollux_TestThreadsWithTrie_node300_40lookup_40rq 40 40
./run_script_sizes.pl pollux_TestSizesWithTrie_node300_80threads_40lookup_40rq 40 40

./run_script_lookup_update.pl pollux_TestLookupWithTrie_node300_80threads 80
./run_script_rq_update.pl pollux_TestRQWithTrie_node300_80threads 80




