#ifndef RWLOCK_H__
#define RWLOCK_H__

#define vwLock unsigned long
#define MAX_ALL_THREADS 4

static volatile vwLock order = 0;
static volatile vwLock sync_vars[MAX_ALL_THREADS] = {0,};

inline int fetch_and_add( int * variable, int value ){
    asm volatile(
            "lock; xaddl %%eax, %2;"
            :"=a" (value)                   //Output
            : "a" (value), "m" (*variable)  //Input
            :"memory" );
    return value;
}

static void OrderedLock_Acquire(vwLock *my_order)
{
        int idx;

	*my_order = fetch_and_add(&order, 1);
	idx = *my_order % MAX_ALL_THREADS;//g_num_of_threads;
	
	while (sync_vars[idx] != *my_order);
}

static void OrderedLock_Release(vwLock *my_order)
{
	int next = *my_order + 1;
	int idx = next % MAX_ALL_THREADS; //g_num_of_threads;
	sync_vars[idx] = next; 
}

static volatile vwLock num_of_readers = 0;
static volatile vwLock read_order = 0;
static volatile vwLock read_locked = 0;
static volatile vwLock transit = 0;
static volatile vwLock num_blocked_in_transit = 0;

static void OrderedRWLock_AcquireWrite(vwLock *my_order)
{
	OrderedLock_Acquire(my_order);
}
static void OrderedRWLock_ReleaseWrite(vwLock *my_order)
{
	OrderedLock_Release(my_order);
}

static void OrderedRWLock_AcquireRead(vwLock *my_order)
{
	if (fetch_and_add(&num_of_readers, 1) == 0)
	{
		OrderedLock_Acquire(my_order);
		read_order = *my_order;
		read_locked = 1;
	} else
	{
		if (transit) 
		{
			fetch_and_add(&num_blocked_in_transit, 1);
			while (transit == 1) {};
			fetch_and_add(&num_blocked_in_transit, -1);
		}
		while (read_locked == 0) {};
	}
	*my_order = read_order;
}

static void OrderedRWLock_ReleaseRead(vwLock *my_order)
{
	if (fetch_and_add(&num_of_readers, -1) == 1)
	{
		transit = 1;
		if (num_of_readers > 1)
		{
			while ((num_blocked_in_transit+1) < num_of_readers) {};
		}
		read_locked = 0;
		transit = 0;
		OrderedLock_Release(my_order);
	}
}

#endif RWLOCK_H__
