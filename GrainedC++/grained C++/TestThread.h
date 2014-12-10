#pragma once
#include "LeapListDB.h"

class TestThread : public  CCP::Thread
{
public:

	LeapListDB* db ;
	int funcToRun;
	long* arrKeys;
	int arrStart;
	int arrEnd;
	int size;
	void insert1();
	void insert2();
	void insert3();
	void run();
	void getRQ();
	void removeRand();
	void insertRand();

	TestThread(LeapListDB* db, int funcToRun, long* arrKeys, int arrStart,int arrEnd,int size);
	~TestThread(void);
};
