#pragma once

class LeapSet
{
public:
	 long key;
	 void* value;

	LeapSet(void);
	LeapSet (long key, void* value);
	~LeapSet(void);
};
