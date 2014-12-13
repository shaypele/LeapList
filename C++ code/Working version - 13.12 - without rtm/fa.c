#include <stdio.h>
#include <immintrin.h>

void main(void)
{
	int x = 0;
	
	if (_xbegin() == -1) {
		x = 1;
		_xabort();
		_xend();
	} else {
		printf("transaction failed\n");
	}
	
	printf("value of x: %d\n", x);
}
