// GenerateNumbers.cpp : Defines the entry point for the console application.
//

#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
#include <string>
#include <fstream>
#include <stdio.h>

using namespace std;
void RangedRand(int range_min, int range_max, long n);
void Numbers(int range_min, int range_max, long n);

int main(int argc, char* argv[])
{
	if (3 == argc)
	{
		int a = atoi(argv[1]);
		long b = atol(argv[2]);
		if (1 == a)
		{
			printf("extents.txt.... \n");
			RangedRand(0, 1000, b);
		}
		else if (2 == a)
		{
			printf("numbers.txt.... \n");
			Numbers(0, 1000, b);
		}
	}

    return 0;
}

void Numbers(int range_min, int range_max, long n)
{
	ofstream file;
	file.open("numbers.txt");

	srand((unsigned)time(NULL));

	long i;
	for (i = 0; i < n; i++)
	{
		int x = (int)rand() % (range_max - range_min) + range_min;

		char str[10];
		if (i == 0)
			sprintf(str, "%d", x);
		else
			sprintf(str, "\n%d", x);

		file << str;
	}
	file.close();
}

void RangedRand(int range_min, int range_max, long n)
{
	// Generate random numbers in the half-closed interval
	// [range_min, range_max). In other words,
	// range_min <= random number < range_max
	ofstream ofile;
	ofile.open("extents.txt");

	srand((unsigned)time(NULL));

	long i;
	for (i = 0; i < n; i++)
	{
		
		int x = (int)rand() % (range_max - range_min)	+ range_min;

		int y = (int)rand() % (range_max - range_min) 	+ range_min;

		char str[10];
		if (x < y)
		{
			if (i == 0)
				sprintf(str,"%d %d", x, y);
			else
				sprintf(str,"\n%d %d", x, y);
		}
		else
		{
			if (i == 0)
				sprintf(str,"%d %d", y, x);
			else
				sprintf(str,"\n%d %d", y, x);
		}
		ofile << str;
	}
	ofile.close();
}
