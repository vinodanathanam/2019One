#include <iostream>
#include <thread>
#include <fstream>
#include <list>
#include <vector>
#include <algorithm>
#include <mutex> 
#include <condition_variable>
#include <chrono>


using namespace std;


//defaults
const int THREADCOUNT = 10;
const int BUCKETSIZE = 1000;
const int NUMBUCKETS = 10;

int g_bucketSize = BUCKETSIZE;
int g_threadCount = THREADCOUNT;
int g_numBuckets = NUMBUCKETS;

list<int> g_ReadQ;
list<int> g_ProcQ;
vector<int> g_listInput[NUMBUCKETS];
bool g_bfileEOF = false;


std::mutex _mtxPrint;

std::mutex _mtxRead;
std::mutex _mtxProcess;
std::condition_variable _cv;

std::mutex _mtxData;
long g_total = 0;


void readNumberProc()
{

	//load lookup numbers
	for (int j = 0; j < g_numBuckets ; j++)
		g_ReadQ.push_back(j);

	ifstream myfile ("numbers.txt");
	if (myfile.is_open())
	{
		bool indexValid = false;
        int index = -1;
        int counter = 0;

		string line;
		while ( getline (myfile,line) )
		{
			if (false == indexValid)
			{
				std::lock_guard<std::mutex> lck (_mtxRead);
				if (g_ReadQ.empty() == false)
				{
					counter = 0;
					//index = -1;
					index = g_ReadQ.front();
					g_ReadQ.pop_front();
					g_listInput[index].clear();
					indexValid = true;
				}
			}

			if (true == indexValid)
			{
				g_listInput[index].push_back((int)atoi(line.c_str()));

				counter++;
			}

			if (counter == g_bucketSize)
			{
				unique_lock<std::mutex> lock(_mtxProcess);
				g_ProcQ.push_back(index);

				indexValid = false;
				index = -1;
				counter = 0;
				_cv.notify_one();
			}

		}
		//In case of not full last bucket.Also push -1 as the last element
		{
			unique_lock<std::mutex> lock(_mtxProcess);
			if(index != -1)
				g_ProcQ.push_back(index);

			g_ProcQ.push_back(-1);
			_cv.notify_all();
		}

		lock_guard<std::mutex> l(_mtxPrint);
		cout << "End of file ..........." << endl;

    	myfile.close();
  	}
}

void emptyBucket(int index)
{
	for(auto i = g_listInput[index].begin(); i != g_listInput[index].end(); i++)
	{
		lock_guard<std::mutex> ld(_mtxData);
		g_total++;
		//lock_guard<std::mutex> l(_mtxPrint);
		//cout << "Bucket : " << index << " Value : " << *i << endl;
	}

	//add the index to the read list.
	lock_guard<std::mutex> lock(_mtxRead);
	g_listInput[index].clear();
	g_ReadQ.push_back(index);
	index = -1;
}

void ProcessNumbers()
{
	bool loop = true;

	std::thread threads[g_threadCount];

	int count(0);
	while(loop)
	{
		unique_lock<std::mutex> lock(_mtxProcess);
		_cv.wait(lock, [](){ return !g_ProcQ.empty(); });

		int item = g_ProcQ.front();
		g_ProcQ.pop_front();	
		if(-1 == item)
		{
			loop = false;
			break;
		}
		
		threads[count++] = std::thread(emptyBucket, item);

		//std::this_thread::sleep_for(std::chrono::milliseconds(10));

		if(count == 10)
		{
			for(int id = 0; id < count; id++)
			{
				if(threads[id].joinable())
				{
					threads[id].join();
				}
			}
			
			count = 0;	
			//cout << "Cleared....................." << endl;
		}
		
	}

	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	//wait for remaining threads	
	for(int id = 0; id < count; id++)
	{
		if(threads[id].joinable())
			threads[id].join();
	}
	

}

 
int main(int argc, char* argv[])  
{
  
   	g_threadCount = THREADCOUNT;
	g_bucketSize = BUCKETSIZE;

	if (argc == 2)
	{
		g_bucketSize = atoi(argv[1]);
	}

	if (argc == 3)
	{
		g_bucketSize = atoi(argv[1]);
		g_numBuckets = g_threadCount =  atoi(argv[2]);
	}

	std::thread threadObj(readNumberProc);

	ProcessNumbers();

	threadObj.join();    
    std::cout<<"Exit of Main function : "<< g_total << std::endl;
    return 0;
}
