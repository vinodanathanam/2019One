#include <iostream>
#include <thread>
#include <fstream>
#include <list>
#include <vector>
#include <algorithm>
#include <mutex> 
#include <condition_variable>
#include <chrono>
#include <map>
#include <numeric>
#include <functional>
#include <boost/algorithm/string.hpp>

using namespace std;


//defaults
const int THREADCOUNT = 20;
const int BUCKETSIZE = 1000;
const int NUMBUCKETS = 20;

int g_bucketSize = BUCKETSIZE;
int g_threadCount = THREADCOUNT;

list<int> g_ReadQ;
list<int> g_ProcQ;
vector<int> g_listInput[NUMBUCKETS];


std::mutex _mtxPrint;
std::mutex _mtxRead;
std::mutex _mtxProcess;
std::condition_variable _cv;

std::mutex _mtxData;
long g_total = 0;


typedef map<int, map<int, long> > _tdataStruct;
typedef map<int, map<int, long> >::iterator mainItr;
typedef map<int, long>::iterator subItr;

_tdataStruct g_DS;


void storeRecord(string& s)
{
	std::vector<std::string> results;
	boost::algorithm::split(results, s, boost::is_any_of(" ,\n"));

	if(results.size() != 2)
		return;

	int key = atoi(results[0].c_str());
	int val = atoi(results[1].c_str());
	mainItr itr = g_DS.find(key);

	if (itr != g_DS.end())
	{
		subItr it = itr->second.find(val);
		if (it != itr->second.end())
		{
			it->second = it->second + 1;
		}
		else
		{
			itr->second.insert(make_pair(val, 1));
		}
	}
	else
	{
		map<int, long> temp;
		temp.insert(make_pair(val, 1));
		g_DS.insert(make_pair(key, temp));
	}

}

void loadRecords()
{
	ifstream ifile("extents.txt");
	if(ifile.is_open())
	{
		string line;
		while( getline( ifile, line))
		{
			storeRecord(line);
		}

		ifile.close();
	}	
}

long countall(int val, const std::pair<int, long>& x)
{
	return val + x.second;
}

long getCount(const int& num)
{
	long count(0);

	for (mainItr itr = g_DS.begin(); itr != g_DS.end() && itr->first <= num; itr++) //check if the start is less than num
	{
		if (num <= itr->second.rbegin()->first) // check if the end is > num
		{
			subItr it = itr->second.lower_bound(num); //start from the num
			if (it != itr->second.end())
			{
				count = accumulate(it, itr->second.end(), count, countall);
			}
		}
	}
	return count;
}

void readNumberProc()
{

	//load lookup numbers
	for (int j = 0; j < NUMBUCKETS; j++)
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
		long count = getCount(*i);
		lock_guard<std::mutex> l(_mtxPrint);
		cout << "Bucket : " << index << " Value : " << *i << " Count : "<< count << endl;
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

		if(count == g_threadCount)
		{
			for(int id = 0; id < count; id++)
			{
				if(threads[id].joinable())
				{
					threads[id].join();
				}
			}
			
			count = 0;	
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

void cleanup()
{
	if (!g_DS.empty())
	{
		for (mainItr it = g_DS.begin(); it != g_DS.end(); it++)
		{
			it->second.clear();
		}
		g_DS.clear();
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
		g_threadCount =  atoi(argv[2]);
	}

	loadRecords();

	std::thread threadObj(readNumberProc);

	ProcessNumbers();

	threadObj.join();

	cleanup();

    std::cout<<"Exit of Main function : "<< g_total << std::endl;
    return 0;
}
