#include <map>
#include <set>
#include <list>
#include <cmath>
#include <ctime>
#include <deque>
#include <queue>
#include <stack>
#include <string>
#include <bitset>
#include <cstdio>
#include <limits>
#include <vector>
#include <climits>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <numeric>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <mpi.h>
#include <chrono>

using namespace std;

void Multiply_serial(float *A, float *B, float *C, int m, int n, int p)//mn*np=mp
{
	for (int i = 0; i < m; i++)
	{
		for (int j = 0; j < p; j++)
		{
			C[i*p + j] = 0;
			for (int k = 0; k < n; k++)
				C[i*p + j] += A[i*n + k] * B[k*p + j];
		}
	}
}

int IsEqual(float *A, float *B, int m, int n)
{
	for (int i = 0; i < m; i++)
	{
		for (int j = 0; j < n; j++)
		{
			if(A[i*n + j] !=B[i*n + j])
				return 0;
		}
	}
	return 1;
}

int main(int argc, char **argv) 
{

	int processes,rank;

	int vars[2];

	float *A;//n*m
	float *B;//m*n
	float *C;//n*n
	float *C_serial;//n*n
	int n;
	int m;
	n=stoi(argv[1]);
	m=32;

	A=new float[n*m];//n*m
	B=new float[m*n];//m*n
	C=new float[n*n];//n*n
	C_serial=new float[n*n];//n*n
	bool sparse=false;
	MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &processes);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    std::chrono::time_point<std::chrono::system_clock> begin = std::chrono::system_clock::now();
	if(rank==0)
	{
		vars[0]=n;
		vars[1]=m;
		for (int i = 0; i < n; i++)
		{  
			for (int j = 0; j < m; j++)
			{
				float sparse1=1.0;
				float sparse2=1.0;
				if(sparse){
					float r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
					if(r>0.2)
						sparse1=0.0;
					r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
					if(r>0.2)
						sparse2=0.0;
				}
				A[i*m+j] = (float (rand()%10)+0.1)*sparse1;
				B[j+i*m] = (float (rand()%10)+0.1)*sparse2;
			}
		}
	
	}

	MPI_Bcast(&vars, 2, MPI_INT, 0, MPI_COMM_WORLD);	\
	MPI_Bcast(A, vars[0]*vars[1] , MPI_FLOAT, 0, MPI_COMM_WORLD);	\
	MPI_Bcast(B, vars[1]*vars[0] , MPI_FLOAT, 0, MPI_COMM_WORLD);	

	int startrow = rank * ( vars[0] / processes);
	int endrow = ((rank + 1) * ( vars[0] / processes));

	float *C_temp=new float[vars[0]*vars[0]/processes];// n*n/processes

	int l=0;
	std::chrono::time_point<std::chrono::system_clock> multiply = std::chrono::system_clock::now();
    for (int i = startrow; i < endrow; i++) 
    {
        for (int j = 0; j < vars[0]; j++) 
        {
        	C_temp[l]=0;
            for (int k = 0; k < vars[1]; k++) 
            {
                C_temp[l] += A[ (i*vars[1] + k) ] * B[ (k*vars[0] + j) ];
            }
            l++;
        }
    }
	cout<<"mult"<<rank<<" "<< (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - multiply)).count()<<endl;
    MPI_Gather(C_temp, vars[0]*vars[0]/processes, MPI_FLOAT, C, vars[0]*vars[0]/processes,  MPI_FLOAT, 0, MPI_COMM_WORLD);
    cout<<"parallel_time:"<<rank<<" "<< (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - begin)).count()<<endl;
    if(rank==0)
    {
    	std::chrono::time_point<std::chrono::system_clock> begin = std::chrono::system_clock::now();
    	Multiply_serial(A,B,C_serial,vars[0],vars[1],vars[0]); 
    	cout<<"serial_time: "<< (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - begin)).count()<<endl;

    	cout<<"IsEqual: "<<IsEqual(C,C_serial,vars[0],vars[0])<<endl;
    	// cout<< (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - begin)).count()<<endl;
    }

	// MPI_Barrier(MPI_COMM_WORLD);

	MPI_Finalize();
	return 0;
}