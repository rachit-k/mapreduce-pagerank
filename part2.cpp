#include <bits/stdc++.h>
#include <mpi.h>
using namespace std;
long long num_pages=0;

vector<vector<int> > outedges(100000);
vector<double> pageranks;
vector<vector<double> > temppageranks(100000);
// vector<double> sumlist;

void mapper(MPI_Comm mpi_comm, int nproc, int rank, vector<double> pageranks)
{
    int batchsize = num_pages/(nproc);
    

    for(long long i=0; i<num_pages; i++)
    {
        temppageranks[i].push_back(0.0);
      	int n = outedges[i].size();
      	if(n!=0)
        {
            long start=rank;
            for(int j =start*n/nproc; j<(start+1)*n/nproc; j++)
            {
                temppageranks[outedges[i][j]].push_back( (double)(pageranks[i]/n));
            }
            MPI_Send(&temppageranks,batchsize, MPI_DOUBLE, 0, 1, mpi_comm);
        }
    }
}

void reducer(MPI_Comm mpi_comm, int nprocs, vector<double> &pagerank, int rank)
{
    int n = num_pages/(nprocs);
    vector<double> pg(n,0.0f);
    vector<double> temppageranks(n,0.0f);
    if(rank==0)
    {
    for (int j = 0; j < nprocs; j++) 
    {
        cout<<"Here receiving from process "<<j<<" for rank "<<rank<<endl;
        MPI_Recv(&pg[0],n, MPI_DOUBLE, j, 1, mpi_comm, 0);
        for(int i =0; i<n; i++)
        {
          temppageranks[i] += pg[i];
        }
    }
    pagerank =  temppageranks;
    }
}

int main(int narg, char **args) 
{
        
    MPI_Init(&narg,&args);

    int me,nprocs;
    nprocs=2;
    float s=0.85;

    ifstream fin;
    fin.open("test/bull.txt");
    int a,b;
    while(!fin.eof())
    {
        fin>>a>>b;
        outedges[a].push_back(b);
        num_pages = max(num_pages,max(a,b));
    }
    num_pages++;
    fin.close();

    double def_pagerank=1.0/num_pages;
    vector<double> temp(num_pages+1, 0.0);
    vector<double> temp2(num_pages+1, 0.0);
    for(int i=0;i<num_pages;i++)
    {
        temp[i]=(def_pagerank);
    }
    pageranks=temp;

    MPI_Comm_rank(MPI_COMM_WORLD,&me);
    MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

    int iter=0;
    while(true)
    {
        mapper(MPI_COMM_WORLD, nprocs, me, pageranks);

        cout<<"here "<<me<<endl;
        MPI_Barrier(MPI_COMM_WORLD);
        //cout<<"Beyonf the barrier "<<endl;
        reducer(MPI_COMM_WORLD, nprocs, pageranks, me);

        for(int i=0; i<num_pages; i++)
        {
            cout<<i<<" = "<<pageranks[i]<<endl;
        }
        vector<vector<double> > tempassign(100000);
        temppageranks=tempassign;
        if(iter>=0)
            break;
        iter++;
    }

    MPI_Finalize();
    return 0;
}