#include <bits/stdc++.h>
#include <mpi.h>
using namespace std;
int num_pages=0;

vector<vector<int> > outedges(100000);
vector<double> pageranks(10000, 0.0f);
// vector<double> sumlist;

void mapper(MPI_Comm mpi_comm, int nproc, int rank, vector<double> pageranks)
{
    int batchsize = num_pages/(nproc);
    vector<double> pageranks_im(num_pages, 0.0f);
    for(int i=0; i<pageranks.size(); i++)
    {
      	int n = outedges[(rank*batchsize) + i].size();
      	if(n!=0)
        {
            for(int j =0; j<n; j++)
            {
                pageranks_im[outedges[(rank*batchsize)+i][j]] += (double)(pageranks[i]/n);
            }
        }
    }
    for (int i = 0; i < nproc; i++) 
    {
        if(i!=rank)
        {
            MPI_Send(&pageranks_im[i*batchsize],batchsize, MPI_DOUBLE, i, 1, mpi_comm);
        }
    }
}

void reducer(MPI_Comm mpi_comm, int nprocs, vector<double> &pagerank)
{
    int n = num_pages/(nprocs);
    vector<double> pg(n,0.0f);
    vector<double> pg_acc(n,0.0f);
    for (int j = 0; j < nprocs; j++) 
    {
        MPI_Recv(&pg[0],n, MPI_DOUBLE, j, 1, mpi_comm, MPI_STATUS_IGNORE);
        for(int i =0; i<n; i++)
        {
          pg_acc[i] += pg[i];
        }
    }
    pagerank =  pg_acc;
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

    double def_pagerank=1/num_pages;

    for(int i=0;i<num_pages;i++)
    {
        pageranks.push_back(def_pagerank);
    }

    MPI_Comm_rank(MPI_COMM_WORLD,&me);
    MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

    int iter=0;
    while(true)
    {
        mapper(MPI_COMM_WORLD, nprocs, me, pageranks);

        cout<<"here"<<me<<endl;
        MPI_Barrier(MPI_COMM_WORLD);

        reducer(MPI_COMM_WORLD, nprocs, pageranks);

        for(int i=0; i<num_pages; i++)
        {
            cout<<i<<" = "<<pageranks[i]<<endl;
        }
        if(iter>20)
            break;
        iter++;
    }

    MPI_Finalize();
    return 0;
}