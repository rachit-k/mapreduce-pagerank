#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/stat.h"
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;

vector<vector<int>> outedges;
vector<vector<double>> temppageranks;
vector<double> pageranks;
int num_pages=10000;

// void fileread(int, char *, KeyValue *, void *);

struct structformapper
{
    int key;
    double pgrank;
};

struct structforreducer
{
    int key;
    vector<double> pgranks;
};

void mapper(int key, double pgrank)
{
    kv->add(key,sizeof(int),0.0,sizeof(double));
    int n = outedges[key].size();
    if(n!=0)
    {
        for(int i =0; i<n; i++)
        {
            kv->add(outgoing_links[key][i],sizeof(int),(pgrank/n),sizeof(double));
        }
    }
}

void reducer(int key, vector<double> pgranks)
{
    double new_rank = 0.0f;
    int n=pg.size();
    for(int i =0; i<n; i++)
    {
        new_rank = new_rank+ pg[i];
    }
    pageranks[key] = new_rank;
}

int main(int narg, char **args)
{
    MPI_Init(&narg,&args);

    int me,nprocs;
    nprocs=2;
    float s=0.85;

    ifstream fin;
    fin.open("barabasi-10000.txt");
    int a,b;
    while(!fin.eof())
    {
        fin>>a>>b;
        outedges[a].push_back(b);
    }
    fin.close();

    double def_pagerank=1/numpages;

    for(int i=0;i<num_pages;i++)
    {
        pageranks.pushback(def_pagerank)
    }

    MPI_Comm_rank(MPI_COMM_WORLD,&me);
    MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

    if (narg <= 1) 
    {
        if (me == 0) printf("Syntax: wordfreq file1 file2 ...\n");
        MPI_Abort(MPI_COMM_WORLD,1);
    }

    MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  // double tstart = MPI_Wtime();
    int iter=0;
    while(true)
    {
        MPI_Barrier(MPI_COMM_WORLD);
        for(i=0; i<num_webpages; i++)
        {
            structformapper sfm;
            sfm.key=i;
            sfm.pgrank=pageranks[i]
            int nwords = mr->map(nprocs,mapper,&sfm);
        }
        mr->collate(NULL);
        for(i=0; i<num_webpages; i++)
        {
            structforreducer sfr;
            sfr.key=i;
            sfr.pgranks=pageranks; //change
            int nunique = mr->reduce(reducer,&sfr);
        }
        mr->gather(0);
        for(int i=0; i<num_webpages; i++)
        {
            pageranks[i] = s*pageranks_up[i] +  (double)(1-s)/num_webpages; //+s*dp
        }
        if(iter>20)
            break;
        iter++;
    }

 
  // double tstop = MPI_Wtime();

    delete mr;

    MPI_Finalize();
}

/* ----------------------------------------------------------------------
   read a file
   for each word in file, emit key = word, value = NULL
------------------------------------------------------------------------- */

// void fileread(int itask, KeyValue *key, void *ptr)
// {
//     n=outedges[key].size();
//     for (int i=0;i<n;i++)
//     {
//         double temp = value/n;
//         kv->add(outedges[key][i],sizeof(int),temp,sizeof(int));
//     }
// }
