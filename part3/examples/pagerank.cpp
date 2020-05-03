#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/stat.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include <bits/stdc++.h>

using namespace MAPREDUCE_NS;

vector<vector<int>> outedges(10000);
vector<vector<double>> temppageranks(10000);
vector<double> pageranks(10000, 0.0f);
vector<double> dp_arr;
int num_pages=0;

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

void mapper1(int key, double pgrank)
{
    int n = outedges[key].size();
    if(n==0)
    {
        dp_arr.push_back(pgrank);
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

    // if (narg <= 1) 
    // {
    //     if (me == 0) printf("Syntax: wordfreq file1 file2 ...\n");
    //     MPI_Abort(MPI_COMM_WORLD,1);
    // }


  // double tstart = MPI_Wtime();
    int iter=0;
    while(true)
    {
        MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
        mr->verbosity = 2;
        mr->timer = 1;

        for(i=0; i<num_pages; i++)
        {
            structformapper sfm;
            sfm.key=i;
            sfm.pgrank=pageranks[i];
            int nwords = mr->map(nprocs,mapper1,&sfm);
        }

        double dp=0.0;
        for(i=0; i<dp_arr.size(); i++)
        {
            dp = dp+ (dp_arr[i]/num_pages);
        }

        MPI_Barrier(MPI_COMM_WORLD);

        for(i=0; i<num_pages; i++)
        {
            structformapper sfm;
            sfm.key=i;
            sfm.pgrank=pageranks[i];
            int nwords = mr->map(nprocs,mapper,&sfm);
        }
        mr->collate(NULL);
        for(i=0; i<num_pages; i++)
        {
            structforreducer sfr;
            sfr.key=i;
            sfr.pgranks=pageranks; //change - how do we get the intermediate 2D array?????
            int nunique = mr->reduce(reducer,&sfr);
        }
        mr->gather(1);
        for(int i=0; i<num_pages; i++)
        {
            pageranks[i] = s*pageranks_up[i] +  (1-s)/num_pages + s*dp;
        }
        if(iter>20)
            break;
        iter++;
        delete mr;
    } 
  // double tstop = MPI_Wtime();

    MPI_Finalize();

    double ans = 0.0;
    for(int i=0; i<num_pages; i++)
    {
        cout<<i<<" = "<<pageranks[i]<<endl;
        ans =ans+ pageranks[i];
    }
    cout<<"sum "<<ans;
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
