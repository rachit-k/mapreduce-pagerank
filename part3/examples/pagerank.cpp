#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/stat.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include <bits/stdc++.h>

using namespace MAPREDUCE_NS;

vector<vector<int> > outedges(100000);
// vector<vector<double> > temppageranks(100000);
vector<double> pageranks(100000);
int num_pages=0;



// struct structformapper
// {
//     int key;
//     double pgrank;
// };

// struct structforreducer
// {
//     int key;
//     vector<double> pgranks;
// };

void mapper(int itask, KeyValue *kv, void *ptr)//int key, double pgrank)
{
    int rank, nprocs;
    MPI_Comm_size(MPI_COMM_WORLD,&nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    for(int i=((rank*num_pages)/nprocs); i < (((rank+1)*num_pages)/nprocs); i++) 
    {
        double val = 0.0;
        kv->add((char *) &i,sizeof(int),(char *) &val,sizeof(double));
        int n = outedges[i].size();
        if(n!=0)
        {
            for(int j =0; j<n; j++)
            {
                double val=(double)(pageranks[i]/n);
                kv->add((char *) &outedges[i][j],sizeof(int),(char *) &val,sizeof(double));
            }
        }
    }

    // kv->add(key,sizeof(int),0.0,sizeof(double));
    // int n = outedges[key].size();
    // if(n!=0)
    // {
    //     for(int i =0; i<n; i++)
    //     {
    //         kv->add((char *) outedges[key][i],sizeof(int),(char *) &(double)(pgrank/n),sizeof(double));
    //     }
    // }
}

// void mapper1(int key, double pgrank)
// {
//     int n = outedges[key].size();
//     if(n==0)
//     {
//         dp_arr.push_back(pgrank);
//     }
// }

void reducer(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr)//int key, vector<double> pgranks)
{
    // double new_rank = 0.0f;
    // int n=pgranks.size();
    // for(int i =0; i<n; i++)
    // {
    //     new_rank = new_rank+ pgranks[i];
    // }
    // pageranks[key] = new_rank;
    int keyy = *(int *) key;
    double* vals = (double *) multivalue;
    double pgrank = 0;
    for(int i = 0; i < nvalues; i++)
    {
        pgrank =pgrank+ vals[i];
    }
    pageranks[keyy]=pgrank;
}

int main(int argc, char **argv)
{
    MPI_Init(&argc,&argv);

    int me,nprocs;
    int max_iters=stoi(argv[2]);

    float s=0.85;

    MPI_Comm_size(MPI_COMM_WORLD,&nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD,&me);

    ifstream fin;
    fin.open(argv[1]); // test/barabasi-20000.txt

    int a,b;
    while(!fin.eof())
    {
        fin>>a>>b;
        outedges[a].push_back(b);
        num_pages = max(num_pages,max(a,b));
    }
    num_pages++;
    fin.close();

    double def_pagerank=(1.0/num_pages);
    vector<double> temp(num_pages+1, 0.0);
    for(int i=0;i<num_pages;i++)
    {
        temp[i]=(def_pagerank);
        // pageranks[i]=(def_pagerank);
    }
    pageranks=temp;


    // if (narg <= 1) 
    // {
    //     if (me == 0) printf("Syntax: wordfreq file1 file2 ...\n");
    //     MPI_Abort(MPI_COMM_WORLD,1);
    // }


  double tstart = MPI_Wtime();
    int iter=0;
    while(true)
    {
        MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
        mr->verbosity = 0;
        mr->timer = 0;

        // for(int i=0; i<num_pages; i++)
        // {
        //     structformapper sfm;
        //     sfm.key=i;
        //     sfm.pgrank=pageranks[i];
        //     int nwords = mr->map(nprocs,&mapper1,&sfm);
        // }

        double dp=0.0;
        for(int i=0; i<num_pages; i++)
        {    
            if(outedges[i].size()==0)
            {
               dp = dp+ (double)(pageranks[i]/num_pages);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);


        int nwords = mr->map(nprocs,mapper,NULL);

        // for(int i=0; i<num_pages; i++)
        // {
        //     structformapper sfm;
        //     sfm.key=i;
        //     sfm.pgrank=pageranks[i];
        //     int nwords = mr->map(nprocs,&mapper,&sfm);
        // }
        mr->gather(1);
        mr->convert();

        int nunique=mr->reduce(reducer,NULL);

        // for(int i=0; i<num_pages; i++)
        // {
        //     structforreducer sfr;
        //     sfr.key=i;
        //     sfr.pgranks=pageranks; //change - how do we get the intermediate 2D array?????
        //     int nunique = mr->reduce(&reducer,&sfr);
        // }

        // MPI_Barrier(MPI_COMM_WORLD);
        for(int i=0; i<num_pages; i++)
        {
            pageranks[i] = (s*pageranks[i]) + (double)((1-s)/num_pages) + s*dp;
            // cout<<i<<" : "<<pageranks[i]<<" = "<<(s*pageranks[i])<<" + "<<(double)((1-s)/num_pages)<<" + "<<s*dp<<endl;
        }

        // double ans1 = 0.0;
        // for(int i=0; i<num_pages; i++)
        // {
        //     ans1 += pageranks[i];
        // }
        // cout<<me<<" sum "<<ans1<<endl;
        // for(int i=0; i<num_pages; i++)
        // {
        //     pageranks[i]=pageranks[i]/ans1;
        // }
        if(iter>max_iters)
            break;
        iter++;
        delete mr;


    } 
  double tstop = MPI_Wtime();
  cout<<me<<" time "<<tstop-tstart<<endl;

    MPI_Finalize();

    if(me==0)
    {
        // double ans1 = 0.0;
        // for(int i=0; i<num_pages; i++)
        // {
        //     ans1 += pageranks[i];
        // }
        // for(int i=0; i<num_pages; i++)
        // {
        //     pageranks[i]=pageranks[i]/ans1;
        // }

        double ans = 0.0;
        for(int i=0; i<num_pages; i++)
        {
            // cout<<i<<" = "<<pageranks[i]<<endl;
            ans =ans+ pageranks[i];
        }
        cout<<"sum "<<ans<<endl;
    }
}

