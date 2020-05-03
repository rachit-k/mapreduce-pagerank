#include <bits/stdc++.h>
#include <chrono>
using namespace std;

vector<vector<int>> outedges(100000);
vector<vector<double>> temppageranks(100000);
vector<double> pageranks;
vector<double> dp_arr;
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

void mapper(int key, double pgrank)
{
    // kv->add(key,sizeof(int),0.0,sizeof(double));
    temppageranks[key].push_back(0.0);
    int n = outedges[key].size();
    if(n!=0)
    {
        for(int i =0; i<n; i++)
        {
            // kv->add(outgoing_links[key][i],sizeof(int),(pgrank/n),sizeof(double));
            temppageranks[outedges[key][i]].push_back((pgrank/n));
            // cout<<"Page "<<outedges[key][i]<<" got assigned the value "<<(pgrank/n)<<endl;
        }
    }
}

void mapper1(int key, double pgrank)
{
    int n = outedges[key].size();
    //std::cout<<"Size for key = "<<key<<" is "<<n<<" with value "<<pgrank<<std::endl;
    if(n==0)
    {
        dp_arr[key]=(pgrank);
    }
}

void reducer(int key, vector<double> pgranks)
{
    double new_rank = 0.0f;
    int n=pgranks.size();
    for(int i =0; i<n; i++)
    {
        new_rank = new_rank+ pgranks[i];
        // std::cout<<"New page rank for key "<<key<<" is "<<pgranks[i]<<std::endl;
    }
    // std::cout<<"New page rank for key "<<key<<" is "<<new_rank<<std::endl;
    pageranks[key] = new_rank;
}

int main(int argc, char *argv[])
{
    // MPI_Init(&narg,&args);

    // int me,nprocs;
    // nprocs=2;
    float s=0.85;
    // std::cout<<"OHHHHHHHHH"<<std::endl;
    ifstream fin;
    string filename="test/mytest.txt";
    if(argc>=1){
        filename=argv[1];
    }
    fin.open(filename);
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
    dp_arr=temp2;
    for(int i=0;i<num_pages;i++)
    {
        temp[i]=(def_pagerank);
    }
    pageranks=temp;
    // MPI_Comm_rank(MPI_COMM_WORLD,&me);
    // MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

    // if (narg <= 1) 
    // {
    //     if (me == 0) printf("Syntax: wordfreq file1 file2 ...\n");
    //     MPI_Abort(MPI_COMM_WORLD,1);
    // }


  // double tstart = MPI_Wtime();
    int iter=0;
    std::chrono::time_point<std::chrono::system_clock> start, end; 
    start = std::chrono::system_clock::now(); 
    while(true)
    {
        // MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
        // mr->verbosity = 2;
        // mr->timer = 1;

        for(int i=0; i<num_pages; i++)
        {
            // structformapper sfm;
            // sfm.key=i;
            // sfm.pgrank=pageranks[i]
            // int nwords = mr->map(nprocs,mapper1,&sfm);
            mapper1(i,pageranks[i]);
        }

        double dp=0.0;
        for(int i=0; i<dp_arr.size(); i++)
        {
            dp = dp+ (dp_arr[i]/num_pages);
            //std::cout<<"updating dp with"<<(dp_arr[i]/num_pages)<<std::endl;
        }

        // MPI_Barrier(MPI_COMM_WORLD);

        for(int i=0; i<num_pages; i++)
        {
            // structformapper sfm;
            // sfm.key=i;
            // sfm.pgrank=pageranks[i]
            // int nwords = mr->map(nprocs,mapper,&sfm);
            mapper(i,pageranks[i]);
        }
        // mr->collate(NULL);
        for(int i=0; i<num_pages; i++)
        {
            // structforreducer sfr;
            // sfr.key=i;
            // sfr.pgranks=pageranks; //change - how do we get the intermediate 2D array?????
            // int nunique = mr->reduce(reducer,&sfr);
            reducer(i,temppageranks[i]);
        }
        // mr->gather(1);
        for(int i=0; i<num_pages; i++)
        {
            pageranks[i] = (s*pageranks[i]) +  ((1-s)/(num_pages)) + (s*dp) ;
        }

        //normalize
        // double ans = 0.0;
        // for(int i=0; i<num_pages; i++)
        // {
        //     ans += pageranks[i];
        // }
        // for(int i=0; i<num_pages; i++)
        // {
        //     pageranks[i]=pageranks[i]/ans;
        // }
       
        temppageranks.clear();
        vector<vector<double>> tempassign(100000);
        temppageranks=tempassign;
        if(iter>=20)
            break;
        iter++;
        // delete mr;
    }

    double ans = 0.0;
    end = std::chrono::system_clock::now(); 
    std::chrono::duration<double> elapsed_seconds = end - start; 
         for(int i=0; i<num_pages; i++)
        {
            cout<<i<<" = "<<pageranks[i]<<endl;
            ans =ans+ pageranks[i];
        }
        cout<<"sum "<<ans<<std::endl;
    // for(int i=0; i<num_pages; i++)
    // {
    //     cout<<i<<" = "<<pageranks[i]<<endl;
    //     ans =ans+ pageranks[i];
    // }
    // cout<<"sum "<<ans;
    std::cout<< "elapsed time: " << elapsed_seconds.count() << "s\n"; 
 
  // double tstop = MPI_Wtime();



    // MPI_Finalize();
}

