#include <bits/stdc++.h>
#include <mpi.h>

using namespace std;
long long num_pages=0;

vector<vector<int> > outedges(100000);
vector<double> pageranks;
vector<vector<double> > temppageranks(100000);
double* reduced_sum;
int* d_vec;
double dprod=0.0;
double fraction=0.85;
// vector<double> sumlist;
// make the destination change for each value
void mapper(MPI_Comm mpi_comm, long long nproc, long long rank, vector<double> pageranks)
{
    long long batchsize = num_pages/(nproc);
    long long start=rank;
    for(long long i=0; i<num_pages; i++)
    {
        if(i%nproc==rank)
        {
        long long n = outedges[i].size();
        if(n!=0)
        {
            
            for(long long j =0; j<n; j++)
            {
                double keyval[]={outedges[i][j], fraction*(double)(pageranks[i]/n)};
                MPI_Send(&keyval,2, MPI_DOUBLE, rank, 1, MPI_COMM_WORLD);
                //cout<<"Sending to rank "<<i%nproc<<endl;
            }
            
            // cout<<"Value sent was "<<((1-fraction)/num_pages)+ (fraction*dprod/num_pages)<<endl;
            double keyvaltemp[]={i*1.0,((1-fraction)/num_pages)+ (fraction*dprod/num_pages)};
            MPI_Send(&keyvaltemp,2, MPI_DOUBLE, rank, 1, MPI_COMM_WORLD);
            //cout<<"@Sending key "<<i<<" for rank "<<rank<<endl;
        }
        else{
            // cout<<"Sending key "<<i<<" for rank "<<i%nproc<<endl;
            double keyvaltemp[]={i*1.0, ((1-fraction)/num_pages)+ (fraction*dprod/num_pages)};
            MPI_Send(&keyvaltemp,2, MPI_DOUBLE, rank, 1, MPI_COMM_WORLD);

        }
        }


    }
    //can remove
    double keyval[]={-1.0, -1.0};
    MPI_Send(&keyval,2, MPI_DOUBLE, rank, 1, MPI_COMM_WORLD);
    //cout<<"End Sending to rank "<<rank<<endl;
    // cout<<"Sent for rank "<<rank<<endl;
}

void mapperd(MPI_Comm mpi_comm, long long nproc, long long rank, vector<double> pageranks)
{
    long long batchsize = num_pages/(nproc);
    long long start=rank;
    for(long long i=start*num_pages/nproc; i<(start+1)*num_pages/nproc; i++)
    {
                double keyval[]={0, d_vec[i]*pageranks[i]};
                //cout<<"Value getting sent is "<<pageranks[i]<<" and "<<d_vec[i]<<" for i= "<<i<<endl;
                MPI_Send(&keyval,2, MPI_DOUBLE, i%nproc, 1, MPI_COMM_WORLD);
                //cout<<"Sending to rank "<<i%nproc<<endl;
    }
    //can remove
    MPI_Barrier(MPI_COMM_WORLD);
    double keyval[]={-1.0, -1.0};
    MPI_Send(&keyval,2, MPI_DOUBLE, rank, 1, MPI_COMM_WORLD);
    //cout<<"End Sending to rank "<<rank<<endl;
    // cout<<"Sent for rank "<<rank<<endl;
}

void semireducer(MPI_Comm mpi_comm, long long nprocs, vector<double> &pagerank, long long rank, long long numpages)
{
    long long batchsize = num_pages/(nprocs);
    vector<double> pg(batchsize,0.0f);
    vector<double> temppageranks(batchsize,0.0f);
    for(int i=0;i<numpages;++i){
        reduced_sum[i]=0.0;
    }
    int count=0;
    //cout<<"In semireduced for rank "<<rank<<endl;
    for (int j = 0; j < nprocs; j++) 
    {
        if(rank==j)
        while(true)
        {
        double keyval[]={0.0, 0.0};
        // if(rank==1)
        // cout<<"Here receiving  for rank "<<rank<<endl;
        //  cout<<"Here receiving  for rank "<<rank<<" at count "<<count++<<endl;
        MPI_Recv(&keyval[0],2, MPI_DOUBLE, MPI_ANY_SOURCE, 1, mpi_comm, 0);
        // if(rank==1)
        //  cout<<"Val received is "<<keyval[0]<<" and "<<keyval[1]<<" for rank "<<rank<<endl;
        if(keyval[0]<-0.5)
        {
            // cout<<"Broken "<<" for rank "<<rank<<endl;
            break;
        }
        reduced_sum[(int)keyval[0]]+=keyval[1];
        }
       
    }
    // cout<<"Reduced sum is "<<reduced_sum[0]<<" and "<<reduced_sum[1]<<" for rank "<<rank<<endl;
    //cout<<" Done semi reduce"<<endl;
    MPI_Barrier(MPI_COMM_WORLD);
    
    if(rank!=0)
    MPI_Send(reduced_sum,num_pages, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD);
    // cout<<"End of semireducer for rank "<<rank<<endl;
    
}
void semireducerd(MPI_Comm mpi_comm, long long nprocs, vector<double> &pagerank, long long rank, long long numpages)
{
    //cout<<"In semireduced for rank "<<rank<<endl;
    
    for (int j = 0; j < nprocs; j++) 
    {
        if(rank==j)
        while(true)
        {
        double keyval[]={0.0, 0.0};
        

        MPI_Recv(&keyval[0],2, MPI_DOUBLE, MPI_ANY_SOURCE, 1, mpi_comm, 0);
        //cout<<"Val received is "<<keyval[0]<<" and "<<keyval[1]<<" for rank "<<rank<<endl;
        if(keyval[0]<0)
            break;
        dprod+=keyval[1];
        }
       
    }
    //cout<<"Value to be sent isis "<<dprod<<" for rank "<<rank<<endl;
    //cout<<" Done semi reduce"<<endl;
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank!=0)
    MPI_Send(&dprod,1, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD);
    
}
void reducer(MPI_Comm mpi_comm, long long nprocs, vector<double> &pagerank, long long rank, long long numpages)
{
    if(rank==0)
    {
    double temp_reduced_sum[numpages];
    for(int i=0;i<numpages;++i){
        temp_reduced_sum[i]=0.0;
    }
    
    for (int j = 1; j < nprocs; j++) 
    {
        //cout<<"Val received is "<<temp_reduced_sum[0]<<" and "<<temp_reduced_sum[1]<<" for rank "<<j<<endl;
       
        MPI_Recv(temp_reduced_sum,num_pages, MPI_DOUBLE, j, 1, mpi_comm, 0);
        //cout<<"Val received is "<<temp_reduced_sum[0]<<" and "<<temp_reduced_sum[1]<<" for rank "<<j<<endl;
        for(int i=0;i<numpages;++i){
            reduced_sum[i]+=temp_reduced_sum[i];
        }
    }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Bcast(reduced_sum, num_pages, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    
    return;


    
}
void reducerd(MPI_Comm mpi_comm, long long nprocs, vector<double> &pagerank, long long rank, long long numpages)
{
    double temp_dprod=0.0;
    //cout<<"Initially dprod is "<<dprod<<" for rank "<<rank<<endl;
    if(rank==0)
    for (int j = 1; j < nprocs; j++) 
    {
        MPI_Recv(&temp_dprod,2, MPI_DOUBLE, j, 1, mpi_comm, 0);
        //cout<<"Val received is "<<temp_dprod<<" for rank "<<j<<endl;
        dprod+=temp_dprod;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Bcast(&dprod, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    //cout<<"dprod is "<<dprod<<" for rank "<<rank<<endl;
    return;


    
}

int main(int narg, char **args) 
{
        
    MPI_Init(&narg,&args);

    int me,nprocs;
    nprocs=2;
    float s=0.85;

    ifstream fin;
    string filename;
    if(narg>1)
    {
        filename=args[1];
    }
    fin.open(filename);
    long long a,b;
    while(!fin.eof())
    {
        fin>>a>>b;
        outedges[a].push_back(b);
        num_pages = max(num_pages,max(a,b));
    }
    num_pages++;
    fin.close();

    d_vec=new int[num_pages];
    reduced_sum=new double[num_pages];
    double def_pagerank=1.0/num_pages;
    vector<double> temp(num_pages, 0.0);
    vector<double> temp2(num_pages, 0.0);
    for(int i=0;i<num_pages;i++)
    {
        temp[i]=(def_pagerank);
        if(outedges[i].size()==0)
            d_vec[i]=1;
        else
        {
            d_vec[i]=0;
        }
        
    }

    pageranks=temp;

    MPI_Comm_rank(MPI_COMM_WORLD,&me);
    MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

    int iter=0;
    std::chrono::time_point<std::chrono::system_clock> start, end; 
    start = std::chrono::system_clock::now();
    // cout<<"Starting while loop"<<endl;
    while(true)
    {
        mapperd(MPI_COMM_WORLD, nprocs, me, pageranks);

        // cout<<"here "<<me<<endl;
        //MPI_Barrier(MPI_COMM_WORLD);
        semireducerd(MPI_COMM_WORLD, nprocs, pageranks, me, num_pages);
        // cout<<"Beyonf the barrier "<<endl;
        reducerd(MPI_COMM_WORLD, nprocs, pageranks, me, num_pages);
        MPI_Barrier(MPI_COMM_WORLD);
        // cout<<"Dprod is "<<dprod/num_pages<<endl;
        mapper(MPI_COMM_WORLD, nprocs, me, pageranks);

        // cout<<"@here "<<me<<endl;
        //MPI_Barrier(MPI_COMM_WORLD);
        semireducer(MPI_COMM_WORLD, nprocs, pageranks, me, num_pages);
        // cout<<"@@@@Reduced sum for rank "<<me<<" is "<<reduced_sum[1]<<endl;
        // cout<<"@Beyonf the barrier "<<endl;
        reducer(MPI_COMM_WORLD, nprocs, pageranks, me, num_pages);
        // cout<<"@!!!!Beyonf the barrier "<<endl;
        //MPI_Barrier(MPI_COMM_WORLD);
        double sum=0.0;
        for(int i=0; i<num_pages; i++)
        {
            pageranks[i]=reduced_sum[i];
            sum=sum+reduced_sum[i];
            // if(me==0)
            // {
            // cout<<i<<" = "<<pageranks[i]<<endl;
            
            // }
            
        }
        //if(me==0)
        // cout<<"One iteration done "<<endl;

        if(iter>=20)
            break;
        dprod=0;
        iter++;
        // cout<<"End of one iter for rank "<<me<<" number of iterations is "<<iter<<endl;
    }
    // cout<<"Out of the loop for rank "<<me<<endl;
    end = std::chrono::system_clock::now(); 
    std::chrono::duration<double> elapsed_seconds = end - start; 
    double sum=0.0;

   
    if(me==0)
    {
     for(int i=0; i<num_pages; i++)
        {   sum=sum+pageranks[i];
                // cout<<i<<" = "<<pageranks[i]<<endl;
        }
    // cout<<"Sum "<<sum<<endl;
    // std::cout<< "Parallel elapsed time: " << elapsed_seconds.count() << "s\n"; 
    std::cout<<filename<<"\t";
    std::cout<<elapsed_seconds.count()<<"\n"; 
    }
    // cout<<"End for rank "<<me<<endl;
    MPI_Finalize();
    return 0;
}