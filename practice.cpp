
#include <bits/stdc++.h>
#include <mpi.h>
using namespace std;
int main(int narg, char **args) 
{
        
    MPI_Init(&narg,&args);
    int me,nprocs;
    nprocs=2;
    MPI_Comm_rank(MPI_COMM_WORLD,&me);
    MPI_Comm_size(MPI_COMM_WORLD,&nprocs);
    int size=10;
    int batchsize=size/nprocs;

    for(int i=me*size/nprocs;i<(me+1)*size/nprocs;++i){
       int dumdum[]={i/5, i};
        MPI_Send(&dumdum,2, MPI_INT, i%nprocs, i%nprocs, MPI_COMM_WORLD);
        cout<<"sent "<<i<<" by rank "<<me<<endl;
    }
    
    // cout<<"SENT all for rank "<<me<<endl;
    int sum[]={0, 0};
    //cout<<"Sum[0] = "<<sum[0]<<" and sum[1] = "<<sum[1]<<endl; 
    for(int i=0;i<size;++i){
            int j[2];

            if(i%nprocs==me)
            {
            cout<<"With rank "<<me<<" at i = "<<i/batchsize<<endl;
            MPI_Recv(&j, 2, MPI_INT, i/batchsize, i%nprocs, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            sum[j[0]]+=j[1];

            cout<<"I have received j "<<j[0]<<endl;
            }
            else{
                continue;
            //     cout<<"With rank "<<me<<" at i = "<<i<<endl;
            // MPI_Recv(&j, 1, MPI_INT, i/2, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // cout<<"I have received j "<<j<<endl;

            }

        }
        MPI_Barrier(MPI_COMM_WORLD);
        cout<<"Sum[0] = "<<sum[0]<<" and sum[1] = "<<sum[1]<<endl; 
        if(me!=0){
             MPI_Send(sum,2, MPI_INT, 0, 1, MPI_COMM_WORLD);
        }
        if(me==0){
            int temp_sum[]={0,0};
            for(int i=1;i<nprocs;++i){

                MPI_Recv(temp_sum, 2, MPI_INT, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                sum[0]=sum[0]+temp_sum[0];
                sum[1]=sum[1]+temp_sum[1];
            }
            cout<<"Sum[0] = "<<sum[0]<<" and sum[1] = "<<sum[1]<<endl; 
        }

        MPI_Finalize();
    return 0;
    return 0;
}