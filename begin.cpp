// Copyright (c) 2009-2016 Craig Henderson
// https://github.com/cdmh/mapreduce

#include <boost/config.hpp>
#include <fstream>
#if defined(BOOST_MSVC)
#   pragma warning(disable: 4127)

// turn off checked iterators to avoid performance hit
#   if !defined(__SGI_STL_PORT)  &&  !defined(_DEBUG)
#       define _SECURE_SCL 0
#       define _HAS_ITERATOR_DEBUGGING 0
#   endif
#endif
#include <string>
#include "mapreduce.hpp"
double *probablity;
unsigned long global_size;
unsigned **page_rank;
void print_prob(){
    for(unsigned long i=0;i<global_size;++i){
        std::cout<<probablity[i]<<std::endl;
    }
}
void print_graph(){
    for(unsigned long i=0;i<global_size;++i){
        for(unsigned long j=0;j<global_size;++j){
            std::cout<<page_rank[i][j]<<" ";
        }
        std::cout<<std::endl;
    }
}
namespace Ap_calc {
// pls pld check outedges wala code with -1
std::vector<std:: vector<unsigned> > outedges;
std::vector<double> pr;
unsigned long size_graph;
bool const is_outgoing(unsigned const page1, unsigned const page2)
{
    return (page_rank[page1][page2])==1;
}
unsigned long const outgoing(unsigned const page1)
{
    unsigned long sum=0;
    for(unsigned long i=0;i<size_graph;++i){
        sum=sum+page_rank[page1][i];
    }
    return sum;
}
void calc_outedges()
{
     for(unsigned long i=0;i<size_graph;++i){
        std::vector<unsigned> vec;
        for(unsigned long j=0;j<size_graph;++j){
            if(page_rank[i][j]>0){
                vec.push_back(j);
            }
        }
        if(vec.size()==0){
            vec.push_back(-1);
        }
        outedges.push_back(vec);
    }
}
template<typename MapTask>
class datasource : mapreduce::detail::noncopyable
{
  public:
    datasource() : sequence_(0)
    {
    }
    datasource(unsigned long size): sequence_(0){
        len=size;
    }

    bool const setup_key(typename MapTask::key_type &key)
    {
        key = sequence_++;
        return key<len;
    }

    bool const get_data(typename MapTask::key_type const &key, typename MapTask::value_type &value)
    {
        value=probablity[key];
        //std::cout<<"Len is "<<len<<std::endl;
        return true;
    }

  private:
    unsigned sequence_;
    unsigned long len;
};
struct map_task : public mapreduce::map_task<unsigned, double >
{
    template<typename Runtime>
    void operator()(Runtime &runtime, key_type const &key, value_type const &value) const
    {
        int n = outedges[key].size();
        //calc prod_dp   
        // /std::cout<<"Value is"<<value<<std::endl;
        for (int i=0;i<n;i++)
        {
            typename Runtime::reduce_task_type::key_type const emit_key = outedges[key][i];
            double temp=value/n;
            // temp = value/n; what to do here?
            //std::cout<<"Emitting for key "<<key<<std::endl;
            if(emit_key==24){
                //std::cout<<"value emitted is "<<temp<<std::endl;
            }
            runtime.emit_intermediate(emit_key, temp);
            
        }
        runtime.emit_intermediate(key, 0.0);
    }
};

struct reduce_task : public mapreduce::reduce_task<unsigned, double >
{
    template<typename Runtime, typename It>
    void operator()(Runtime &runtime, key_type const &key, It it, It ite) const
    {
        if(key>size_graph){
            return;
        }
        value_type results=0.0;
        
        for (It it1=it; it1!=ite; ++it1)
        {
            results = results+ (*it1);
            
        }
        
        runtime.emit(key, results);        
    }
};  

typedef
mapreduce::job<Ap_calc::map_task,
               Ap_calc::reduce_task,
               mapreduce::null_combiner,
               Ap_calc::datasource<Ap_calc::map_task>
> job;

} // namespace Ap_calc
namespace Dp_calc {
unsigned *d_vec;
unsigned long size_graph;
bool const is_outgoing(unsigned const page1, unsigned const page2)
{
    return (page_rank[page1][page2])==1;
}
unsigned long const outgoing(unsigned const page1)
{
    unsigned long sum=0;
    for(unsigned long i=0;i<size_graph;++i){
        sum=sum+page_rank[page1][i];
    }
    return sum;
}

void mask_nonoutgoing()
{
    d_vec= new unsigned[size_graph];

    for(unsigned long i=0;i<size_graph;++i){
        bool flag=false;
        for(unsigned long j=0;j<size_graph;++j){
            if(page_rank[i][j]>0){
                d_vec[i]=0;
                flag=true;
                continue;
            }
        }
        if(!flag)
            d_vec[i]=1;
    }
}
template<typename MapTask>
class datasource : mapreduce::detail::noncopyable
{
  public:
    datasource() : sequence_(0)
    {
    }
    datasource(unsigned long size): sequence_(0){
        len=size;
    }

    bool const setup_key(typename MapTask::key_type &key)
    {
        key = sequence_++;
        return key<len;
    }

    bool const get_data(typename MapTask::key_type const &key, typename MapTask::value_type &value)
    {
        value=probablity[key];
        //std::cout<<"Len is "<<len<<std::endl;
        return true;
    }

  private:
    unsigned sequence_;
    unsigned long len;
};
  

float prod_dp=0.0;

struct map_task : public mapreduce::map_task<unsigned, double >
{
    template<typename Runtime>
    void operator()(Runtime &runtime, key_type const &key, value_type const &value) const
    {
        int n = d_vec[key];
        double temp=value*n;
        runtime.emit_intermediate(key, temp);
        runtime.emit_intermediate(key, 0.0);
    }
};

struct reduce_task : public mapreduce::reduce_task<unsigned, double >
{
    template<typename Runtime, typename It>
    void operator()(Runtime &runtime, key_type const &key, It it, It ite) const
    {
        if(key>size_graph){
            return;
        }
        value_type results=0.0;
        
        for (It it1=it; it1!=ite; ++it1)
        {
            results = results+ (*it1);
            
        }
        
        runtime.emit(key, results);        
    }
};  

typedef
mapreduce::job<Dp_calc::map_task,
               Dp_calc::reduce_task,
               mapreduce::null_combiner,
               Dp_calc::datasource<Dp_calc::map_task>
> job;

} // namespace Dp_calc
int main(int argc, char *argv[])
{
    mapreduce::specification spec;

    if (argc > 3)
        spec.map_tasks = std::max(1, atoi(argv[1]));

    if (argc > 4)
        spec.reduce_tasks = atoi(argv[2]);
    else
        spec.reduce_tasks = std::max(1U, std::thread::hardware_concurrency());

    

    std::cout <<"\nPage Rank analysis MapReduce..." <<std::endl;
    std::string filename="test/mytest.txt";
    if(argc>1){
        filename=argv[1];
    }
    std::ifstream inFile;
    inFile.open(filename);
    std::vector<std::pair<int, int> > page_coordinates;
    if (!inFile) {
        std::cout << "Unable to open file";
        exit(1); // terminate with error
    }
    // std::cout <<"\nHere" <<std::endl;
    unsigned long a, b;
    unsigned long max=0; 
    while(inFile>>a>>b){
        unsigned long temp=0;
        page_coordinates.push_back(std::make_pair(a,b));
        temp=a>b?a:b;
        if(temp>max){
            max=temp;
        }
    }
    unsigned long size=max+1;
    global_size=size;
    Ap_calc::size_graph=size;
    Dp_calc::size_graph=size;
    page_rank= new unsigned*[size];
    for(unsigned i = 0; i < size; ++i)
        page_rank[i] = new unsigned[size];
    for(unsigned i = 0; i < size; ++i){
        for(unsigned j=0; j < size; ++j){
            page_rank[i][j]=0;
        }
    }
    //page rank initialisation
    for(auto x:page_coordinates){
        page_rank[x.first][x.second]=1;
    }
    probablity = new double[size];
    //probability initailisation
    for(unsigned i =0;i<size;++i){
        probablity[i] = (double)1/size;

    }
    //print the page rank graph
    print_graph();
    int num_iterations=0;
    Ap_calc::calc_outedges();
    Dp_calc::mask_nonoutgoing();
    int max_iterations=20;
    if(argc>2){
        max_iterations=atoi(argv[2]);
    }
    double probability_dp[global_size];
    double probability_ap[global_size];
    for(int i=0;i<global_size;++i){
        probability_ap[i]=0.0;
        probability_dp[i]=0.0;
    }
    while(num_iterations<max_iterations){
        Ap_calc::job::datasource_type datasource(size);
        Ap_calc::job job(datasource, spec);
        Dp_calc::job::datasource_type datasource_dp(size);
        Dp_calc::job job_dp(datasource_dp, spec);
        mapreduce::results result;
        mapreduce::results result_dp;
        #ifdef _DEBUG
            job.run<mapreduce::schedule_policy::sequential<Ap_calc::job> >(result);
            job_dp.run<mapreduce::schedule_policy::sequential<Dp_calc::job> >(result_dp);
        #else
            job.run<mapreduce::schedule_policy::cpu_parallel<Ap_calc::job> >(result);
            job_dp.run<mapreduce::schedule_policy::cpu_parallel<Dp_calc::job> >(result_dp);
        #endif
        
        for (auto it=job.begin_results(); it!=job.end_results(); ++it)
        {
            probability_ap[it->first]=it->second;
        }
        for (auto it=job_dp.begin_results(); it!=job_dp.end_results(); ++it)
        {
            probability_dp[it->first]=it->second/global_size;
            std::cout<<"We are here at probability_dp with "<<it->second<<std::endl;
        }
        for(int i=0;i<global_size;++i){
            probablity[i]=probability_ap[i]+probability_dp[i];
        }
        std::cout<<"After "<<num_iterations+1<<" number of iterations "<<std::endl;
        print_prob();
        num_iterations++;
    }
    return 0;
}

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
