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

#include "mapreduce.hpp"

namespace friend_graph {

unsigned const friends[8][8] = { { 0, 1, 0, 1, 1, 0, 0, 0 },
                                 { 0, 0, 0, 1, 0, 0, 0, 1 },
                                 { 0, 0, 0, 1, 0, 1, 0, 0 },
                                 { 0, 0, 0, 0, 1, 0, 0, 1 },
                                 { 0, 0, 0, 0, 0, 1, 0, 0 },
                                 { 0, 0, 0, 0, 0, 0, 0, 0 },
                                 { 0, 0, 0, 0, 0, 0, 0, 1 },
                                 { 0, 0, 0, 0, 0, 0, 0, 0 } };
char const * const names[] = { "Steve", "Anne", "Michael", "Brett", "Diane", "Sue", "Ruby", "Jack" };
unsigned **page_rank;
unsigned long size_graph;
bool const is_friend(unsigned const person1, unsigned const person2)
{
    return person1 != person2  &&  (friends[person1][person2]  ||  friends[person2][person1]);
}
void print_graph(){
    for(unsigned long i=0;i<size_graph;++i){
        for(unsigned long j=0;j<size_graph;++j){
            std::cout<<page_rank[i][j]<<" ";
        }
        std::cout<<std::endl;
    }
}
template<typename MapTask>
class datasource : mapreduce::detail::noncopyable
{
  public:
    datasource() : sequence_(0)
    {
    }

    bool const setup_key(typename MapTask::key_type &key)
    {
        key = sequence_++;
        return key < 8;
    }

    bool const get_data(typename MapTask::key_type const &key, typename MapTask::value_type &value)
    {
        for (unsigned loop=0; loop<8; ++loop)
            if (is_friend(key,loop))
                value.push_back(loop);
        return true;
    }

  private:
    unsigned sequence_;
};

struct map_task : public mapreduce::map_task<unsigned, std::vector<unsigned> >
{
    template<typename Runtime>
    void operator()(Runtime &runtime, key_type const &key, value_type const &value) const
    {
        std::cout << "\n\n" << names[key] << "\n";

        for (auto const &v1 : value)
        {
            typename Runtime::reduce_task_type::key_type const emit_key = std::make_pair(std::min(key, v1), std::max(key, v1));

            std::cout << "    {" << names[emit_key.first] << ", " << names[emit_key.second] << "}";
            std::cout << " -> [";
            for (auto const &v2 : value)
                std::cout << " " << names[v2];
            std::cout << " ]\n";

            runtime.emit_intermediate(emit_key, value);
        }
    }
};

struct reduce_task : public mapreduce::reduce_task<std::pair<unsigned, unsigned>, std::vector<unsigned> >
{
    template<typename Runtime, typename It>
    void operator()(Runtime &runtime, key_type const &key, It it, It ite) const
    {
        if (it == ite)
            return;
        else if (std::distance(it,ite) == 1)
        {
            runtime.emit(key, *it);
            return;
        }

        // calculate the itersection of all of the vectors in (it .. ite]
        // i.e. values that are in all the vectors
        value_type results(*it);
        for (It it1=++it; it1!=ite; ++it1)
        {
            std::vector<unsigned> working_set;
            std::swap(working_set, results);
            std::set_intersection(
                working_set.cbegin(),
                working_set.cend(),
                it1->begin(),
                it1->end(),
                std::back_inserter(results));
        }

        // don't emit empty results
        if (results.size())
        {
            std::cout << "\n{ " << names[key.first] << ", " << names[key.second] << "} -> [ ";
            for (auto uid=results.cbegin(); uid!=results.cend(); ++uid)
                std::cout << names[*uid] << " ";
            std::cout << "]";

            runtime.emit(key, results);
        }
    }
};

typedef
mapreduce::job<friend_graph::map_task,
               friend_graph::reduce_task,
               mapreduce::null_combiner,
               friend_graph::datasource<friend_graph::map_task>
> job;

} // namespace friend_graph

int main(int argc, char *argv[])
{
    mapreduce::specification spec;

    if (argc > 1)
        spec.map_tasks = std::max(1, atoi(argv[1]));

    if (argc > 2)
        spec.reduce_tasks = atoi(argv[2]);
    else
        spec.reduce_tasks = std::max(1U, std::thread::hardware_concurrency());

    friend_graph::job::datasource_type datasource;

    std::cout <<"\nPage Rank analysis MapReduce..." <<std::endl;
    std::string filename="test/walther.txt";
    std::ifstream inFile;
    inFile.open(filename);
    std::vector<std::pair<int, int> > page_coordinates;
    if (!inFile) {
        std::cout << "Unable to open file";
        exit(1); // terminate with error
    }
    std::cout <<"\nHere" <<std::endl;
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
    std::cout <<"\nggg Here with max " <<max<<std::endl;
    unsigned long size=max;
    friend_graph::size_graph=size;
    friend_graph::page_rank= new unsigned*[size];
    for(unsigned i = 0; i < size; ++i)
        friend_graph::page_rank[i] = new unsigned[size];
    for(unsigned i = 0; i < size; ++i){
        for(unsigned j=0; j < size; ++j){
            friend_graph::page_rank[i][j]=0;
        }
    }
    for(auto x:page_coordinates){
        friend_graph::page_rank[x.first][x.second]=1;
    }
    friend_graph::print_graph();
    return 0;
    

    /*
    for (unsigned loop=0; loop<sizeof(friend_graph::names)/sizeof(friend_graph::names[0]); ++loop)
    {
        std::cout << loop << " " << friend_graph::names[loop] << " is friends with";
        for (unsigned friend_ndx=0; friend_ndx<sizeof(friend_graph::names)/sizeof(friend_graph::names[0]); ++friend_ndx)
        {
            if (friend_graph::is_friend(loop,friend_ndx))
                std::cout << " " << friend_graph::names[friend_ndx];
        }
        std::cout << "\n";
    }

    friend_graph::job job(datasource, spec);
    mapreduce::results result;
#ifdef _DEBUG
    job.run<mapreduce::schedule_policy::sequential<friend_graph::job> >(result);
#else
    job.run<mapreduce::schedule_policy::cpu_parallel<friend_graph::job> >(result);
#endif
    std::cout <<"\nMapReduce finished in " << result.job_runtime.count() << "s with " << std::distance(job.begin_results(), job.end_results()) << " results\n\n";

    for (auto it=job.begin_results(); it!=job.end_results(); ++it)
    {
        std::cout << friend_graph::names[it->first.first]
                  << " and "
                  << friend_graph::names[it->first.second]
                  << " are both friends with: ";

        for (unsigned const value : it->second)
            std::cout << friend_graph::names[value] << " ";
        std::cout << "\n";
    }
    */
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
