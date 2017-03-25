#include <queue>
#include <map>
#include <string>
#include <mutex>
#include <set>
using namespace std;

// a scheduler class
class scheduler{
private:

  // multiple queues for incoming maple, juice jobs
  mutex mtx;
  queue<map<string,string>> maple_queue;
  queue<map<string,string>> juice_queue;
  map<string,int> worker_info;

public:
  // add a single worker
  void add_worker(string name){
    mtx.lock();
    if(worker_info.find(name) == worker_info.end()){
      worker_info[name] = 0;
    }
    mtx.unlock();
  }
  
  // delete a worker
  int delete_worker(string name){
    mtx.lock();
    worker_info.erase(name);
    mtx.unlock();
    return 0;
  }

  // receive a single ack
  void ack_received(string name){
    mtx.lock();
    worker_info[name] += 1;
    mtx.unlock();
  }

  // return the number of ack
  int ack_sum(){
    mtx.lock();
    int cnt = 0;
    for(auto i: worker_info){
      cnt += i.second;
    }
    mtx.unlock();
    return cnt;
  }

  // clear all worker in the system
  void clear_worker(){
    mtx.lock();
    worker_info.clear();
    mtx.unlock();
  }

  // function to add a maple job
  void add_maple_job(string exe, string tmp_prefix, string DFS_filename, int num, int upload){
    map<string,string> job;
    job["type"] = "maple";
    job["exe"] = exe;
    job["tmp_prefix"] = tmp_prefix;
    job["DFS_filename"] = DFS_filename;
    job["num"] = to_string(num);
    job["upload"] = to_string(upload);

    mtx.lock();
    maple_queue.push(job);
    mtx.unlock();
  }

  // function to add a juice job
  void add_juice_job(string exe, string tmp_prefix, string dest_filename, int num, int del){
    map<string,string> job;
    job["type"] = "juice";
    job["exe"] = exe;
    job["tmp_prefix"] = tmp_prefix;
    job["dest_filename"] = dest_filename;
    job["num"] = to_string(num);
    job["del"] = to_string(del);

    mtx.lock();
    juice_queue.push(job);
    mtx.unlock();
  }

  // function to get the job to run
  map<string,string> get_job(){
    mtx.lock();
    if(maple_queue.size() != 0){
      map<string,string> job = maple_queue.front();
      mtx.unlock();
      return job;
    } else if(juice_queue.size() != 0){
      map<string,string> job = juice_queue.front();
      mtx.unlock();
      return job;
    } else{
      mtx.unlock();
      map<string,string> job;
      return job;
    }
  }

  // function to delete a job in queue (when finished)
  void delete_job(string type){
    mtx.lock();
    if(type == "maple"){
      maple_queue.pop();
    } else if(type == "juice"){
      juice_queue.pop();
    }
    mtx.unlock();
  }

};
