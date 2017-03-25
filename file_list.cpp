#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <algorithm>

using namespace std;

struct file_list{
  // a dictionary from filename to the nodes that contain the file
  map<string,vector<string>> my_files;
  mutex mtx;

  // add a file into the dictionary
  void add_file_to_dic(string file, string node1, string node2, string node3){

    // construct a vector
    vector<string> node_vec;
    node_vec.push_back(node1);
    node_vec.push_back(node2);
    node_vec.push_back(node3);

    // assign vector to dictionary
    mtx.lock();
    my_files[file] = node_vec;
    mtx.unlock();
  }

  // add a replica node to an exsiting file
  void add_node_to_file(string node, string file){
    mtx.lock();
    my_files[file].push_back(node);
    mtx.unlock();
  }

  // delete a file
  void delete_file_from_dic(string filename){
    mtx.lock();
    my_files.erase(filename);
    mtx.unlock();
  }

  // when delete a node, update the dictionary
  void delete_node_from_file(string node){
    mtx.lock();

    // loop over each file
    for(auto i: my_files){

      // loop over each node
      int to_erase = -1;
      for(int j=0; j<i.second.size(); j++){
	if(i.second[j] == node){
	  to_erase = j;
	}
      }

      // erase if needed
      if(to_erase != -1){
	my_files[i.first].erase(my_files[i.first].begin() + to_erase);
      }
    }
    mtx.unlock();
  }

  // get the files
  map<string, vector<string>> get_current_files(){
    mtx.lock();
    map<string, vector<string>> current_files = my_files;
    mtx.unlock();
    return current_files;
  }
  
  void clear(){
    mtx.lock();
    my_files.clear();
    mtx.unlock();
  }

  // for debugging
  void print(){
    mtx.lock();
    cout << "\nFiles on this machine: \n";
    for(auto i: my_files){
      cout << i.first << ": ";
      for(auto j: i.second){
	cout << j << " ";
      }
      cout << endl;
    }
    mtx.unlock();
  }
};
