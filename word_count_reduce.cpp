#include <stdio.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <map>

using namespace std;

int main(int argc, char ** argv){
  string prefix(argv[1]);
  string filename(argv[2]);
  ifstream in_file(prefix);
  map<string, int> word_count;
  
  // read each line and process
  cout << filename << ", "<< prefix << endl;
  string line;
  while(getline(in_file, line)){
    istringstream iss(line);
    string key, value;
    iss >> key;
    word_count[key] = 0;
    while(iss >> value){
      word_count[key] += stoi(value);
    }
  }

  // write to file
  system("mkdir -p tmp_output");
  ofstream ofs("__"+ filename);
  for(auto i: word_count){
    ofs << i.first << " " << i.second << "\n";
  }
  ofs.close();
  string mv_keyset = "mv __"+ filename + " ./tmp_output";
  system(mv_keyset.c_str());
}
