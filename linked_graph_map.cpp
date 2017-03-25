#include <stdio.h>
#include <iostream>
#include <string>
#include <fstream>
using namespace std;

int main(int argc, char **argv){
  string filename(argv[1]);
  string prefix(argv[2]);
  ifstream ifs(filename);
  ofstream ofs("__"+ prefix +"_keyset");

  // reverse order
  string from, to;
  while(ifs >> from >> to){
    ofs << to << " " << from << "\n";
  }
  ifs.close();
  ofs.close();

  // write to file
  system("mkdir -p tmp_output");
  string mv = "mv __" + prefix + "_keyset ./tmp_output";
  system(mv.c_str());
  return 0;
}
