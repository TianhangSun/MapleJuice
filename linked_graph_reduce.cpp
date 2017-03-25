#include <stdio.h>
#include <string>
#include <stdlib.h>
using namespace std;

int main(int argc, char **argv){
  string prefix(argv[1]);
  string filename(argv[2]);
 
  // most stupid reduce ever, did nothing :)
  system("mkdir -p tmp_output");
  string mv = "mv " + prefix + " ./tmp_output/__" + filename;
  system(mv.c_str());
  return 0;
}
