#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>

using namespace std;

int main(int argc, char ** argv){
  string filename(argv[1]);
  string prefix(argv[2]);
  ifstream ifs(filename);
  unordered_map<string, int> word_count;

  // get a word
  string tmp_word;
  while(ifs >> tmp_word){
    string word = "";
    for(auto i: tmp_word){
      if(isalnum(i) or i == '-'){
	word += i;
      } else if(word != ""){
	if(word_count.find(word) != word_count.end()){
	  word_count[word] ++;
	} else{
	  word_count[word] = 1;
	}
        word = "";
      }
    }
    if(word != ""){
      if(word_count.find(word) != word_count.end()){
	word_count[word] ++;
      } else{
	word_count[word] = 1;
      }
    }
  }
  
  // write to file
  system("mkdir -p tmp_output");
  ofstream key_set("__"+ prefix +"_keyset");
  for(auto i: word_count){
    key_set << i.first << " " << i.second << "\n";
  }
  key_set.close();
  string mv_keyset = "mv __" + prefix + "_keyset ./tmp_output";
  system(mv_keyset.c_str());
}
