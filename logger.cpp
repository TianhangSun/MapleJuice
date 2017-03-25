#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <string.h>
#include <fstream>
#include <vector>
#include <unistd.h>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>
#include <pthread.h>
#include <thread>
#include <chrono>
#include <ctime>
#include <map>
#include <mutex>
#include <atomic>

using namespace std;

// global line count and output buffer as well as lock for them
map<string, string> output;
long total_line_cnt;
mutex mtx;

extern atomic<bool> finish;

// process one individual request
void processRequest(int sk2){
  // declare variables

  int BUFLEN = 100;
  char localBuf[BUFLEN];
  int mesgLength;

  // get the pattern
  mesgLength = read(sk2, localBuf, BUFLEN);
  localBuf[mesgLength] = '\0';
  //cout << "\nPassive side: Incoming pattern: " << localBuf << endl;

  /*
   * generate the file and return back
   *
   */
  string pattern(localBuf);
  
  // save the result into file and read the file
  string to_run = pattern + " ./log.txt >> out.txt";
  
  system("rm ./out.txt");
  system(to_run.c_str());

  string line;
  ifstream myfile ("out.txt");
  long cnt = 0;
  if (myfile.is_open()){
    while (getline(myfile,line)) {
      // cout << line << '\n';
      line += '\n';
      cnt ++;
      // write to socket
      write(sk2, line.c_str(), line.size());
    }
    myfile.close();
  }

  // add local line count and a end of file flag
  string line_cnt = "\nLine_count: " + to_string(cnt) + "\n";
  write(sk2, line_cnt.c_str(), line_cnt.size());
  write(sk2, "EOFFLAG", 8);
  close(sk2);
}

// waiting for incoming request
void passiveSide(){
  // declare variable needed
  int sk1;
  sockaddr_in local;
  int localLength = sizeof(local);
  socklen_t slen = sizeof(sockaddr_in);

  // set up socket
  sk1 = socket(AF_INET,SOCK_STREAM,0);
  local.sin_family = AF_INET;
  local.sin_addr.s_addr = INADDR_ANY;
  local.sin_port = 5000;

  // bind and get socket name, and queue up to 5 request
  bind(sk1,(struct sockaddr*)&local, localLength);
  getsockname(sk1,(struct sockaddr*)&local, &slen);
  listen(sk1,5);

  // main thread will loop forever, receiving requests
  while(!finish){
    int sk2 = accept(sk1, (struct sockaddr*)0, 0);
    if(sk2 == -1){
      cout << "\nPassive side: accept failed!\n" << endl;
    } else{
      // let another independent thread process the request
      thread(processRequest, sk2).detach();
    }
  }
  close(sk1);
}

// send the request to others, and receive result
void activeSide(string address, string pattern){
  // declare variables
  int BUFLEN = 64000;
  int sk;
  sockaddr_in remote;
  char buf[BUFLEN];
  hostent *hp;
  int mesgLength;
  
  // set up the socket
  sk = socket(AF_INET,SOCK_STREAM,0);
  remote.sin_family = AF_INET;
  hp = gethostbyname(address.c_str());
  memcpy(&remote.sin_addr, hp->h_addr, hp->h_length);
  remote.sin_port = 5000;

  // connect to others, store error message if error
  if(connect(sk, (struct sockaddr*)&remote, sizeof(remote)) < 0){
    mtx.lock();
    output[address] = "Request side: connection error!";
    mtx.unlock();
    close(sk);
    return;
  }
  
  // send the pattern
  write(sk, pattern.c_str(), strlen(pattern.c_str()));

  // receive result, loop until receive EOFFLAG
  string tmp = "";
  long line_cnt = 0;
  while(!finish){
    mesgLength = read(sk, buf, BUFLEN);
    buf[mesgLength] = '\0';
    // cout << buf;
    string end(buf);
    tmp += end;

    // store line count (since c++ socket does not have readline option)
    if (end.find("Line_count:") != string::npos){
      long start = end.find("Line_count:");
      long stop = end.find("\n", start+1);
      line_cnt = stol(end.substr(start+12, stop-start-12));
    }

    // end the loop if receive end of file flag
    if (end.find("EOFFLAG") != string::npos)
      break;

    // store error message if the node if not responding
    if (mesgLength == 0){
      mtx.lock();
      output[address] = "Node not responding!";
      mtx.unlock();
      close(sk);
      return;
    }
  }

  // store result into global buffer
  mtx.lock();
  output[address] = tmp;
  total_line_cnt += line_cnt;
  mtx.unlock();
  close(sk);
}

// get number and address of nodes from text file
vector<string> getNodes(){
  ifstream line("nodes.txt");
  string num, node;
  vector<string> nodes;

  line >> num;
  for(int i=0; i<stoi(num); i++){
    line >> node;
    nodes.push_back(node);
  }
  return nodes;
}

void getInput(){
  string commend, pattern;
  vector<string> nodes;
  nodes = getNodes();

  // get the input
  getline(cin, pattern);
  //cout << "pattern: " << pattern << endl;
      
  mtx.lock();
  output.clear();
  total_line_cnt = 0;
  mtx.unlock();

  // create 10 thread to handle each machine (including itself)
  thread t[10];
  for(int i=0; i<nodes.size(); i++){
    t[i] = thread(activeSide, nodes[i], pattern);
  }
  for(int i=0; i<nodes.size(); i++){
    t[i].join();
  }

  mtx.lock();
  for(auto i: output){
    if(i.second != "Request side: connection error!"){
      cout << "\nResult from node: " << i.first << endl << i.second << endl;
    }
  }
  mtx.unlock();
  cout << "\nTotal line count: " << total_line_cnt << endl;
}
