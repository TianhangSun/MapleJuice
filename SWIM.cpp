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
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <atomic>
#include <chrono>
#include <ctime>
#include <map>
#include <mutex>
#include <random>
#include <algorithm>
#include "logger.cpp"
#include "file_list.cpp"

using namespace std;

extern file_list my_file_list;

// max string length is 30
#define MaxAddr 30

// a membership list for alive node, IP => timestamp
map<string, string> alive_nodes;
mutex mtx_a;

// a dictionary for failed node, IP => cnt
map<string, int> failed_nodes;
mutex mtx_f;

// start time and it's own IP
string start_time;
string self_IP;
mutex mtx_s;

// some flags shared by all threads
atomic<bool> leave(false);
atomic<bool> finish(false);
atomic<int> loss_rate(0);

// define a type for data transmission
// includes packet type info, two alive node, two failed node
typedef struct Msg_Type
{
  char type[8];
  char time[MaxAddr];
  char live1[MaxAddr];
  char live2[MaxAddr];
  char time1[MaxAddr];
  char time2[MaxAddr];
  char fail1[MaxAddr];
  char fail2[MaxAddr];
}Msg;

// get the current time in string
string getCurrentTime(){
  time_t rawtime;
  struct tm * timeinfo;
  char buffer[50];
  time (&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(buffer,80,"%d-%m-%Y %I:%M:%S",timeinfo);
  string str(buffer);
  return str;
}

// get the whole list
vector<string> getMembershipList(){
  vector<string> member;
  mtx_a.lock();
  for(auto i: alive_nodes){
    member.push_back(i.first);
  }
  mtx_a.unlock();
  return member;
}

// get ip address
string get_self_ip(){
  mtx_s.lock();
  string ip = self_IP;
  mtx_s.unlock();
  return ip;
}

// get a random node to send
string getRandomNode(){
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  mt19937_64 gen(seed);
  
  // return introducer when there's no alive nodes
  mtx_a.lock();
  if(alive_nodes.size() == 0){
    mtx_a.unlock();
    return "172.22.148.234";
  }

  // return a random node otherwise
  uniform_int_distribution<int> dist(0, alive_nodes.size()-1);
  int cnt = 0;
  int rand = dist(gen);
  string node = "None";
  for(auto it = alive_nodes.begin(); it != alive_nodes.end(); it++){
    if(cnt == rand){
      node = it->first;
      break;
    }
    cnt ++;
  }
  mtx_a.unlock();
  return node;
}

// get a failed node to send
string getFailed(){

  // if there's no failed node, exit
  mtx_f.lock();
  if(failed_nodes.size() == 0){
    mtx_f.unlock();
    return "None";
  }

  // choose the one with min count to send
  int min = 3;
  string node = "None";
  for(auto i: failed_nodes){
    
    // if all failed node have been sent three times, return None
    if(i.second < min){
      node = i.first;
      min = i.second;
    }
  }

  // increase counter
  if(node != "None"){
    failed_nodes[node] += 1;
  }

  mtx_f.unlock();
  return node;
}

// sender function, send packet with info
string send(string type, int port, int &fd, unsigned long addr = 0){
  int sk;
  sockaddr_in remote;
  hostent *hp;
  Msg msg;
  string node;

  // get a node to send
  node = getRandomNode();
  if(node != "None"){
    sk = socket(AF_INET, SOCK_DGRAM, 0);
    fd = sk;
    remote.sin_family = AF_INET;
    remote.sin_addr.s_addr = inet_addr(node.c_str());
    remote.sin_port = port;

    // if address specified (when it's ack), change it 
    if(addr != 0){
      remote.sin_addr.s_addr = addr;
    }

    // cout << "sending: " << remote.sin_port << ", " << inet_ntoa(remote.sin_addr) << endl;

    // construct the message
    memcpy(msg.type, type.c_str(), type.size()+1);
    string live1 = getRandomNode();
    string live2 = getRandomNode();
    memcpy(msg.live1, live1.c_str(), strlen(live1.c_str())+1);
    memcpy(msg.live2, live2.c_str(), strlen(live2.c_str())+1);
    
    mtx_a.lock();
    memcpy(msg.time, start_time.c_str(), strlen(start_time.c_str())+1);
    mtx_s.lock();
    if(live1 != self_IP and live2 != self_IP){
      memcpy(msg.time1, alive_nodes[live1].c_str(), alive_nodes[live1].size()+1);
      memcpy(msg.time2, alive_nodes[live2].c_str(), alive_nodes[live2].size()+1);
    }
    mtx_s.unlock();
    mtx_a.unlock();

    string fail1 = getFailed();
    string fail2 = getFailed();
    memcpy(msg.fail1, fail1.c_str(), strlen(fail1.c_str())+1);
    memcpy(msg.fail2, fail2.c_str(), strlen(fail2.c_str())+1);
    
    // send system call, with a loss rate
    if(((double)random()/(double)RAND_MAX) > loss_rate/100.0){
      sendto(sk, &msg, sizeof(msg), 0, (sockaddr*)&remote, sizeof(remote));
    }

    // if it's request message, leave the socket, close otherwise
    if(type != "Req"){
      close(sk);
    }
  } 

  return node;
}

// delete a node from membership list
void deleteNode(string node, string self){

  // if it's itself, do nothing
  if(node == self or node == "None") return;

  // check if node in failed list
  mtx_f.lock();
  if(failed_nodes.find(node) == failed_nodes.end()){
    // add to failed list if it's not there
    failed_nodes[node] = 0;
    my_file_list.delete_node_from_file(node);

    // delete if it's in alive list
    mtx_a.lock();
    if(alive_nodes.find(node) != alive_nodes.end()){
      alive_nodes.erase(node);

      // write to log
      ofstream log;
      log.open ("log.txt", std::ofstream::out | std::ofstream::app);
      log << getCurrentTime() << " " << node << " failed or left\n";
      log.close();
    }
    mtx_a.unlock();
  }
  mtx_f.unlock();
}

// add a node in membership list
void addNode(string node, string self, string time){

  // if it's itself, do nothing
  if(node == self or node == "None") return;

  // add if it's in membership list
  mtx_a.lock();

  // check condition if we need to write to log
  if(alive_nodes.find(node) == alive_nodes.end() and self_IP == "172.22.148.234"){
    ofstream ofs;
    ofs.open ("config.txt", std::ofstream::out | std::ofstream::app);
    ofs << node << "\n";
    ofs.close();
  }
  if(alive_nodes.find(node) == alive_nodes.end()){
    ofstream log;
    log.open ("log.txt", std::ofstream::out | std::ofstream::app);
    log << getCurrentTime() << " " << node << " joined the group with time: " << time << "\n";
    log.close();
  }
  if(alive_nodes.find(node) != alive_nodes.end() and alive_nodes[node] == "" and alive_nodes[node] != time){
    ofstream log;
    log.open ("log.txt", std::ofstream::out | std::ofstream::app);
    log << getCurrentTime() << " " << node << " update timestamp to: " << time << "\n";
    log.close();
  }
  alive_nodes[node] = time;
  mtx_a.unlock();

  // delete if it's in failed list
  mtx_f.lock();
  if(failed_nodes.find(node) != failed_nodes.end()){
    failed_nodes.erase(node);
  }
  mtx_f.unlock();
}

// function to update membership list
void updateMembership(Msg msg, string peer = "None"){
  string live1(msg.live1);
  string live2(msg.live2);
  string fail1(msg.fail1);
  string fail2(msg.fail2);
  string time(msg.time);
  string time1(msg.time1);
  string time2(msg.time2);

  // get message info and call add/delete
  mtx_s.lock();
  if(peer != "None"){
    addNode(peer, self_IP, time);
  }
  addNode(live1, self_IP, time1);
  addNode(live2, self_IP, time2);
  deleteNode(fail1, self_IP);
  deleteNode(fail2, self_IP);
  mtx_s.unlock();

  // print membership list
  /*mtx_a.lock();
  cout << "\nAlive nodes list:\n";
  for(auto i: alive_nodes){
    cout << "IP: " << i.first << ",\ttime: " << i.second << "\n";
  }
  mtx_a.unlock();

  mtx_f.lock();
  cout << "\nFailed nodes list:\n";
  for(auto i: failed_nodes){
    cout << i.first << ", count: " << i.second << "\n";
  }
  mtx_f.unlock();*/
}

// ping function
void ping(){
  while(!finish){
    while(leave){
      sleep(1);
    }
    // send the Request message
    int fd;
    string node = send("Req", 6000, fd);
  
    // declare variable
    sockaddr_in remote;
    sockaddr_in local;
    socklen_t slen = sizeof(sockaddr_in);
    int msgLength;
    Msg msg;

    // create a scoket to wait for the ACK
    local.sin_family = AF_INET;
    sockaddr_in waiting_addr;
    socklen_t sslen = sizeof(sockaddr_in);
    getsockname(fd, (sockaddr*)&waiting_addr, &sslen);
    local.sin_port = waiting_addr.sin_port;
    local.sin_addr.s_addr = INADDR_ANY;

    // set a 500ms timeout
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 500000;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
      perror("Error");
    }

    // if acked, update list, delete node otherwise
    bind(fd, (sockaddr*)&local, sizeof(local));
    msgLength = recvfrom(fd, &msg, sizeof(msg), 0, (sockaddr*)&remote, &slen);
  
    //cout << "\nreceive ACK from node "<< node << " status: " << msgLength << endl;
    
    // update membership list or delete node based on ACK
    if(msgLength > 0){
      string peer(inet_ntoa(remote.sin_addr));
      updateMembership(msg, peer);
    }
    else{
      mtx_s.lock();
      deleteNode(node, self_IP);
      mtx_s.unlock();
    }
    close(fd);
    sleep(1);
  }
}

// pong function
void pong(){
  // declare variable
  int sk;
  sockaddr_in remote;
  sockaddr_in local;
  socklen_t slen = sizeof(sockaddr_in);
  int msgLength;

  // create a scoket to wait for the ACK
  sk = socket(AF_INET, SOCK_DGRAM, 0);
  local.sin_family = AF_INET;
  local.sin_port = 6000;
  local.sin_addr.s_addr = INADDR_ANY;

  // infinite loop to receive
  bind(sk, (sockaddr*)&local, sizeof(local));
  while(!finish){
    while(leave){
      sleep(1);
    }
    Msg msg;
    msgLength = recvfrom(sk, &msg, sizeof(msg), 0, (sockaddr*)&remote, &slen);

    //cout << "receiving from " << inet_ntoa(remote.sin_addr) << " status: " << msgLength << endl;

    // update membership list and send ACK
    if(msgLength > 0){
      string peer(inet_ntoa(remote.sin_addr));
      updateMembership(msg, peer);
      int fd;
      send("ACK", remote.sin_port, fd, remote.sin_addr.s_addr);
    }
  }
  close(sk);
}

void SWIM_command(){
    // command line interface
    cout << "\ntype in commend:\n1, list membership of current node: ls\n2, show the IP address and timestamp of current node: id\n";
    cout << "3, join or rejoin(when leave) the group: join\n4, voluntarily leave the group(process will not exit): leave\n";
    cout << "5, change loss rate(number in percentage): -c <loss-rate>\n6, enter a grep commend without filename: -g <grep-commend>\n";
    string commend;
    cout << "\nSWIM commend: ";
    cin >> commend;
    if(commend == "ls"){
        mtx_a.lock();
        cout << "\nAlive nodes list:\n";
        for(auto i: alive_nodes){
            cout << "IP: " << i.first << ",\ttimestamp: " << i.second << "\n";
        }
        mtx_a.unlock();
    }
    if(commend == "id"){
        mtx_a.lock();
        mtx_s.lock();
        cout << "\nIP: " << self_IP << ",\ttimestamp: " << start_time << endl;
        mtx_s.unlock();
        mtx_a.unlock();
    }
    if(commend == "join"){
        string str = getCurrentTime();
        mtx_a.lock();
        start_time = str;
        mtx_a.unlock();
        leave = false;
        ofstream log;
        log.open ("log.txt", std::ofstream::out | std::ofstream::app);
        log << "\n" << str << " current node: " << get_self_ip()  << " joined the group" << "\n";
        log.close();
    }
    if(commend == "leave"){
        leave = true;
        ofstream log;
        log.open ("log.txt", std::ofstream::out | std::ofstream::app);
        log << getCurrentTime() << " current node: " << get_self_ip()  << " leave the group" << "\n\n";
        log.close();
        mtx_a.lock();
        alive_nodes.clear();
        mtx_a.unlock();
        mtx_f.lock();
        failed_nodes.clear();
        mtx_f.unlock();
	my_file_list.clear();
    }
    if(commend == "-c"){
        int loss;
        cin >> loss;
        loss_rate = loss;
    }
    if(commend == "-g"){
        getInput();
    }
}

// main function, call others
int start_SWIM(){
  // get time
  string str = getCurrentTime();
  start_time = str;

  // get IP address
  size_t len = 100;
  char name[len];
  gethostname(name, len);
  name[len] = '\0';
  hostent *hp;
  hp = gethostbyname(name);
  struct in_addr **addr_list;
  addr_list = (struct in_addr **)hp->h_addr_list;
  string self(inet_ntoa(*addr_list[0]));
  self_IP = self;

  // if it's introducer, check the configration file
  if(self == "172.22.148.234"){
    ifstream ifs ("config.txt");
    string line;
    if (ifs.is_open()){
      while (getline(ifs,line)) {
	alive_nodes[line] = "";
      }
      ifs.close();
    }
  }  
  // create thread to ping, pong, receive grep command
  thread t1(pong);
  thread t2(ping);
  thread t3(passiveSide);

  // join thread and exit
  t1.join();
  t2.join();
  t3.join();
  return 0;
}
