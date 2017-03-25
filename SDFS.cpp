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
#include <sys/types.h>
#include <pwd.h>
#include "SWIM.cpp"

// global file list, shared with SWIM
file_list my_file_list;

#define MaxLen 30
typedef struct DFS_Msg_Type{
  char type[5];
  char filename[MaxLen];
  char rename[MaxLen];
  char node1[MaxLen];
  char node2[MaxLen];
  char node3[MaxLen];
}DFS_Msg;

typedef struct MJ_Msg_type{
  char type[5];
  char exe[MaxLen];
  char filename[MaxLen];
  char prefix[MaxLen];
}MJ_Msg;

// string to string version gethostbyaddr
/*string get_host_by_addr(string addr){
  struct sockaddr_in sa;
  char host[1024];
  char service[20];
  inet_pton(AF_INET, addr.c_str(), &(sa.sin_addr));
  getnameinfo((struct sockaddr*)&sa, sizeof(sa), host, sizeof(host), service, sizeof(service), 0);
  return string(host);
  }*/

// get string version hostname
/*string get_host_name(){
  size_t len = 1024;                                                             
  char name[len];                                                               
  gethostname(name, len);                                                       
  name[len] = '\0';
  return string(name);
  }*/

// system command with lock, so they'll be serialized
mutex s;
void delete_file(string DFS_filename);
void system_safe(string command, bool record_time = false){
  //std::chrono::time_point<std::chrono::system_clock> start, end;
  //start = std::chrono::system_clock::now();
  s.lock();
  system(command.c_str());
  s.unlock();
  //end = std::chrono::system_clock::now();
  //std::chrono::duration<double> elapsed_seconds = end-start;
  //if(record_time == true){
  //  cout << "scp transfer time: " << elapsed_seconds.count() << endl;
  //}
}

// a function to create log
mutex f;
void logging(string message){
  f.lock();
  ofstream log;
  log.open ("log.txt", std::ofstream::out | std::ofstream::app);
  log << getCurrentTime() << " " << message << "\n";
  log.close();
  f.unlock();
}

// construct and send a message (UDP)
void send_message(string type, string dest, string filename = "", string rename = "",
		  string node1 = "", string node2 = "", string node3 = "", int port = 0){
  int sk;
  sockaddr_in remote;
  DFS_Msg msg;

  // construct the message
  memcpy(msg.type, type.c_str(), type.size()+1);
  memcpy(msg.filename, filename.c_str(), filename.size()+1);
  memcpy(msg.rename, rename.c_str(), rename.size()+1);
  memcpy(msg.node1, node1.c_str(), node1.size()+1);
  memcpy(msg.node2, node2.c_str(), node2.size()+1);
  memcpy(msg.node3, node3.c_str(), node3.size()+1);

  // send the message
  sk = socket(AF_INET, SOCK_DGRAM, 0);
  remote.sin_family = AF_INET;
  remote.sin_addr.s_addr = inet_addr(dest.c_str());
  remote.sin_port = 10086;

  if(port != 0){
    remote.sin_port = port;
    MJ_Msg mj_msg;
    memcpy(mj_msg.type, "ack", 4);
    sendto(sk, &mj_msg, sizeof(mj_msg), 0, (sockaddr*)&remote, sizeof(remote));
    close(sk);
    return;
  } else{
    sendto(sk, &msg, sizeof(msg), 0, (sockaddr*)&remote, sizeof(remote));
    close(sk);
  }
}

// put a file in DFS
void put_file(string local_filename, string DFS_filename,
	      bool replica = false, bool once = false, string node1 = ""){
  // get two random nodes (IP)
  string node0 = get_self_ip();
  if(once == false){
    node1 = getRandomNode();
    delete_file(DFS_filename);
  }
  string node2 = node1;
  int cnt = 0;
  while(node2 == node1){
    node2 = getRandomNode();
    cnt++;
    if(cnt >= 10){
      cout << "put failed, not enough node" << endl;
      return;
    }
  }
  string to_check = "./" + local_filename;
  if(access(to_check.c_str(), F_OK) == -1){
    cout << "file not exist" << endl;
    return;
  }

  // store in it's own DFS folder, and add to local file list
  if(replica == false){
    string to_call = "cp -r " + local_filename + " ~/DFS_files/" + DFS_filename;
    system_safe(to_call);
    logging("put " + local_filename + " to SDFS as " + DFS_filename);
    logging("put file " + DFS_filename + " on this machine");
  }
  my_file_list.add_file_to_dic(DFS_filename, node0, node1, node2);

  // transfer to two other nodes and send the message
  string re_replica = " (put)";
  if(replica == true){
    re_replica = " (re-replica)";
  }
  if(once == false){
    string transfer1 = "scp -r -o StrictHostKeyChecking=no ~/DFS_files/" + DFS_filename + " " + node1 + ":~/DFS_files/";
    system_safe(transfer1);
    logging("transfer " + DFS_filename + " to " + node1 + re_replica);
  }
  string transfer2 = "scp -r -o StrictHostKeyChecking=no ~/DFS_files/" + DFS_filename + " " + node2 + ":~/DFS_files/";
  system_safe(transfer2);
  logging("transfer " + DFS_filename + " to " + node2 + re_replica);
  logging("file " + DFS_filename + " currently on " + node0 + " " + node1 + " " + node2);

  send_message("put", node1, DFS_filename, "", node0, node1, node2);
  send_message("put", node2, DFS_filename, "", node0, node1, node2);
}

// function to spread the get message
int get_file(string DFS_filename, string local_filename){
  vector<string> members = getMembershipList();
  int exist = -1;
  struct passwd *pw = getpwuid(getuid());
  const char *homedir = pw->pw_dir;
  string file(homedir);
  //file += "/GET_files/" + local_filename;

  map<string,vector<string>> my_files = my_file_list.get_current_files();
  if(my_files.find(DFS_filename) != my_files.end()){
    string cp = "cp -r "+ file +"/DFS_files/"+ DFS_filename +" "+ file +"/GET_files/"+ local_filename;
    system_safe(cp.c_str());
    return 1;
  }

  file += "/GET_files/" + local_filename;
  // if file not transfered, resend the message
  int cnt = 0;
  while(exist == -1){
    for(auto i: members){
      send_message("get", i, DFS_filename, local_filename);
    }
    sleep(1);
    exist = access(file.c_str(), F_OK);

    // give up after several tries
    if(cnt >= 3 and exist == -1){
      cout << "file not available" << endl;
      logging("get file " + DFS_filename + " failed");
      return -1;
    }
    cnt++;
  }
  logging("get file " + DFS_filename + " succeeded, store to local as " + local_filename);
  return 0;
}

// spread a delete message
void delete_file(string DFS_filename){
  vector<string> members = getMembershipList();
  for(auto i: members){
    send_message("del", i, DFS_filename);
  }
  string to_delete = "rm -rf ~/DFS_files/" + DFS_filename;
  system_safe(to_delete);
  my_file_list.delete_file_from_dic(DFS_filename);
}

void list_file(string DFS_filename){
  vector<string> members = getMembershipList();
  for(auto i: members){
    send_message("ls", i, DFS_filename);
  }
}

// waiting for put command
void waiting_put(DFS_Msg msg){
  string filename(msg.filename);
  string node1(msg.node1);
  string node2(msg.node2);
  string node3(msg.node3);
  my_file_list.add_file_to_dic(filename, node1, node2, node3);
  logging("put file " + filename + " on this machine");
  logging("file " + filename + " currently on " + node1 + " " + node2 + " " + node3);
}

// waiting for get message
void waiting_get(DFS_Msg msg, string addr){
  string filename(msg.filename);
  string rename(msg.rename);
  map<string, vector<string>> files = my_file_list.get_current_files();

  // if it have the file and it has largest id, send
  if(files.find(filename) != files.end()){
    vector<string> file_location = files[filename];
    string ip = get_self_ip();
    bool largest = true;
    for(auto i: file_location){
      if(i > ip){
	largest = false;
      }
    }
    if(largest == true){
      string to_send = "scp -r -o StrictHostKeyChecking=no ~/DFS_files/" + filename + " " + addr + ":~/GET_files/" + rename;
      system_safe(to_send, true);
      logging("transfer " + filename + " to " + addr + " as " + rename + " (get)");
      send_message("ack", addr, "","","","","", 12580);
    }
  }
}

// waiting for delete message
void waiting_del(DFS_Msg msg){
  string filename(msg.filename);
  string to_delete = "rm -rf ~/DFS_files/" + filename;
  system_safe(to_delete);
  my_file_list.delete_file_from_dic(filename);
}

void waiting_list(DFS_Msg msg, string addr){
  string type(msg.type);
  string filename(msg.filename);
  if(type == "ret"){
    cout << "\n" << msg.node1 << " " << msg.node2 << " " << msg.node3 << endl;
  } else if (type == "ls"){
    map<string, vector<string>> files = my_file_list.get_current_files();
    if(files.find(filename) != files.end()){
      vector<string> file_location = files[filename];
      string ip = get_self_ip();
      bool largest = true;
      string node1, node2, node3;
      int cnt = 1;
      for(auto i: file_location){
	if(i > ip){
	  largest = false;
	}
	if(cnt == 1){
	  node1 = i;
	} else if(cnt == 2){
	  node2 = i;
	} else{
	  node3 = i;
	}
	cnt++;
      }
      if(largest == true){
	send_message("ret", addr, filename, "", node1, node2, node3);
      }
    }
  }
}

// waiting function, call others
void waiting_side(){
  int sk;
  sockaddr_in remote;
  sockaddr_in local;
  socklen_t slen = sizeof(sockaddr_in);
  int msgLength;

  sk = socket(AF_INET, SOCK_DGRAM, 0);
  local.sin_family = AF_INET;
  local.sin_port = 10086;
  local.sin_addr.s_addr = INADDR_ANY;

  // loop for new messages, call needed functions
  bind(sk, (sockaddr*)&local, sizeof(local));
  while(!finish){
    DFS_Msg msg;
    msgLength = recvfrom(sk, &msg, sizeof(msg), 0, (sockaddr*)&remote, &slen);
    //cout << "receiving from " << inet_ntoa(remote.sin_addr) << " status: " << msgLength << endl;      
    string return_address = inet_ntoa(remote.sin_addr);
    if(msgLength > 0){
      string type(msg.type);
      if(type == "put"){
	waiting_put(msg);
      } else if(type == "get"){
	waiting_get(msg, return_address);
      } else if(type == "del"){
	waiting_del(msg);
      } else if(type == "ls" or type == "ret"){
	waiting_list(msg, return_address);
      }
    }
  }
  close(sk);
}

// check if there're enough replica
void check_replica(){
  while(!finish){
    sleep(3);
    map<string, vector<string>> files = my_file_list.get_current_files();
    for(auto i: files){

      // if it's the only node with the replica, choose two
      if(i.second.size() == 1){
	put_file("", i.first, true);
      }

      // else, make new replica only if it's the larger one, and send message to both
      else if(i.second.size() == 2){
	string ip = get_self_ip();
	for(auto j: i.second){
	  if(ip > j){
	    put_file("", i.first, true, true, j);
	  }
	}  
      }
    }
  }
}

// main function
/*int main(){
  system_safe("mkdir -p ~/DFS_files");
  system_safe("mkdir -p ~/GET_files");

  thread t1(start_SWIM);
  thread t2(waiting_side);
  thread t3(check_replica);

  cout << "\ntype in command:\n1, put localfilename sdfsfilename\n2, get sdfsfilename localfilename";
  cout << "\n3, delete sdfsfilename\n4, ls sdfsfilename: list all machine this file is stored";
  cout << "\n5, store: list all files stored on this machine\n6, SWIM: execute SWIM command\n";

  while(!finish){
    string command;
    cout << "\nSDFS command: ";
    cin >> command;
    if(command == "put"){
      string localfilename, sdfsfilename;
      cin >> localfilename >> sdfsfilename;
      thread(put_file, localfilename, sdfsfilename, false, false, "").detach();
    }
    if(command == "get"){
      string sdfsfilename, localfilename;
      cin >> sdfsfilename >> localfilename;
      thread(get_file, sdfsfilename, localfilename).detach();
    }
    if(command == "delete"){
      string sdfsfilename;
      cin >> sdfsfilename;
      thread(delete_file, sdfsfilename).detach();
      logging("delete file " + sdfsfilename + " spreading message");
    }
    if(command == "ls"){
      string sdfsfilename;
      cin >> sdfsfilename;
      list_file(sdfsfilename);
    }
    if(command == "store"){
      my_file_list.print();
    }
    if(command == "SWIM"){
      SWIM_command();
    }
    cout << endl;
  }
  t1.join();
  t2.join();
  t3.join();
  }*/
