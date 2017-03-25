#include <memory>
#include <unordered_map>
#include <functional>
#include <sstream>
#include "SDFS.cpp"
#include "scheduler.cpp"
using namespace std;

// 0->not getting file, 1->getting, not yet received, 2->received
atomic<int> get_file_status(0);
atomic<bool> start(false);
scheduler schedule_queues;
mutex run;

// a function to split file into pieces
void split_file(string filename, int num, bool local = false){
  struct passwd *pw = getpwuid(getuid());
  const char *homedir = pw->pw_dir;
  string file(homedir);
  file += "/GET_files/local_" + filename;
  if(local == true){
    file = filename;
  }
  string line;
  ifstream in_file(file);

  // make num of output files
  vector<shared_ptr<ofstream>> out_files;  
  for(int i=0; i<num; i++){
    out_files.push_back(make_shared<ofstream>(filename + "_" + to_string(i)));
  }

  int cnt = 0;
  while(getline(in_file, line)) {
    (*out_files[cnt%num]) << line << '\n';
    cnt ++;
  }

  system("rm -rf ./tmp_files");
  system("mkdir -p tmp_files");
  in_file.close();
  for(int i=0; i<num; i++){
    out_files[i]->close();
    //put_file(filename + "_" + to_string(i), "SDFS_" + filename + "_" + to_string(i));
    string mv = "mv " + filename + "_" + to_string(i) + " ./tmp_files";
    system(mv.c_str());
  }
  cout << "split file finished" << endl;
  //string del = "rm -f " + filename + "_*";
  //system(del.c_str());
}

// maple worker
void maple_phase_worker(MJ_Msg msg, string addr, int port){
  run.lock();
  cout << "maple worker started" << endl;
  string exe(msg.exe);
  string filename(msg.filename);
  string prefix(msg.prefix);
  logging("start maple task, exe: " + exe + ", from: " + filename + " to: " + prefix);
  
  // run maple exe and send the file back
  string to_run = "~/intermediate_files/" + exe + " ~/intermediate_files/" + filename + " " + prefix;
  if(system(to_run.c_str()) != 0) {
    logging("maple failed");
    return;
  }
  string send_key_set = "scp -r -o StrictHostKeyChecking=no ./tmp_output/__"
    + prefix + "_keyset " + addr + ":~/intermediate_files/__" + prefix + "_keyset_" + filename;
  if(system(send_key_set.c_str()) != 0) {
    logging("maple failed");
    return;
  }
  for(int i=0; i<3; i++){
    send_message("ack",addr,"","","","","",port);
    sleep(1);
  }
  logging("maple success");
  run.unlock();
}

// juice worker
void juice_phase_worker(MJ_Msg msg, string addr, int port){
  run.lock();
  cout << "juice worker started" << endl;
  string exe(msg.exe);
  string filename(msg.filename);
  string prefix(msg.prefix);
  logging("start juice task, exe: " + exe + ", from: " + filename + " to: " + prefix);

  // run juice exe and send the file back
  string to_run = "~/intermediate_files/" + exe + " ~/intermediate_files/" + filename + " " + prefix;
  if(system(to_run.c_str()) != 0) {
    logging("juice failed");
    return;
  }
  string send_result = "scp -o StrictHostKeyChecking=no ./tmp_output/__"
    + prefix + " " + addr + ":~/intermediate_files/__" + prefix + filename;
  if(system(send_result.c_str()) != 0) {
    logging("juice failed");
    return;
  }
  for(int i=0; i<3; i++){
    send_message("ack",addr,"","","","","",port);
    sleep(1);
  }
  run.unlock();
}

// send request
void send_request(string node, string exe, string tmp_prefix, string filename, string type){
  // send exe and files to the node
  string send_exe = "scp -r -o StrictHostKeyChecking=no " + exe + " " + node + ":~/intermediate_files/";
  system(send_exe.c_str());
  string send_file = "scp -r -o StrictHostKeyChecking=no ./tmp_files/" + filename + " " + node + ":~/intermediate_files/";
  system(send_file.c_str());
  logging("sent " + type + " request to " + node);

  // send a message to start maple task
  int sk;
  sockaddr_in remote;
  sockaddr_in local;
  socklen_t slen = sizeof(sockaddr_in);
  MJ_Msg msg;

  // construct the message
  memcpy(msg.type, type.c_str(), type.size()+1);
  memcpy(msg.exe, exe.c_str(), exe.size()+1);
  memcpy(msg.filename, filename.c_str(), filename.size()+1);
  memcpy(msg.prefix, tmp_prefix.c_str(), tmp_prefix.size()+1);

  // send the message
  sk = socket(AF_INET, SOCK_DGRAM, 0);
  remote.sin_family = AF_INET;
  remote.sin_addr.s_addr = inet_addr(node.c_str());
  remote.sin_port = 12580;
  sendto(sk, &msg, sizeof(msg), 0, (sockaddr*)&remote, sizeof(remote));
  
  struct timeval tv;
  tv.tv_sec = 600;
  tv.tv_usec = 0;
  if (setsockopt(sk, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
    perror("Error");
  }
  
  cout << "waiting for " << type << " ack" << endl;
  // use this socket to wait for ack
  local.sin_family = AF_INET;
  local.sin_addr.s_addr = INADDR_ANY;

  MJ_Msg ret_msg;
  bind(sk, (sockaddr*)&local, sizeof(local));
  int msgLength = recvfrom(sk, &ret_msg, sizeof(ret_msg), 0, (sockaddr*)&remote, &slen);
  cout << "receiving from " << inet_ntoa(remote.sin_addr) << " status: " << msgLength << endl;
  close(sk);
  
  // if received ack correctly, mark it
  if(msgLength > 0) {
    logging("receive ack from " + node + " successfully");
    schedule_queues.ack_received(node);
  } else{
    logging("didn't receive ack from " + node + ", time out");
  }
}

// the re-schedule function
void reschedule(vector<string> active_members, string exe, string filename, string tmp_prefix, string type){
 
  // loop until received enough ack
  while(schedule_queues.ack_sum() < active_members.size()){
    sleep(1);
    vector<string> members = getMembershipList();
    for(int i=0; i<active_members.size(); i++){

      // if a member is dead, re-schedule
      if(find(members.begin(), members.end(), active_members[i]) == members.end()){
        string node = getRandomNode();
	schedule_queues.delete_worker(active_members[i]);
	schedule_queues.add_worker(node);
	logging("re-schedule work of node " + active_members[i] + " to node " + node);
	if(type == "map"){
	  thread(send_request, node, exe, tmp_prefix, filename +"_"+ to_string(i), "map").detach();
	} else{
          thread(send_request, node, exe, filename, "__"+ tmp_prefix +"_"+ to_string(i), "juc").detach();
        }
	active_members[i] = node;
      }
    }
  }
}

// maple master logic
void maple_phase_master(string exe, string tmp_prefix, string DFS_filename, int num, int upload = 0){
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();
  logging("maple phase started");
  get_file_status = 1;
  string dir;
  if(upload > 1){
    dir = "dir_";
  }
  
  // get file/directory from SDFS
  int cont = get_file(DFS_filename, dir + "local_" + DFS_filename);
  if(cont == -1){
    cout << "maple failed, file not exist" << endl;
    return;
  } else if(cont == 1){
    get_file_status = 2;
  }
  while(get_file_status != 2){
    sleep(1);
  }
  get_file_status = 0;

  // if directory, merge files in it
  if(upload > 1){
    struct passwd *pw = getpwuid(getuid());
    const char *homedir = pw->pw_dir;
    string file_dir(homedir);
    file_dir += "/GET_files/dir_local_" + DFS_filename;
    string merge = "cat "+ file_dir + "/* > " + DFS_filename;
    system(merge.c_str());
  }

  // check if there're enough worker
  vector<string> members = getMembershipList();
  if(members.size() < 1){
    cout << "not enough workers" << endl;
    return;
  } else if(num > members.size()){
    num = members.size();
  } else if(num < 1){
    num = 1;
  }

  //cout << num << endl;
  split_file(DFS_filename, num, upload > 1);

  // create threads to handle each connection
  vector<string> active_members;
  schedule_queues.clear_worker();
  for(int i=0; i<num; i++){
    thread(send_request, members[i], exe, tmp_prefix, DFS_filename +"_"+ to_string(i), "map").detach();
    active_members.push_back(members[i]);
    schedule_queues.add_worker(members[i]);
  }
  cout << "maple request sent" << endl;
  
  // create a thread to re-schedule
  thread t(reschedule, active_members, exe, DFS_filename, tmp_prefix, "map");
  t.join();

  cout << "map tasks finished" << endl;
  struct passwd *pw = getpwuid(getuid());
  const char *homedir = pw->pw_dir;
  string home(homedir);

  // the master will merge the files
  unordered_map<string, vector<string>> key_value_pairs;
  for(int i=0; i<num; i++){
    ifstream ifs(home + "/intermediate_files/__" + tmp_prefix + "_keyset_" + DFS_filename + "_" + to_string(i));
    string key, value;
    while(ifs >> key >> value){
      key_value_pairs[key].push_back(value);
    }
    ifs.close();
  }
  string rm = "rm -rf "+home + "/intermediate_files/__" + tmp_prefix + "_keyset_" + DFS_filename + "_*";
  system_safe(rm.c_str());

  ofstream kvp("__"+tmp_prefix);
  for(auto i: key_value_pairs){
    kvp << i.first << " ";
    for(auto j: i.second){
      kvp << j << " ";
    }
    kvp << "\n";
  }
  cout << "num of keys: " << key_value_pairs.size() << endl;
  kvp.close();
  put_file("__"+tmp_prefix, tmp_prefix);
  
  // upload files for each key if needed
  if(upload % 2 == 1){
    for(auto i: key_value_pairs){
      ofstream ofs(tmp_prefix+"_"+i.first+".txt");
      ofs << i.first << " ";
      for(auto j: i.second){
	ofs << j << " ";
      }
      ofs.close();
      string mv = "mv " + tmp_prefix + "_" + i.first + ".txt ./tmp_files";
      system(mv.c_str());
      cout << "file generated for key: " << i.first << endl;
    }
    put_file("tmp_files", "tmp_"+tmp_prefix);
  }
  rm = "rm -rf ./tmp_files/"+tmp_prefix+"_*";
  system_safe(rm.c_str());
  //string mv = "mv __" + tmp_prefix + " ./tmp_files";
  //system(mv.c_str());
  cout << "map phase finished" << endl;
  logging("maple phase finished");
  end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed_seconds = end-start;
  cout << "maple time: " << elapsed_seconds.count() << endl; 
}

void juice_phase_master(string exe, string tmp_prefix, string dest_filename, int num, int del){
    std::chrono::time_point<std::chrono::system_clock> start, end;
    start = std::chrono::system_clock::now();
    logging("juice phase started");
    vector<string> members = getMembershipList();
    if(members.size() < 1){
        cout << "not enough workers" << endl;
        return;
    } else if(num > members.size()){
        num = members.size();
    } else if(num < 1){
        num = 1;
    }
    
    string rm;
    split_file("__"+tmp_prefix, num, true);
    //cout << num << endl;
    
    // create threads to handle each connection
    schedule_queues.clear_worker();
    vector<string> active_members;
    for(int i=0; i<num; i++){
      thread(send_request, members[i], exe, dest_filename, "__"+ tmp_prefix +"_"+ to_string(i), "juc").detach();
      active_members.push_back(members[i]);
      schedule_queues.add_worker(members[i]);
    }
    cout << "juice request sent" << endl;

    // create thread to handle re-schedule
    thread t(reschedule, active_members, exe, dest_filename, tmp_prefix, "juc");
    t.join();

    rm = "rm -rf ./tmp_files/__" + tmp_prefix + "*";
    system_safe(rm.c_str());
    
    cout << "juice tasks finished" << endl;
    struct passwd *pw = getpwuid(getuid());
    const char *homedir = pw->pw_dir;
    string home(homedir);
    
    // merge files
    map<string, vector<string>> key_value_pairs;
    for(int i=0; i<num; i++){
        ifstream ifs(home + "/intermediate_files/__" + dest_filename + "__" + tmp_prefix + "_" + to_string(i));
        string key, value, line;
        while(getline(ifs, line)){
	  //key_value_pairs[key] = value;
	  istringstream iss(line);
	  iss >> key;
	  while(iss >> value){
	    key_value_pairs[key].push_back(value);
	  }
        }
        ifs.close();
    }

    rm = "rm -rf "+home + "/intermediate_files/__" + dest_filename + "__" + tmp_prefix + "_*";
    system_safe(rm.c_str());
    
    ofstream kvp("__"+ dest_filename +".txt");
    for(auto i: key_value_pairs){
      kvp << i.first << " " ;
      for(auto j: i.second){
	kvp << j << " ";
      }
      kvp << "\n";
    }
    kvp.close();
    put_file("__"+dest_filename+".txt", dest_filename);
    rm = "rm -rf __" + dest_filename + ".txt __"+tmp_prefix;
    system_safe(rm.c_str());
    //string mv = "mv __" + dest_filename + ".txt ./tmp_files";
    //system(mv.c_str());
    if(del == 1){
      delete_file(tmp_prefix);
    }
    cout << "num of keys: " << key_value_pairs.size() << endl;
    cout << "juice phase finished" << endl;
    logging("juice phase finished");
    end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end-start;
    cout << "juice time: " << elapsed_seconds.count() << endl; 
}

// function to schedule maple/juice tasks
void schedule_tasks(){
  while(!finish){
    sleep(1);
    // if not start, do nothing
    while(start == false){
      sleep(1);
    }
    
    // get a job from queue, finish all maple before start any juice
    map<string,string> job = schedule_queues.get_job();
    if(job.size() == 0){
      start = false;
      continue;
    } else if(job["type"] == "maple"){
      maple_phase_master(job["exe"],job["tmp_prefix"],job["DFS_filename"],stoi(job["num"]),stoi(job["upload"]));
      schedule_queues.delete_job("maple");
    } else if(job["type"] == "juice"){
      juice_phase_master(job["exe"],job["tmp_prefix"],job["dest_filename"],stoi(job["num"]),stoi(job["del"]));
      schedule_queues.delete_job("juice");
    }
  }
}

// a function to wait for message
void maplejuice_wait_message(){
  int sk;
  sockaddr_in remote;
  sockaddr_in local;
  socklen_t slen = sizeof(sockaddr_in);
  int msgLength;

  sk = socket(AF_INET, SOCK_DGRAM, 0);
  local.sin_family = AF_INET;
  local.sin_port = 12580;
  local.sin_addr.s_addr = INADDR_ANY;

  // loop for new messages, call needed functions
  bind(sk, (sockaddr*)&local, sizeof(local));
  while(!finish){
    MJ_Msg msg;
    msgLength = recvfrom(sk, &msg, sizeof(msg), 0, (sockaddr*)&remote, &slen);
    //cout << "receiving from " << inet_ntoa(remote.sin_addr) << " status: " << msgLength << endl;
    string return_address = inet_ntoa(remote.sin_addr);
    if(msgLength > 0){
      string type(msg.type);
      if(type == "ack"){
	get_file_status = 2;
      } else if(type == "map"){
	maple_phase_worker(msg, return_address, remote.sin_port);
      } else if(type == "juc"){
	juice_phase_worker(msg, return_address, remote.sin_port);
      }
    }
  }
  close(sk);
}

int main(){
  system_safe("mkdir -p ~/DFS_files");
  system_safe("mkdir -p ~/GET_files");
  system_safe("mkdir -p ~/intermediate_files");
  thread t1(start_SWIM);
  thread t2(waiting_side);
  thread t3(check_replica);  
  thread t4(schedule_tasks);
  thread t5(maplejuice_wait_message);

  // command line interface
    cout << "\ntype in command:\n1, put localfilename sdfsfilename\n2, get sdfsfilename localfilename";
    cout << "\n3, delete sdfsfilename\n4, ls sdfsfilename: list all machine this file is stored";
    cout << "\n5, store: list all files stored on this machine\n6, SWIM: execute SWIM command";
    cout << "\n7, maple <maple_exe> <num_maples> <filename_prefix> <sdfs_input_filename> <option:{0:read file, do not send files for each key, 1:read file, send files for each key ,2:read directory, do not send file for each key, 3:read directory, send files for each key}>: add maple job";
    cout << "\n8, juice <juice_exe> <num_juices> <filename_prefix> <sdfs_dest_filename> <delete_input:{0:keep intermediate file, 1: delete intermediate file}>: add juice job";
    cout << "\n9, start: start MapleJuice!\n10, help: print these instruction again\n";

    while(!finish){
        string command;
        cout << "\nSDFS/MapleJuice command: ";
        cin >> command;
	if(command == "help"){
	  cout << "\ntype in command:\n1, put localfilename sdfsfilename\n2, get sdfsfilename localfilename";
	  cout << "\n3, delete sdfsfilename\n4, ls sdfsfilename: list all machine this file is stored";
	  cout << "\n5, store: list all files stored on this machine\n6, SWIM: execute SWIM command";
	  cout << "\n7, maple <maple_exe> <num_maples> <filename_prefix> <sdfs_input_filename> <option:{0:read file, do not send files for each key, 1:read file, send files for each key ,2:read directory, do not send file for each key, 3:read directory, send files for each key}>: add maple job";
	  cout << "\n8, juice <juice_exe> <num_juices> <filename_prefix> <sdfs_dest_filename> <delete_input:{0:keep intermediate file, 1: delete intermediate file}>: add juice job";
	  cout << "\n9, start: start MapleJuice!\n10, help: print these instruction again\n";
	}
	if(command == "start"){
	  start = true;
	}
	if(command == "maple"){
	  int num, upload;
	  string exe, prefix, input;
	  cin >> exe >> num >> prefix >> input >> upload;
	  schedule_queues.add_maple_job(exe, prefix, input, num, upload);
	}
	if(command == "juice"){
	  int num, del;
	  string exe, prefix, output;
	  cin >> exe >> num >> prefix >> output >> del;
	  schedule_queues.add_juice_job(exe, prefix, output, num, del);
	}
        if(command == "put"){
            string localfilename, sdfsfilename;
            cin >> localfilename >> sdfsfilename;
            put_file(localfilename, sdfsfilename, false, false, "");
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
  t4.join();
  t5.join();
  return 0;
}
