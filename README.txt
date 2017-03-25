To compile the code, type:
g++ -std=c++11 -pthread MapleJuice.cpp

To run the code, type:
./a.out

The program require each machine have other machine's ssh key autherized.
Here's the python code to set up the machine:

"""
import os
import paramiko

keys = ["","","","","","","","","",""] # enter 10 machine's public key
ip = ["","","","","","","","","",""] # enter 10 machine's ip addresses

for k in keys:
    for i in range(0, 10):
        paramiko.util.log_to_file('ssh.log')
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # enter username and password
        ssh.connect(ip[i], username="", password="")

        ssh.exec_command("mkdir -p .ssh")
        ssh.exec_command("echo \'" + k + "\' >> .ssh/authorized_keys")
"""

Here's the python code to pull from git and compile needed files :)

"""
import paramiko
import time

ip = [234] * 10
for i in range(0, 10):
    ip[i] += i

    paramiko.util.log_to_file('ssh.log')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect("172.22.148.%d" % ip[i], username="", password="")
    ssh.exec_command("rm -rf ./DFS_files/* ./GET_files/* ./intermediate_files/*")
    ssh.exec_command("rm -rf ./CS425MP4/tmp_output/* ./CS425MP4/tmp_files/*")

    ssh.exec_command("cd CS425MP4; git fetch origin master")
    time.sleep(2)
    ssh.exec_command("cd CS425MP4; git reset --hard origin/master")
    time.sleep(2)
    ssh.exec_command("cd CS425MP4; git pull;")
    time.sleep(2)
    ssh.exec_command("cd CS425MP4; g++ -std=c++11 -pthread MapleJuice.cpp")
    ssh.exec_command("cd CS425MP4; g++ -std=c++11 -o wcm word_count_map.cpp")
    ssh.exec_command("cd CS425MP4; g++ -std=c++11 -o wcj word_count_reduce.cpp")
    ssh.exec_command("cd CS425MP4; g++ -std=c++11 -o lgm linked_graph_map.cpp")
    ssh.exec_command("cd CS425MP4; g++ -std=c++11 -o lgj linked_graph_reduce.cpp")
"""
