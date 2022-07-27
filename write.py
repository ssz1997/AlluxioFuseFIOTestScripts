import os
import time

num_clients = ["1", "2", "4", "8", "16", "32", "64"]
#file_size = ["100k", "1m", "10m", "100m", "1G"]
#num_files = [1000, 1000, 100, 10, 1]
#num_clients = ["4", "32", "64"]
file_size = ["100k"]
num_files = [10000]
open_files = [1000, 500, 250, 120, 60, 30, 15]
#open_files = [250, 30, 15]

for a in range(len(num_clients)):
    n = num_clients[a]
    start = time.time()
    for i in range(len(file_size)):
        size = file_size[i]
        num = num_files[i]
        fileName = ""
        for j in range(num):
            fuseFileName = "/home/centos/alluxio-fuse/" + n + "/" + "Write" + "T" + n + "F" + size + "id" + str(j)
            fileName += fuseFileName + ":"
        fileName = fileName[:-1]
        file = open("WriteThroughput.txt", "a+")
        file.write("Starting " + fuseFileName + "\n")
        file.close()
        bufferSize = str(4 * 1024 * 1024)
        if size == "100k" or size == "1m":
            bufferSize = size
        os.system("mkdir /home/centos/alluxio-fuse/" + n)
        command = "fio --name=/home/centos/alluxio-fuse/" + n + "/Write" + " --nrfiles=" + str(num) + " --rw=write" + " --bs=" + bufferSize + " --numjobs=" + n + " --group_reporting=1 --direct=1 --filesize=" + size + " --openfiles=" + str(open_files[a]) + " --alloc-size=1048576"
        stream = os.popen(command)
        output = stream.read()
        for i in range(len(output)):
            if output[i:i+4] == "IOPS":
                file = open("WriteThroughput.txt", "a")
                file.write("IOPS: " + output[i+5:i+9] + "\t")
            if output[i:i+2] == "BW":
                file.write("Throughput: ")
                number = ""
                for j in range(i+3, i+10):
                    if output[j] != " ":
                        number += output[j]
                    else:
                        break
                file.write(number + "\n")
                file.close()
                break
        os.system("rm -R /home/centos/alluxio-fuse/*")
    file = open("WriteThroughput.txt", "a")
    file.write("Time spent on writing " + n + " files: " + str(time.time() - start) + " seconds\n")
    file.close()
