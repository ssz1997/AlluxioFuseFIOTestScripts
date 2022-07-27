import os
import time

num_threads = ["1", "2", "4", "8", "16", "32", "64"]
file_size = ["100k", "1m", "10m", "100m", "1G"]

for t in num_threads:
    start = time.time()
    for size in file_size:
        fuseFileName = "/home/centos/alluxio-fuse/" + "RandRead" + "T" + t + "F" + size
        file = open("ReadThroughput.txt", "a+")
        file.write("Starting " + fuseFileName + "\n")
        file.close()
        bufferSize = str(4 * 1024 * 1024)
        if size == "100k" or size == "1m":
            bufferSize = size
        command = "fio --name=" + fuseFileName + " --rw=read" + " --bs=" + bufferSize + " --numjobs=" + t + " --filesize=" + size + " --direct=1 --runtime=3m --time_based --group_reporting"
        stream = os.popen(command)
        output = stream.read()
        for i in range(len(output)):
            if output[i:i+4] == "IOPS":
                file = open("ReadThroughput.txt", "a")
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
        os.system("rm /home/centos/alluxio-fuse/Read*")
    file = open("ReadThroughput.txt", "a")
    file.write("Time spent on rand read with " + t + " threads: " + str(time.time() - start) + " seconds\n")
    file.close()
