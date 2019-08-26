import ntpath
import os
import subprocess
import sys
from subprocess import call

def split_call(str):
    call(str.split(' '))


def run_cmd(args_list, local):
    """
    run linux commands
    """
    # print('Running system command: {0}'.format(' '.join(args_list)))
    if local:
        proc = subprocess.Popen(args_list, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err

def diffFiles(first_path, second_path):
    try:
        files_n_folder = os.listdir(first_path)
        firstArr = []
        secondArr = []
        for file in files_n_folder:
            if file not in firstArr:
                firstArr.append(file)
        secondArr = readFromHDFS(second_path)
        # files_n_folder = os.listdir(second_path)
        # for file in files_n_folder:
        #     if file not in secondArr:
        #         secondArr.append(file)

        print firstArr
        print secondArr
        # print list(set(firstArr) - set(secondArr))

        return list(set(firstArr) - set(secondArr))
    except Exception as e:
        print "error " + str(e) + " folder not good!!!"
        pass


def readFromHDFS(path):
    firstArr = []
    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-ls', '-C', path], False)
    for file in out.split("\n"):
        if ntpath.basename(file) not in firstArr:
            firstArr.append(ntpath.basename(file))
    return firstArr

# diffFiles("/Users/gopalsr/Documents/Sreeji/code/AirNike/first_folder",
#           "/Users/gopalsr/Documents/Sreeji/code/AirNike/second_folder")git

diffFiles(sys.argv[1], sys.argv[2])

print split_call("hdfs dfs -ls -C {0}".format(sys.argv[3]))
#commit
print diffFiles(sys.argv[1], sys.argv[2])
