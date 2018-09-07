import os
import sys


def diffFiles(first_path, second_path):
    try:
        files_n_folder = os.listdir(first_path)
        firstArr = []
        secondArr = []
        for file in files_n_folder:
            if file not in firstArr:
                firstArr.append(file)
        files_n_folder = os.listdir(second_path)
        for file in files_n_folder:
            if file not in secondArr:
                secondArr.append(file)

        # print firstArr
        # print secondArr
        print list(set(firstArr) - set(secondArr))

        return list(set(firstArr) - set(secondArr))
    except Exception as e:
        print "error " + str(e) + " folder not good!!!"
        pass


# diffFiles("/Users/gopalsr/Documents/Sreeji/code/AirNike/first_folder",
#           "/Users/gopalsr/Documents/Sreeji/code/AirNike/second_folder")

diffFiles(sys.argv[1], sys.argv[2])
