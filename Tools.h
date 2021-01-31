//
// Created by alex on 1/31/21.
//

#ifndef MAPREDUCE_TOOLS_H
#define MAPREDUCE_TOOLS_H
#include <vector>
#include <string>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>
#include <iostream>
using namespace std;
class Tools {
public:
    static string inputFolderName;
    static string outputFolderName;
    static string tempFolderName;
    static vector<string>ReadFolderContents(string path);
    static void CreateFolder(string path);
};


#endif //MAPREDUCE_TOOLS_H
