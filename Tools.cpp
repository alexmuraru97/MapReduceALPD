//
// Created by alex on 1/31/21.
//

#include "Tools.h"
string Tools::inputFolderName="";
string Tools::outputFolderName="";
string Tools::tempFolderName="";
vector<string> Tools::ReadFolderContents(string path) {
    vector<string> v;
    char point[] = ".";
    char doublePoint[] = "..";
    DIR* dir = opendir(path.c_str());
    struct dirent * dp;
    while ((dp = readdir(dir)) != NULL) {
        if(strcmp (dp->d_name,point) != 0&&strcmp (dp->d_name,doublePoint) != 0) {
            v.push_back(dp->d_name);
        }
    }
    closedir(dir);
    return v;
}

void Tools::CreateFolder(string path) {
    struct stat st;
    if (stat(path.c_str(), &st) )
    {
        mkdir(path.c_str(), S_IRWXU | S_IRWXO);
    }
}

