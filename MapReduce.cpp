#include <iostream>
#include <mpi.h>
#include "Tools.h"
#include <fstream>
#include <map>
#include <time.h>
#include <regex>

using namespace std;

enum class TaskType{Map, Reduce,Close};

enum class WorkerState {Free, Working, Done};

void MasterTask(int n,TaskType job);
void MasterEnd(int n);
void WorkerHandle(int rank);
bool AreAllWorkersFree(WorkerState* states, int n);


void MapWorkerHandler(string file);
void ReduceWorkerHandler(string folder,int rank);

void ConcatResults(int n);
int main(int argc, char *argv[]) {
    int rank;
    int n;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &n);

    if (argc != 3)
    {
        if (rank == 0) printf("Eroare in formatul liniei de comanda");
        MPI_Finalize();
        return -1;
    }

    Tools::inputFolderName=argv[1];
    Tools::outputFolderName=argv[2];
    Tools::tempFolderName=string(argv[2])+"/temp";

    if(rank==0){
        double c1=clock();
        double ms=0;
        system(("rm -rf "+Tools::outputFolderName).c_str());
        Tools::CreateFolder(Tools::outputFolderName);
        Tools::CreateFolder(Tools::tempFolderName);
        double diffticks=clock()-c1;
        cout<<"[Master] Folders cleared and created in "<<(diffticks)/(CLOCKS_PER_SEC/1000)<<"ms"<<endl;
        ms=(diffticks)/(CLOCKS_PER_SEC/1000);

        c1=clock();
        MasterTask(n,TaskType::Map);
        diffticks=clock()-c1;
        cout<<"[Master] Map stage finished in "<<(diffticks)/(CLOCKS_PER_SEC/1000)<<"ms"<<endl;
        ms+=(diffticks)/(CLOCKS_PER_SEC/1000);

        c1=clock();
        MasterTask(n,TaskType::Reduce);
        diffticks=clock()-c1;
        cout<<"[Master] Reduce stage finished in "<<(diffticks)/(CLOCKS_PER_SEC/1000)<<"ms"<<endl;
        ms+=(diffticks)/(CLOCKS_PER_SEC/1000);

        c1=clock();
        ConcatResults(n);
        diffticks=clock()-c1;
        cout<<"[Master] Result concatenation finished in "<<(diffticks)/(CLOCKS_PER_SEC/1000)<<"ms"<<endl;

        ms+=(diffticks)/(CLOCKS_PER_SEC/1000);
        cout<<"[Master] Total run time "<<ms<<"ms"<<endl;
        MasterEnd(n);
    }else{
        WorkerHandle(rank);
    }


    MPI_Finalize();
    return 0;
}

void MasterTask(int n,TaskType job){
    //MAP
    MPI_Request sendreq;
    int dummyBuffer;
    vector<string> taskList;
    switch(job){
        case TaskType::Map:
            cout<<"[Master] starting Map phase"<<endl;
            taskList=Tools::ReadFolderContents(Tools::inputFolderName);
            cout<<"[Master] "<<taskList.size()<<" files found for map phase"<<endl;
            break;
        case TaskType::Reduce:
            cout<<"[Master] starting Reduce phase"<<endl;
            taskList=Tools::ReadFolderContents(Tools::tempFolderName);
            cout<<"[Master] "<<taskList.size()<<" words found for reduce phase"<<endl;
            break;
    }

    //Vector de stari
    WorkerState* workerState=new WorkerState[n];
    for(int i=0;i<n;++i){
        workerState[i]=WorkerState::Free;
    }

    //Vector requesturi neblocante
    MPI_Request* workerRequests=new MPI_Request[n];

    int currentTask=0;
    while(currentTask<taskList.size()||(!AreAllWorkersFree(workerState, n))){
        for(int i=1;i<n;++i){
            if(workerState[i]==WorkerState::Free&&currentTask<taskList.size()){
                MPI_Isend(taskList.at(currentTask).c_str(),taskList.at(currentTask).size()+1,MPI_CHAR,i,(int)job,MPI_COMM_WORLD,&sendreq);
                MPI_Request_free(&sendreq);
                MPI_Irecv(&(dummyBuffer), 1, MPI_INT, i, MPI_ANY_TAG, MPI_COMM_WORLD, &(workerRequests[i]));
                workerState[i]=WorkerState::Working;
                currentTask++;
                //taskList.clear();
            }
        }
        int flag;
        MPI_Status recvSts;
        for(int i=1;i<n;++i){
            if(workerState[i]!=WorkerState::Free){
                MPI_Test(&workerRequests[i],&flag,&recvSts);
                if(flag){
                    if(recvSts.MPI_TAG==(int)WorkerState::Done){
                        workerState[i]=WorkerState::Free;
                    }
                }
            }
        }
    }

    free(workerState);
    free(workerRequests);
}




void MasterEnd(int n){
    char dummy='0';
    MPI_Request send;
    for(int i=1;i<n;++i) {
        MPI_Isend(&dummy, 1, MPI_CHAR, i, (int)TaskType::Close, MPI_COMM_WORLD, &send);
        MPI_Request_free(&send);
    }
}

void WorkerHandle(int rank){
    bool running=true;
    MPI_Status sts;
    int message=0;
    MPI_Request sendReq;
    char buffer[255];
    string stringBuffer;
    while(running){
        MPI_Recv(buffer, 255, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &sts);
        stringBuffer=string(buffer);

        switch((TaskType)sts.MPI_TAG){
            case TaskType::Close:
                running=false;
                break;

            case TaskType::Map:
                MapWorkerHandler(buffer);
                MPI_Send(&message, 1, MPI_INT, 0, (int)WorkerState::Done, MPI_COMM_WORLD);
                break;

            case TaskType::Reduce:
                ReduceWorkerHandler(buffer,rank);
                MPI_Send(&message, 1, MPI_INT, 0, (int)WorkerState::Done, MPI_COMM_WORLD);
                break;

            default:
                cout<<"Bad task type"<<endl;
                MPI_Finalize();
                exit(EXIT_FAILURE);
        }
    }
}

void MapWorkerHandler(string file){
    map<string,int> frequency;
    char word[255];
    char c;
    int poz=0;
    string sWord;
    ifstream workerCurrentFile(Tools::inputFolderName+"/"+file);
    if(!workerCurrentFile.fail()){
        while (workerCurrentFile.get(c))
        {
            if ((c >= '0' && c <= '9') ||(c >= 'a' && c <= 'z') ||(c >= 'A' && c <= 'Z')) {
                word[poz++] = c;
            }
            else
            {
                if (poz != 0)
                {
                    word[poz] = '\0';
                    poz=0;
                    sWord=string(word);
                    //Add to map if not exists/ increment value if exists
                    auto it = frequency.find(string(word));
                    if(it != frequency.end())
                    {
                        //element found
                        it->second=it->second+1;
                    }else{
                        //element not found
                        frequency.insert(pair<string,int>(sWord,1));
                    };
                }
            }
        }
        for(auto iter = frequency.begin(); iter != frequency.end(); ++iter)
        {
            string cuvant =  iter->first;
            string wordFolder=Tools::tempFolderName+"/"+cuvant;
            Tools::CreateFolder(wordFolder);
            ofstream outfile (wordFolder+"/"+file);
            outfile<<iter->second;
        }
    }

}

void ReduceWorkerHandler(string folder,int rank){
    std::ofstream ofs;
    string outFileName=Tools::outputFolderName+"/"+"worker_"+to_string(rank)+".json";
    ofs.open (outFileName, std::ofstream::out | std::ofstream::app);
    vector<string> v=Tools::ReadFolderContents(Tools::tempFolderName+"/"+folder);
    std::string buff;

    ofs<<"{ \"word\":\""<<folder<<"\",\"documents\":[";
    int index=0;
    for(string file:v){
        ifstream workerCurrentFile(Tools::tempFolderName+"/"+folder+"/"+file);
        getline( workerCurrentFile, buff );
        ofs<<"{\"doc\":\""+file+"\",\"freq\":"+buff+"}";
        if(index<v.size()-1){
            ofs<<",";
            index++;
        }
    }
    ofs<<"]}\n";
}


bool AreAllWorkersFree(WorkerState* states, int n){
    for(int i=1;i<n;++i){
        if(states[i]!=WorkerState::Free){
            return false;
        }
    }
    return true;
}
void ConcatResults(int n){
    vector<string> v=Tools::ReadFolderContents(Tools::outputFolderName);
    vector<string> files;
    regex workerFileRegex ("worker_([0-9])+.json");
    for(string s:v){
        if(regex_match (s,workerFileRegex)){
            files.push_back(s);
        }
    }

    std::ofstream ofs;
    string outFileName=Tools::outputFolderName+"/output.json";
    ofs.open (outFileName, std::ofstream::out);
    ofs<<"{ \"inverseIndexData\":[";
    string buff;
    bool first=true;
    string aux="";
    int idx=0;
    for(string s:files) {
        ifstream workerCurrentFile(Tools::outputFolderName+"/"+s);
        while (getline( workerCurrentFile, buff ))
        {
            aux+=buff+",";
        }
        if(idx==files.size()-1){
            aux.pop_back();
        }
        ofs<<aux;
        aux="";
        idx++;
    }


    ofs<<"]}";
}
