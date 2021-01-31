#include <iostream>
#include <mpi.h>
#include "Tools.h"
#include <fstream>
#include <map>
using namespace std;

enum class TaskType{Map, Reduce,Close};

enum class WorkerState {Free, Working, Done};

void MasterMap(int n);
void MasterReduce(int n);
void MasterEnd(int n);
void WorkerHandle(int rank);
bool AreAllWorkersFree(WorkerState* states, int n);


void MapWorkerHandler(string file);
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
        system(("rm -r "+Tools::outputFolderName).c_str());
        Tools::CreateFolder(Tools::outputFolderName);
        Tools::CreateFolder(Tools::tempFolderName);
        MasterMap(n);
        MasterReduce(n);
        MasterEnd(n);
    }else{
        WorkerHandle(rank);
    }


    MPI_Finalize();
    return 0;
}

void MasterMap(int n){
    //MAP
    cout<<"[Master] Incep executia Map"<<endl;
    MPI_Request sendreq;
    int dummyBuffer;
    vector<string> taskList=Tools::ReadFolderContents(Tools::inputFolderName);
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
                MPI_Isend(taskList.at(currentTask).c_str(),taskList.at(currentTask).size()+1,MPI_CHAR,i,(int)TaskType::Map,MPI_COMM_WORLD,&sendreq);
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
}

void MasterReduce(int n){



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
                cout<<"Worker ["<<rank<<"] executie incheiata cu succes"<<endl;
                break;

            case TaskType::Map:
                cout<<"Worker ["<<rank<<"] am primit - "<<buffer<<" - pentru operatia Map"<<endl;
                MapWorkerHandler(buffer);
                MPI_Send(&message, 1, MPI_INT, 0, (int)WorkerState::Done, MPI_COMM_WORLD);
                break;

            case TaskType::Reduce:

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


bool AreAllWorkersFree(WorkerState* states, int n){
    for(int i=1;i<n;++i){
        if(states[i]!=WorkerState::Free){
            return false;
        }
    }
    return true;
}