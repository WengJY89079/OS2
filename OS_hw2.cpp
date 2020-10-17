#include <bits/stdc++.h>
#include <pthread.h>
#include <unistd.h>

using namespace std;

pthread_mutex_t Mutex;
pthread_mutex_t insertMutex;

struct json_element{
    char key[10];
    char val[20];
};
typedef struct json_element j_element;

struct json_line{
    int index;
    j_element at[20];
};
typedef struct json_line j_line;

struct index_line{
    int index;
    string line;
};
typedef struct index_line i_line;

struct Q_a_m{
    queue<i_line>* i_line_addr;
    map<int, j_line>* j_line_addr;
};
typedef struct Q_a_m Q_a_m;

static string Num[] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"};

void *i_line_to_j_line(void* i_l){
    Q_a_m *qam = (Q_a_m *)i_l;
    queue<i_line> *qline = qam->i_line_addr;
    j_line *json = (j_line *)malloc(sizeof(j_line));
    i_line line;
    while(1){
        pthread_mutex_lock(&Mutex);
        if((*qline).size() != 0){
            line = (*qline).front();
            (*qline).pop();
            pthread_mutex_unlock(&Mutex);
        }
        else{
            pthread_mutex_unlock(&Mutex);
            break;
        };
        size_t last = line.line.find_first_not_of("|", 0);
        size_t now = line.line.find_first_of("|", last);
        json->index = line.index;
        for(int i = 0; i < 20; ++i){
            string key = "col_" + Num[i];
            strcpy((json->at[i]).key, key.c_str());
            string val = line.line.substr(last, now - last);
            strcpy((json->at[i]).val, val.c_str());
            last = line.line.find_first_not_of("|", now);
            now = line.line.find_first_of("|", last);
        };
        pthread_mutex_lock(&insertMutex);
        qam->j_line_addr->insert(pair<int, j_line>(json->index, *json));
        pthread_mutex_unlock(&insertMutex);
        //cout << clock() << '\n';
    }
    pthread_exit(NULL);
};

struct timespec diff(struct timespec start, struct timespec end){
    struct timespec temp;
    if((end.tv_nsec - start.tv_nsec) < 0){
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    }
    else{
        temp.tv_sec = end.tv_sec - start.tv_sec;
        temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return temp;
};

int main(int argc, char* argv[]){
    int thread_num = atoi(argv[1]);
    string input_file = "input.csv";
    string output_file = "output.json";

    i_line i_L;
    queue<i_line> i_line_Q;
    map<int, j_line> j_line_M;

    cout << "::OS_hw2::\nthread_num : " << thread_num << "\ninput_file : " << input_file << "\noutput_file : " << output_file << "\n";
    cout << input_file << " reading...\n";

    struct timespec start, end, start1, end1, start2, end2, temp;
    /*
       input reading and push to queue
    */
    int index = 0, find = 0;
    string line;
    ifstream input(input_file);
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(; getline(input, line);){
        i_L.index = index++;
        i_L.line = line;
        i_line_Q.push(i_L);
    };
    clock_gettime(CLOCK_MONOTONIC, &end);
    temp = diff(start, end);
    cout << "File line check : index : " << index << ", queue_size : " << i_line_Q.size() << '\n';
    cout << "read input file time : " << temp.tv_sec + (double)temp.tv_nsec / 1000000000.0 << '\n';

    /*
       test pthread
    */

    Q_a_m *Qam = (Q_a_m *)malloc(sizeof(Q_a_m));
    Qam->i_line_addr = &i_line_Q;
    Qam->j_line_addr = &j_line_M;

    pthread_mutex_init(&Mutex, 0);
    pthread_mutex_init(&insertMutex, 0);
    pthread_t* id;
    //i_line **i_L_ptr;
    //i_line *i_L_formulti;
    
    id = (pthread_t *)malloc(sizeof(pthread_t) * thread_num);
    //i_L_ptr = (i_line **)malloc(sizeof(i_line *) * thread_num);
    //i_L_formulti = (i_line *)malloc(sizeof(i_line) * thread_num);
    
    //for(int i = 0; i < thread_num; ++i){
    //    *(i_L_ptr + i) = (i_L_formulti + i);
    //};

    for(int i = 0; i < thread_num; ++i){
        pthread_create((id + i), NULL, i_line_to_j_line, Qam);
    };
    
    clock_gettime(CLOCK_MONOTONIC, &start1);
    //cout << start1.tv_sec << '\n';
    for(int i = 0; i < thread_num; ++i){
        pthread_join(*(id + i), NULL);
        cout << "thread" << i << " : " << *(id + i) << ", back...\n";
    };
    clock_gettime(CLOCK_MONOTONIC, &end1);
    //cout << end1.tv_sec << '\n';
    temp = diff(start1, end1);
    cout << "multi thread parse time : " << temp.tv_sec + (double)temp.tv_nsec / 1000000000.0 << '\n';

    pthread_mutex_destroy(&Mutex);
    pthread_mutex_destroy(&insertMutex);

    cout << j_line_M.size() << '\n';
    cout << "pthread fin..." << '\n';
    cout << "sleep 5 sec..." << '\n';
    sleep(5);
    /*
       pthread finish and output to output.json 
    */
    static map<int, j_line>::iterator iter;
    ofstream output(output_file);
    
    clock_gettime(CLOCK_MONOTONIC, &start2);
    output << "[\n";
    for(;j_line_M.size() != 0;){
        iter = j_line_M.find(find++);
        /*for(int i = 0; i < 20; ++i){
            output << iter->second.at[i].key << " " << iter->second.at[i].val << '\n';
        };
        output << "*****\n";*/
        output << "{\n";
        for(int i = 0; i < 19; ++i){
            output << '\"' << iter->second.at[i].key << "\":" << iter->second.at[i].val << ",\n"; 
        };
        output << '\"' << iter->second.at[19].key << "\":" << iter->second.at[19].val << '\n';
        output << "}";
        j_line_M.erase(iter);
        if(j_line_M.size() == 0){
            output << '\n';
        }
        else{
            output << ",\n";
        }
    };
    output << "]" << '\n';
    clock_gettime(CLOCK_MONOTONIC, &end2);
    temp = diff(start2, end2);
    cout << "write output file time : " << temp.tv_sec + (double) temp.tv_nsec / 1000000000.0 << '\n';

    return 0;
};
