#ifndef WORKER_H
#define WORKER_H

#include <string>
#include <vector>
#include "config.h"
#include "master.h"

class Worker {
public:
    Worker(int id, const Config& config);
    void processMapTasks(const std::vector<FileMetaData>& tasks);
    void map(const std::string& fileName, std::size_t offset, std::size_t size);
    bool is_latin(char c);
    size_t find_first_latin(const std::string& word);
    size_t find_last_latin(const std::string& word);
    std::string cleanWord(const std::string& word);
    bool isValidLatinWord(const std::string& word);
    int getReduceTaskIndex(const std::string& key, int nReduce);

    void processReduceTasks(std::vector<int> reduceTasksList);
private:
    int workerId;
    Config config;
    void reduce(const int taskId);
};

#endif // WORKER_H
