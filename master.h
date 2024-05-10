#ifndef MASTER_H
#define MASTER_H

#include "config.h"
#include <vector>
#include <string>
#include <map>

constexpr int CHUNK_SIZE = 1024;

struct FileMetaData {
    std::string fileName;
    std::size_t fileSize;
    std::size_t offset;
};

class Master {
public:
    Master(const Config& config);
    void distributeWork();
    void printWorkLoad() const;
    void printChunkContent() const;
    void printMapperAssignments() const;
    void startWorkers();
    void merge();

private:
    Config config;
    std::string inputDirectory;
    int numberOfWorkers;
    std::vector<FileMetaData> readFileMetadata();
    std::vector<std::vector<FileMetaData>> mapperAssignments;
    std::vector<std::size_t> workerLoad;
    std::vector<std::vector<int>> reduceTasksList; 
};

#endif
