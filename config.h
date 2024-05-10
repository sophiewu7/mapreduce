#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <vector>

struct Config {
    std::string inputDir;
    std::string outputDir;
    int nWorkers;
    int nReduce;
    std::vector<std::vector<int>> reduceTasksList; 
};

bool parseArguments(int argc, char* argv[], Config& config);
void printConfig(const Config& config);

#endif // CONFIG_H
