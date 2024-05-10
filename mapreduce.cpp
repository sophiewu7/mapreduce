#include <iostream>
#include "config.h"
#include "master.h"
#include "worker.h"
#include "logger.h"
#include <sstream>
#include <chrono>

bool parseArguments(int argc, char* argv[], Config& config) {
    Logger& logger = Logger::getInstance();

    if (argc < 9) {
        logger.log("Usage: " + std::string(argv[0]) + " --input <inputdir> --output <outputdir> --nworkers <nWorkers> --nreduce <nReduce>", LogLevel::ERROR);
        return false;
    }

    for (int i = 1; i < argc; i += 2) {
        std::string arg = argv[i];
        if (arg == "--input") {
            config.inputDir = argv[i + 1];
        } else if (arg == "--output") {
            config.outputDir = argv[i + 1];
        } else if (arg == "--nworkers") {
            config.nWorkers = std::stoi(argv[i + 1]);
        } else if (arg == "--nreduce") {
            config.nReduce = std::stoi(argv[i + 1]);
        } else {
            logger.log("Unknown argument: " + arg, LogLevel::ERROR);
            return false;
        }
    }

    // Initialize the reduceTasksList based on nReduce and nWorkers
    config.reduceTasksList.resize(config.nWorkers);
    for (int i = 0; i < config.nReduce; i++) {
        int workerId = i % config.nWorkers;
        config.reduceTasksList[workerId].push_back(i);
    }

    return true;
}

void printConfig(const Config& config) {
    Logger& logger = Logger::getInstance();
    std::ostringstream oss;

    oss << "Configuration:\n";
    oss << "Input Directory: " << config.inputDir << "\n";
    oss << "Output Directory: " << config.outputDir << "\n";
    oss << "Number of Workers: " << config.nWorkers << "\n";
    oss << "Number of Reduce Tasks: " << config.nReduce << "\n";
    oss << "Reduce Tasks List per Worker:\n";
    
    for (int i = 0; i < config.nWorkers; i++) {
        oss << "  Worker " << i << ":";
        for (int taskId : config.reduceTasksList[i]) {
            oss << " " << taskId;
        }
        oss << "\n";
    }

    logger.log(oss.str(), LogLevel::INFO);
}

int main(int argc, char* argv[]) {
    /* Initiate Logging */
    Logger::getInstance().setLogFile("mapreduce.log");
    Logger& logger = Logger::getInstance();
    
    logger.log("Starting MapReduce program", LogLevel::INFO);


    Config config;
    if (!parseArguments(argc, argv, config)) {
        return 1;
    }

    printConfig(config);

    Master master(config);
    auto start_time = std::chrono::high_resolution_clock::now();
    master.distributeWork();
    master.printWorkLoad();

    logger.log("Workers are being started", LogLevel::INFO);
    master.startWorkers();

    logger.log("All Map Tasks and Reduce Tasks have finished", LogLevel::INFO);
    logger.log("======== Start Merging =========== ", LogLevel::INFO);
    
    master.merge();

    auto end_merge = std::chrono::high_resolution_clock::now();

    logger.log("======== Merging Complete =========== ", LogLevel::INFO);

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;
    logger.log("MapReduce process completed in " + std::to_string(elapsed.count()) + " seconds", LogLevel::INFO);
    
    return 0;
}