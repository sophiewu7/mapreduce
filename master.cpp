#include "master.h"
#include "worker.h"
#include "config.h"
#include <iostream>
#include <filesystem>
#include <fstream>
#include <vector>
#include <sstream>
#include <algorithm>
#include <numeric>
#include <iomanip>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "logger.h"
#include <chrono>

std::mutex mtx;
std::condition_variable cv;
bool readyForMap = false;
bool readyForReduce = false;

namespace fs = std::filesystem;

Master::Master(const Config& config)
    : config(config), inputDirectory(config.inputDir), numberOfWorkers(config.nWorkers), reduceTasksList(config.reduceTasksList) {
    mapperAssignments.resize(config.nWorkers);
    workerLoad.resize(config.nWorkers, 0);
    Logger::getInstance().log("Master initialized", LogLevel::INFO);
}

std::vector<FileMetaData> Master::readFileMetadata() {
    Logger& logger = Logger::getInstance();
    std::vector<FileMetaData> metadata;
    for (const auto& entry : fs::directory_iterator(inputDirectory)) {
        if (entry.is_regular_file()) {
            metadata.push_back({entry.path().string(), fs::file_size(entry)});
        }
    }
    logger.log("Read file metadata for input directory: " + inputDirectory, LogLevel::INFO);
    return metadata;
}

void Master::distributeWork() {
    Logger& logger = Logger::getInstance();
    auto files = readFileMetadata();
    std::size_t chunkSize = CHUNK_SIZE;

    logger.log("Distributing work among workers", LogLevel::INFO);

    for (const auto& file : files) {
        std::ifstream ifs(file.fileName, std::ifstream::binary);
        if (!ifs) {
            logger.log("Error opening file: " + file.fileName, LogLevel::ERROR);
            continue; 
        }

        std::size_t currentOffset = 0;
        while (currentOffset < file.fileSize) {
            std::size_t remainingSize = file.fileSize - currentOffset;
            std::size_t actualChunkSize = std::min(chunkSize, remainingSize);
            std::vector<char> buffer(actualChunkSize + 64); 

            ifs.seekg(currentOffset);
            ifs.read(buffer.data(), buffer.size());
            std::streamsize bytesRead = ifs.gcount();

            if (bytesRead <= 0) {
                break;
            }

            std::size_t endOfChunk = actualChunkSize - 1;
            for (std::streamsize i = endOfChunk; i < bytesRead; ++i) {
                if (buffer[i] == ' ' || buffer[i] == '\n' || ispunct(buffer[i])) {
                    endOfChunk = i + 1;
                    break;
                }
                if (i == bytesRead - 1) {
                    endOfChunk = bytesRead;
                }
            }

            if (endOfChunk > remainingSize) {
                endOfChunk = remainingSize;
            }

            std::size_t workerIndex = std::distance(workerLoad.begin(), std::min_element(workerLoad.begin(), workerLoad.end()));
            mapperAssignments[workerIndex].push_back({file.fileName, endOfChunk, currentOffset});
            workerLoad[workerIndex] += endOfChunk;
            currentOffset += endOfChunk;

            if (currentOffset >= file.fileSize) {
                break;
            }
        }
    }
    logger.log("Work distribution complete", LogLevel::INFO);
}

void Master::printWorkLoad() const {
    Logger& logger = Logger::getInstance();
    std::ostringstream logStream;
    logStream << "\n";
    for (size_t i = 0; i < mapperAssignments.size(); ++i) {
        logStream << "Worker " << i << " will process files:\n";
        for (const auto& file : mapperAssignments[i]) {
            logStream << "\t" << file.fileName << " (" << file.fileSize << " bytes)\n";
        }
    }
    logger.log(logStream.str(), LogLevel::INFO);
}

void Master::printChunkContent() const {
    for (size_t workerIndex = 0; workerIndex < mapperAssignments.size(); ++workerIndex) {
        std::cout << "Worker " << workerIndex << " will process the following chunk content:\n";
        for (const auto& file : mapperAssignments[workerIndex]) {
            std::ifstream ifs(file.fileName, std::ifstream::binary);
            if (!ifs) {
                std::cerr << "Failed to open file: " << file.fileName << std::endl;
                continue;
            }
            std::vector<char> buffer(file.fileSize);
            ifs.seekg(file.offset);
            ifs.read(buffer.data(), buffer.size());

            std::cout << "File: " << file.fileName << " Chunk starting at " << file.offset << " for " << file.fileSize << " bytes\n";
            std::cout << "Content:\n" << std::string(buffer.begin(), buffer.end()) << "\n\n";
        }
    }
}

void Master::startWorkers() {
    Logger& logger = Logger::getInstance();
    std::vector<std::thread> threads;
    std::vector<Worker> workers;

    logger.log("Initializing worker threads", LogLevel::INFO);

    for (int i = 0; i < numberOfWorkers; ++i) {
        workers.emplace_back(i, config);
    }

    {
        std::unique_lock<std::mutex> lck(mtx);
        logger.log("====================== Map phase starting ====================", LogLevel::INFO);
        for (int i = 0; i < numberOfWorkers; ++i) {
            threads.emplace_back([i, &workers, this, &logger](){
                std::unique_lock<std::mutex> lk(mtx);
                cv.wait(lk, [this]{ return readyForMap; });
                auto start = std::chrono::high_resolution_clock::now();
                logger.log("Worker " + std::to_string(i) + " starts map tasks", LogLevel::INFO);

                workers[i].processMapTasks(mapperAssignments[i]);

                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed = end - start;
                logger.log("Worker " + std::to_string(i) + " completed map tasks in " + std::to_string(elapsed.count()) + " seconds", LogLevel::INFO);
            });
        }

        readyForMap = true;
        cv.notify_all();
        lck.unlock();
    }

    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();
    logger.log("====================== Map phase complete ====================", LogLevel::INFO);
    logger.log("====================== Reduce phase starting ====================", LogLevel::INFO);
    
    {
        std::unique_lock<std::mutex> lck(mtx);
        for (int i = 0; i < numberOfWorkers; ++i) {
            threads.emplace_back([i, &workers, this, &logger](){
                std::unique_lock<std::mutex> lk(mtx);
                cv.wait(lk, [this]{ return readyForReduce; });
                auto start = std::chrono::high_resolution_clock::now();
                workers[i].processReduceTasks(reduceTasksList[i]);

                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed = end - start;
                logger.log("Worker " + std::to_string(i) + " completed reduce tasks in " + std::to_string(elapsed.count()) + " seconds", LogLevel::INFO);

            });
        }

        readyForReduce = true;
        cv.notify_all();
        lck.unlock();
    }

    for (auto& thread : threads) {
        thread.join();
    }
    logger.log("====================== Reduce phase complete ======================", LogLevel::INFO);
}

void Master::merge() {
    std::map<std::string, int> wordCounts;
    std::vector<std::pair<std::string, int>> sortedWords;

    for (int i = 0; i < config.nReduce; ++i) {
        std::string filename = config.outputDir + "/reduce.part-" + std::to_string(i) + ".txt";
        std::ifstream file(filename);
        std::string line;

        while (std::getline(file, line)) {
            std::istringstream lineStream(line);
            std::string word;
            int count;
            if (std::getline(lineStream, word, ',') && lineStream >> count) {
                wordCounts[word] += count;
            }
        }
    }

    for (const auto& pair : wordCounts) {
        sortedWords.push_back(pair);
    }

    std::sort(sortedWords.begin(), sortedWords.end(), [](const auto& a, const auto& b) {
        return (a.second > b.second) || (a.second == b.second && a.first < b.first);
    });

    std::ofstream outputFile(config.outputDir + "/output.txt");
    for (const auto& [word, count] : sortedWords) {
        outputFile << word << "," << count << std::endl;
    }
}
