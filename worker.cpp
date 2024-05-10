#include "worker.h"
#include <fstream>
#include <iostream>
#include <cctype>
#include <locale>
#include <mutex>
#include <sstream>
#include <algorithm>
#include <map>
#include <vector>
#include "logger.h"
#include <chrono>

static std::mutex printMutex;

Worker::Worker(int id, const Config& config) : workerId(id), config(config) {
    Logger::getInstance().log("Worker " + std::to_string(workerId) + " created", LogLevel::INFO);
}

void Worker::processMapTasks(const std::vector<FileMetaData>& tasks) {
    for (const auto& task : tasks) {
        map(task.fileName, task.offset, task.fileSize);
    }
}

bool Worker::is_latin(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

size_t Worker::find_first_latin(const std::string& word) {
    for (size_t i = 0; i < word.length(); ++i) {
        if (is_latin(word[i])) {
            return i;
        }
    }
    return std::string::npos;
}

size_t Worker::find_last_latin(const std::string& word) {
    for (size_t i = word.length(); i > 0; --i) {
        if (is_latin(word[i-1])) {
            return i-1;
        }
    }
    return std::string::npos;
}


std::string Worker::cleanWord(const std::string& word) {
    size_t start = find_first_latin(word);
    if (start == std::string::npos) return "";

    size_t end = find_last_latin(word);
    std::string cleaned = word.substr(start, end - start + 1);

    std::transform(cleaned.begin(), cleaned.end(), cleaned.begin(), ::tolower);
    return cleaned;
}

bool Worker::isValidLatinWord(const std::string& word) {
    for (char ch : word) {
        if (!is_latin(ch)) {
            return false;
        }
    }
    return true;
}

int Worker::getReduceTaskIndex(const std::string& key, int nReduce) {
    std::hash<std::string> hasher;
    return hasher(key) % nReduce;
}

void Worker::map(const std::string& fileName, std::size_t offset, std::size_t size) {
    Logger& logger = Logger::getInstance();
    std::ifstream file(fileName, std::ifstream::binary);
    if (!file) {
        logger.log("Worker " + std::to_string(workerId) + " could not open file: " + fileName, LogLevel::ERROR);
        return;
    }

    file.seekg(offset);
    std::vector<char> buffer(size);
    if (!file.read(buffer.data(), size)) {
        logger.log("Worker " + std::to_string(workerId) + " could not read from file: " + fileName, LogLevel::ERROR);
        return;
    }

    std::string text(buffer.begin(), buffer.end());
    std::istringstream iss(text);
    std::string word;

    while (iss >> word) {
        std::string cleanedWord = cleanWord(word);
        if (!cleanedWord.empty() && isValidLatinWord(cleanedWord)) {
            int reduceIndex = getReduceTaskIndex(cleanedWord, config.nReduce);
            std::ostringstream oss;
            oss << config.outputDir << "/map.part-" << workerId << "-" << reduceIndex << ".txt";
            std::ofstream out(oss.str(), std::ios::app); // Open in append mode
            out << cleanedWord << ",1\n";
        }
    }
}

void Worker::processReduceTasks(const std::vector<int> reduceTasksList) {
    for (int reduceTaskId : reduceTasksList) {
        reduce(reduceTaskId);
    }
}

void Worker::reduce(const int reduceTaskId) {
    Logger& logger = Logger::getInstance();
    logger.log("Worker " + std::to_string(workerId) + " starts reduce task ID: " + std::to_string(reduceTaskId), LogLevel::INFO);
    auto start_time = std::chrono::high_resolution_clock::now();
    std::map<std::string, int> wordCountMap;


    for (int i = 0; i < config.nWorkers; ++i) {
        std::ostringstream oss;
        oss << config.outputDir << "/map.part-" << i << "-" << reduceTaskId << ".txt";

        std::ifstream inFile(oss.str());
        std::string line;

        if (!inFile) {
            logger.log("Worker " + std::to_string(workerId) + " failed to open intermediate file: " + oss.str(), LogLevel::WARNING);
            continue;
        }

        while (getline(inFile, line)) {
            std::istringstream lineStream(line);
            std::string key;
            int value;

            if (std::getline(lineStream, key, ',') && lineStream >> value) {
                key = cleanWord(key);

                if (!key.empty()) {
                    wordCountMap[key] += value;
                }
            }
        }
    }

    std::ostringstream oss;
    oss << config.outputDir << "/reduce.part-" << reduceTaskId << ".txt";
    std::ofstream outFile(oss.str());

    if (!outFile) {
        logger.log("Worker " + std::to_string(workerId) + " failed to open output file: " + oss.str(), LogLevel::ERROR);
        return;
    }

    for (const auto& pair : wordCountMap) {
        outFile << pair.first << "," << pair.second << std::endl;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> eplased = end_time - start_time;
    logger.log("Worker " + std::to_string(workerId) + " completed reduce task ID: " + std::to_string(reduceTaskId) + " in " + std::to_string(eplased.count()) + " seconds", LogLevel::INFO);
}
