#ifndef LOGGER_H
#define LOGGER_H

#include <iostream>
#include <fstream>
#include <string>
#include <mutex>
#include <iomanip>
#include <sstream>

enum class LogLevel {
    INFO,
    DEBUG,
    ERROR,
    WARNING
};

class Logger {
public:
    static Logger& getInstance() {
        static Logger instance;
        return instance;
    }

    void setLogFile(const std::string& filename) {
        logFile.open(filename, std::ios::app);
    }

    void log(const std::string& message, LogLevel level) {
        auto now = std::chrono::system_clock::now();
        auto time= std::chrono::system_clock::to_time_t(now);

        std::stringstream time_stream;
        time_stream << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S");
        std::string time_str = time_stream.str();

        std::lock_guard<std::mutex> lock(mtx);
        std::cout << time_str << " - ";
        logFile << time_str << " - ";


        switch (level) {
            case LogLevel::INFO:
                std::cout << "INFO: " << message << std::endl;
                logFile << "INFO: " << message << std::endl;
                break;
            case LogLevel::DEBUG:
                std::cout << "DEBUG: " << message << std::endl;
                logFile << "DEBUG: " << message << std::endl;
                break;
            case LogLevel::ERROR:
                std::cout << "ERROR: " << message << std::endl;
                logFile << "ERROR: " << message << std::endl;
                break;
            case LogLevel::WARNING:
                std::cout << "WARNING: " << message << std::endl;
                logFile << "WARNING: " << message << std::endl;
                break;
        }
    }

private:
    std::ofstream logFile;
    std::mutex mtx;

    Logger() {}
    ~Logger() {
        if (logFile.is_open()) {
            logFile.close();
        }
    }
};

#endif // LOGGER_H