//
// Created by m34ferna on 05/06/24.
//

#ifndef ENJIMA_BENCHMARKS_WORKLOAD_EXCEPTION_H
#define ENJIMA_BENCHMARKS_WORKLOAD_EXCEPTION_H

#include <exception>
#include <string>
namespace enjima::benchmarks::workload {

    class WorkloadException : public std::exception {
    public:
        WorkloadException() = default;
        explicit WorkloadException(std::string message) : message_(std::move(message)) {}

        [[nodiscard]] const char* what() const noexcept override
        {
            return message_.c_str();
        }

    private:
        const std::string message_ = "Workload exception!";
    };

}// namespace enjima::benchmarks::workload


#endif//ENJIMA_BENCHMARKS_WORKLOAD_EXCEPTION_H
