#ifndef LRB_UTILITIES_H
#define LRB_UTILITIES_H

#include <tuple>
#include <functional>

// Custom hash for 3-element tuples
struct HashTuple3 {
    template <class T1, class T2, class T3>
    std::size_t operator()(const std::tuple<T1, T2, T3>& x) const {
        return std::hash<T1>()(std::get<0>(x)) 
             ^ (std::hash<T2>()(std::get<1>(x)) << 1) 
             ^ (std::hash<T3>()(std::get<2>(x)) << 2);
    }
};

// Custom hash for 4-element tuples
struct HashTuple4 {
    template <class T1, class T2, class T3, class T4>
    std::size_t operator()(const std::tuple<T1, T2, T3, T4>& x) const {
        return std::hash<T1>()(std::get<0>(x)) 
             ^ (std::hash<T2>()(std::get<1>(x)) << 1) 
             ^ (std::hash<T3>()(std::get<2>(x)) << 2) 
             ^ (std::hash<T4>()(std::get<3>(x)) << 3);
    }
};

// Specialization of std::hash for std::tuple<int, int, int, uint64_t>
namespace std {
    template<>
    struct hash<std::tuple<int, int, int, uint64_t>> {
        std::size_t operator()(const std::tuple<int, int, int, uint64_t>& k) const {
            return (std::hash<int>()(std::get<0>(k)) 
                 ^ (std::hash<int>()(std::get<1>(k)) << 1)) 
                 ^ ((std::hash<int>()(std::get<2>(k)) << 2) 
                 ^ (std::hash<uint64_t>()(std::get<3>(k)) << 3));
        }
    };
}

#endif // LRB_UTILITIES_H