//
// Created by m34ferna on 02/03/24.
//

#ifndef ENJIMA_STATIC_JOIN_PREDICATE_H
#define ENJIMA_STATIC_JOIN_PREDICATE_H

#include <type_traits>

namespace enjima::api {

    template<typename TInput>
    class StaticJoinPredicate {
    public:
        virtual bool operator()(const TInput& inputEvent) = 0;
    };

    template<typename T, typename U>
    concept StatJoinPredT = std::is_base_of_v<enjima::api::StaticJoinPredicate<U>, T>;

}// namespace enjima::api


#endif//ENJIMA_STATIC_JOIN_PREDICATE_H
