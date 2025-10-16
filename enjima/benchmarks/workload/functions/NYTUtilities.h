#ifndef NYT_UTILITIES_H
#define NYT_UTILITIES_H

#include "enjima/api/data_types/NewYorkTaxiEventODQ.h"

#include <boost/functional/hash.hpp>
#include <cmath>
#include <iostream>

namespace enjima::benchmarks::workload {
    using NYTCsvReaderT =
            csv2::Reader<csv2::delimiter<','>, csv2::quote_character<'"'>, csv2::first_row_is_header<false>>;
    using NYTODQEventT = enjima::api::data_types::NewYorkTaxiEventODQ;

    static double CustomStrToDouble(const std::string& doubleStr)
    {
        const char* begin = doubleStr.data();
        const char* end = begin + doubleStr.size();
        double result = 0.0;
        double factor = 1.0;
        bool negative = false;
        if (begin == end) return result;
        if (*begin == '-') {
            negative = true;
            ++begin;
        }
        else if (*begin == '+') {
            ++begin;
        }
        while (begin < end && *begin >= '0' && *begin <= '9') {
            result = result * 10.0 + (*begin - '0');
            ++begin;
        }
        if (begin < end && *begin == '.') {
            ++begin;
            while (begin < end && *begin >= '0' && *begin <= '9') {
                factor *= 0.1;
                result += (*begin - '0') * factor;
                ++begin;
            }
        }
        return negative ? -result : result;
    }

    class TimeConverter {
    public:
        TimeConverter() = default;

        [[maybe_unused]] TimeConverter(int year, int month, int day, int hour, int minute, int second)
            : baseYear_(year), baseMonth_(month), baseDay_(day), baseHour_(hour), baseMinute_(minute),
              baseSecond_(second)
        {
        }

        static bool IsLeapYear(int year)
        {
            if (year % 4 != 0) {
                return false;
            }
            else if (year % 100 != 0) {
                return true;
            }
            else if (year % 400 != 0) {
                return false;
            }
            else {
                return true;
            }
        }

        static int GetDaysInMonth(int year, int month)
        {
            const int daysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

            if (month == 2 && IsLeapYear(year)) {
                return 29;
            }
            else {
                return daysInMonth[month - 1];
            }
        }

        [[nodiscard]] int GetTotalDays(int year, int month, int day) const
        {
            int totalDays = 0;
            for (int y = baseYear_; y < year; ++y) {
                totalDays += IsLeapYear(y) ? 366 : 365;
            }
            for (int m = baseMonth_; m < month; ++m) {
                totalDays += GetDaysInMonth(year, m);
            }
            totalDays += (day - baseDay_);
            return totalDays;
        }

        [[nodiscard]] int GetTotalSeconds(int year, int month, int day, int hour, int minute, int second) const
        {
            int totalDay = GetTotalDays(year, month, day);
            int totalSecond = totalDay * 24 * 3600;
            totalSecond += hour * 3600 + minute * 60 + second;
            return totalSecond;
        }

        [[nodiscard]] int GetTotalSeconds(const std::string& timeStr) const
        {
            int year =
                    (timeStr[0] - '0') * 1000 + (timeStr[1] - '0') * 100 + (timeStr[2] - '0') * 10 + (timeStr[3] - '0');
            int month = (timeStr[5] - '0') * 10 + (timeStr[6] - '0');
            int day = (timeStr[8] - '0') * 10 + (timeStr[9] - '0');
            int hour = (timeStr[11] - '0') * 10 + (timeStr[12] - '0');
            int minute = (timeStr[14] - '0') * 10 + (timeStr[15] - '0');
            int second = (timeStr[17] - '0') * 10 + (timeStr[18] - '0');
            int timeInSecs = GetTotalSeconds(year, month, day, hour, minute, second);
            assert(timeInSecs >= 0 && timeInSecs < 31536000);
            return timeInSecs;
        }

    private:
        int baseYear_{2013};
        int baseMonth_{1};
        int baseDay_{1};
        int baseHour_{0};
        int baseMinute_{0};
        int baseSecond_{0};
    };

    class AreaMapper {
    public:
        AreaMapper() : AreaMapper(0.25) {}

        explicit AreaMapper(double cellWidth) : cellWidth_(cellWidth)
        {
            O_LAT_ = MoveNorth(kLat_, cellWidth_ / 2);
            O_LONG_ = MoveWest(kLon_, cellWidth_ / 2);
            NE_LONG_ = MoveEast(O_LONG_, GRID_WIDTH_);
            S_LAT_ = MoveSouth(O_LAT_, GRID_WIDTH_);
        }

        [[nodiscard]] bool CheckCoord(double lat, double lon) const
        {
            return !(lat > O_LAT_ || lat < S_LAT_ || lon > NE_LONG_ || lon < O_LONG_);
        }

        [[nodiscard]] std::pair<int, int> GetCellID(double lat, double lon) const
        {
            if (!CheckCoord(lat, lon)) {
                return std::make_pair(-1, -1);
            }
            int we = static_cast<int>(std::lround(ToKm(O_LONG_, lon) / cellWidth_)) + 1;
            int ns = static_cast<int>(std::lround(ToKm(O_LAT_, lat) / cellWidth_)) + 1;
            if (we > MAX_CELL || we < MIN_CELL || ns > MAX_CELL || ns < MIN_CELL) {
                return std::make_pair(-1, -1);
            }
            return std::make_pair(we, ns);
        }

        [[nodiscard]] int GetTransformedCellID(double lat, double lon) const
        {
            if (!CheckCoord(lat, lon)) {
                return -1;
            }
            int we = static_cast<int>(std::lround(ToKm(O_LONG_, lon) / cellWidth_)) + 1;
            int ns = static_cast<int>(std::lround(ToKm(O_LAT_, lat) / cellWidth_)) + 1;
            if (we > MAX_CELL || we < MIN_CELL || ns > MAX_CELL || ns < MIN_CELL) {
                return -1;
            }
            auto cellId = ns * (MAX_CELL - 1) + we;
            assert(cellId > 0 && cellId <= MAX_CELL * MAX_CELL);
            return cellId;
        }

    private:
        [[nodiscard]] double ToDeg(double km) const
        {
            return km / EARTH_RADIUS_ * (180 / M_PI);
        }

        [[nodiscard]] double ToKm(double startDeg, double endDeg) const
        {
            return std::abs((endDeg - startDeg) * M_PI / 180.0 * EARTH_RADIUS_);
        }

        double MoveNorth(double lat, double km)
        {
            return lat + ToDeg(km);
        }

        double MoveSouth(double lat, double km)
        {
            return lat - ToDeg(km);
        }

        double MoveEast(double lon, double km)
        {
            return lon + ToDeg(km);
        }

        double MoveWest(double lon, double km)
        {
            return lon - ToDeg(km);
        }

        const double EARTH_RADIUS_{6371};// km
        const double GRID_WIDTH_{150};   // km
        const int MAX_CELL = 600;
        const int MIN_CELL = 1;
        const double kLat_ = 41.474937;
        const double kLon_ = -74.913583;
        double cellWidth_;// km

        double O_LAT_{std::numeric_limits<double>::lowest()};
        double O_LONG_{std::numeric_limits<double>::lowest()};
        double NE_LAT_{std::numeric_limits<double>::lowest()};
        double NE_LONG_{std::numeric_limits<double>::lowest()};
        double S_LAT_{std::numeric_limits<double>::lowest()};
        double S_LONG_{std::numeric_limits<double>::lowest()};
        double SE_LAT_{std::numeric_limits<double>::lowest()};
        double SE_LONG_{std::numeric_limits<double>::lowest()};
    };

    template<typename T>
    class NYTRowReader {
    public:
        explicit NYTRowReader(std::vector<T>* eventCachePtr, char delim = ',')
            : delim_(delim), rowReaderEventCachePtr_(eventCachePtr)
        {
        }

        void ReadFromRow(const NYTCsvReaderT::Row& row)
        {
            assert(row.length() > 104 && row.length() < 300);
            rowStr_.clear();
            row.read_raw_value(rowStr_);
            auto rowIter = rowStr_.begin();
            CustomReadCell(rowStr_, medallion_, rowIter);
            CustomReadCell(rowStr_, hackLicense_, rowIter);
            CustomReadCell(rowStr_, vendorId_, rowIter);
            CustomReadCell(rowStr_, puDateTime_, rowIter);
            CustomReadCell(rowStr_, doDateTime_, rowIter);
            CustomReadCell(rowStr_, tripTimeInSecsStr_, rowIter);
            CustomReadCell(rowStr_, tripDistanceStr_, rowIter);
            CustomReadCell(rowStr_, puLonStr_, rowIter);
            CustomReadCell(rowStr_, puLatStr_, rowIter);
            CustomReadCell(rowStr_, doLonStr_, rowIter);
            CustomReadCell(rowStr_, doLatStr_, rowIter);
            CustomReadCell(rowStr_, paymentTypeStr_, rowIter);
            CustomReadCell(rowStr_, fareAmountStr_, rowIter);
            CustomReadCell(rowStr_, surchargeStr_, rowIter);
            CustomReadCell(rowStr_, mtaTaxStr_, rowIter);
            CustomReadCell(rowStr_, tipAmountStr_, rowIter);
            CustomReadCell(rowStr_, tollsAmountStr_, rowIter);
            CustomReadCell(rowStr_, totalAmountStr_, rowIter);
            assert(medallion_.size() == 32);
            assert(puDateTime_.size() == 19);
            assert(doDateTime_.size() == 19);
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            rowReaderEventCachePtr_->emplace_back(medallion_, hackLicense_, vendorId_, puDateTime_, doDateTime_,
                    std::stoul(tripTimeInSecsStr_), benchmarks::workload::CustomStrToDouble(tripDistanceStr_),
                    CustomStrToDouble(puLonStr_), CustomStrToDouble(puLatStr_), CustomStrToDouble(doLonStr_),
                    CustomStrToDouble(doLatStr_), paymentTypeStr_, CustomStrToDouble(fareAmountStr_),
                    CustomStrToDouble(surchargeStr_), CustomStrToDouble(mtaTaxStr_), CustomStrToDouble(tipAmountStr_),
                    CustomStrToDouble(tollsAmountStr_), CustomStrToDouble(totalAmountStr_), currentTime);
        }

        void CustomReadCell(const std::string& rowStr, std::string& outStr, std::string::iterator& iter) const
        {
            if (iter == rowStr.end()) {
                throw enjima::runtime::IllegalArgumentException{"Cannot read further. Reached end of row!"};
            }
            outStr.clear();
            auto rowEnd = rowStr.end();
            const auto begin = iter;
            while (iter != rowEnd && *iter != delim_) {
                ++iter;
            }
            outStr.append(begin, iter);
            if (iter != rowEnd) {
                ++iter;
            }
        }

    private:
        const char delim_;
        std::vector<T>* rowReaderEventCachePtr_{nullptr};
        std::string rowStr_;
        std::string medallion_;
        std::string hackLicense_;
        std::string vendorId_;
        std::string puDateTime_;
        std::string doDateTime_;
        std::string tripTimeInSecsStr_;
        std::string tripDistanceStr_;
        std::string puLatStr_;
        std::string puLonStr_;
        std::string doLatStr_;
        std::string doLonStr_;
        std::string paymentTypeStr_;
        std::string fareAmountStr_;
        std::string tipAmountStr_;
        std::string surchargeStr_;
        std::string mtaTaxStr_;
        std::string tollsAmountStr_;
        std::string totalAmountStr_;
    };

    class NYTOptimizedDQRowReader {
    public:
        explicit NYTOptimizedDQRowReader(std::vector<NYTODQEventT>* eventCachePtr, char delim = ',')
            : delim_(delim), rowReaderEventCachePtr_(eventCachePtr)
        {
        }

        void ReadFromRow(const NYTCsvReaderT::Row& row)
        {
            assert(row.length() > 104 && row.length() < 300);
            rowStr_.clear();
            row.read_raw_value(rowStr_);
            auto rowIter = rowStr_.begin();
            CustomReadCell(rowStr_, medallion_, rowIter);
            CustomReadCell(rowStr_, hackLicense_, rowIter);
            CustomReadCell(rowStr_, vendorId_, rowIter);
            CustomReadCell(rowStr_, puDateTime_, rowIter);
            CustomReadCell(rowStr_, doDateTime_, rowIter);
            CustomReadCell(rowStr_, tripTimeInSecsStr_, rowIter);
            CustomReadCell(rowStr_, tripDistanceStr_, rowIter);
            CustomReadCell(rowStr_, puLonStr_, rowIter);
            CustomReadCell(rowStr_, puLatStr_, rowIter);
            CustomReadCell(rowStr_, doLonStr_, rowIter);
            CustomReadCell(rowStr_, doLatStr_, rowIter);
            CustomReadCell(rowStr_, paymentTypeStr_, rowIter);
            CustomReadCell(rowStr_, fareAmountStr_, rowIter);
            CustomReadCell(rowStr_, surchargeStr_, rowIter);
            CustomReadCell(rowStr_, mtaTaxStr_, rowIter);
            CustomReadCell(rowStr_, tipAmountStr_, rowIter);
            CustomReadCell(rowStr_, tollsAmountStr_, rowIter);
            CustomReadCell(rowStr_, totalAmountStr_, rowIter);
            assert(medallion_.size() == 32);
            assert(puDateTime_.size() == 19);
            assert(doDateTime_.size() == 19);
            auto puCellId =
                    areaMapper_.GetTransformedCellID(CustomStrToDouble(puLatStr_), CustomStrToDouble(puLonStr_));
            auto doCellId =
                    areaMapper_.GetTransformedCellID(CustomStrToDouble(doLatStr_), CustomStrToDouble(doLonStr_));
            if (puCellId < 0 || doCellId < 0) {
                invalidLocRecordsCount_++;
            }
            auto puDateTimeInSecs = timeConverter_.GetTotalSeconds(puDateTime_);
            auto doDateTimeInSecs = timeConverter_.GetTotalSeconds(doDateTime_);
            auto currentTime = enjima::runtime::GetSystemTimeMillis();
            rowReaderEventCachePtr_->emplace_back(api::data_types::CustomUUIDType(medallion_),
                    api::data_types::CustomUUIDType(hackLicense_), vendorId_, puDateTimeInSecs, doDateTimeInSecs,
                    std::stoul(tripTimeInSecsStr_), CustomStrToDouble(tripDistanceStr_), puCellId, doCellId,
                    currentTime);
        }

        void CustomReadCell(const std::string& rowStr, std::string& outStr, std::string::iterator& iter) const
        {
            if (iter == rowStr.end()) {
                throw enjima::runtime::IllegalArgumentException{"Cannot read further. Reached end of row!"};
            }
            outStr.clear();
            auto rowEnd = rowStr.end();
            const auto begin = iter;
            while (iter != rowEnd && *iter != delim_) {
                ++iter;
            }
            outStr.append(begin, iter);
            if (iter != rowEnd) {
                ++iter;
            }
        }

        [[nodiscard]] uint64_t GetInvalidLocRecordsCount() const
        {
            return invalidLocRecordsCount_;
        }

    private:
        const char delim_;
        AreaMapper areaMapper_;
        TimeConverter timeConverter_{2013, 1, 1, 0, 0, 0};
        std::vector<NYTODQEventT>* rowReaderEventCachePtr_{nullptr};
        std::string rowStr_;
        std::string medallion_;
        std::string hackLicense_;
        std::string vendorId_;
        std::string puDateTime_;
        std::string doDateTime_;
        std::string tripTimeInSecsStr_;
        std::string tripDistanceStr_;
        std::string puLatStr_;
        std::string puLonStr_;
        std::string doLatStr_;
        std::string doLonStr_;
        std::string paymentTypeStr_;
        std::string fareAmountStr_;
        std::string tipAmountStr_;
        std::string surchargeStr_;
        std::string mtaTaxStr_;
        std::string tollsAmountStr_;
        std::string totalAmountStr_;

        uint64_t invalidLocRecordsCount_{0};
    };

}// namespace enjima::benchmarks::workload

namespace std {
    template<>
    struct hash<std::pair<int, int>> {
        std::size_t operator()(const std::pair<int, int>& k) const
        {
            size_t retval = std::hash<int>{}(k.first);
            boost::hash_combine(retval, k.second);
            return retval;
        }
    };

    template<>
    struct hash<enjima::api::data_types::CustomUUIDType> {
        std::size_t operator()(const enjima::api::data_types::CustomUUIDType& k) const
        {
            size_t retval = std::hash<uint64_t>{}(k.GetUuidHighPart());
            boost::hash_combine(retval, k.GetUuidLowPart());
            return retval;
        }
    };
}// namespace std

#endif//NYT_UTILITIES_H