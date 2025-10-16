//
// Created by t86kim on 10/08/24.
//

#ifndef ENJIMA_LRB_INTERIM_EVENT_TYPES_H
#define ENJIMA_LRB_INTERIM_EVENT_TYPES_H

namespace enjima::api::data_types {
    class LRBProjectedEvent {
    public:
        LRBProjectedEvent() : time_(0), vid_(0), speed_(0), xway_(0), lane_(0), dir_(0), seg_(0), pos_(0) {}

        LRBProjectedEvent(uint64_t time, int vid, int speed, int xway, int lane, int dir, int seg, int pos)
            : time_(time), vid_(vid), speed_(speed), xway_(xway), lane_(lane), dir_(dir), seg_(seg), pos_(pos)
        {
        }

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return time_;
        }

        [[nodiscard]] int GetVid() const
        {
            return vid_;
        }

        [[nodiscard]] int GetSpeed() const
        {
            return speed_;
        }

        [[nodiscard]] int GetXway() const
        {
            return xway_;
        }

        [[nodiscard]] int GetLane() const
        {
            return lane_;
        }

        [[nodiscard]] int GetDir() const
        {
            return dir_;
        }

        [[nodiscard]] int GetSeg() const
        {
            return seg_;
        }

        [[nodiscard]] int GetPos() const
        {
            return pos_;
        }

    private:
        uint64_t time_;
        int vid_;
        int speed_;
        int xway_;
        int lane_;
        int dir_;
        int seg_;
        int pos_;
    };

    class LRBAccidentReport {
    public:
        LRBAccidentReport() : type_(0), xway_(0), dir_(0), seg_(0), time_(0), pos_(0) {}

        explicit LRBAccidentReport(int type, int xway, int dir, int seg, uint64_t time, int pos)
            : type_(type), xway_(xway), dir_(dir), seg_(seg), time_(time), pos_(pos)
        {
        }

        [[nodiscard]] int GetType() const
        {
            return type_;
        }

        [[nodiscard]] int GetXway() const
        {
            return xway_;
        }

        [[nodiscard]] int GetDir() const
        {
            return dir_;
        }

        [[nodiscard]] int GetSeg() const
        {
            return seg_;
        }

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return time_;
        }

        [[nodiscard]] int GetPos() const
        {
            return pos_;
        }

    private:
        int type_;// 0: reporting accident, 1: reporting cleared accident
        int xway_;
        int dir_;
        int seg_;
        uint64_t time_;// First time that vehicle was observed as stopped
        int pos_;
    };

    class LRBCountReport {
    public:
        LRBCountReport() : xway_(0), dir_(0), seg_(0), count_(0), time_(0) {}

        explicit LRBCountReport(int xway, int dir, int seg, int count, uint64_t time)
            : xway_(xway), dir_(dir), seg_(seg), count_(count), time_(time)
        {
        }

        [[nodiscard]] int GetXway() const
        {
            return xway_;
        }

        [[nodiscard]] int GetDir() const
        {
            return dir_;
        }

        [[nodiscard]] int GetSeg() const
        {
            return seg_;
        }

        [[nodiscard]] int GetCount() const
        {
            return count_;
        }

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return time_;
        }

    private:
        int xway_;
        int dir_;
        int seg_;
        int count_;
        uint64_t time_;
    };

    class LRBSpeedReport {
    public:
        LRBSpeedReport() : xway_(0), dir_(0), seg_(0), avgspd_(0), time_(0) {}

        explicit LRBSpeedReport(int xway, int dir, int seg, int avgspd, uint64_t time)
            : xway_(xway), dir_(dir), seg_(seg), avgspd_(avgspd), time_(time)
        {
        }

        [[nodiscard]] int GetXway() const
        {
            return xway_;
        }

        [[nodiscard]] int GetDir() const
        {
            return dir_;
        }

        [[nodiscard]] int GetSeg() const
        {
            return seg_;
        }

        [[nodiscard]] int GetSpeed() const
        {
            return avgspd_;
        }

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return time_;
        }

    private:
        int xway_;
        int dir_;
        int seg_;
        int avgspd_;
        uint64_t time_;
    };

    class LRBTollReport {
    public:
        LRBTollReport() : xway_(0), dir_(0), seg_(0), toll_(0l), time_(0ul) {}

        [[maybe_unused]] explicit LRBTollReport(int xway, int dir, int seg, long toll, uint64_t time)
            : xway_(xway), dir_(dir), seg_(seg), toll_(toll), time_(time)
        {
        }

        [[nodiscard]] uint64_t GetTimestamp() const
        {
            return time_;
        }

        [[nodiscard]] int GetXway() const
        {
            return xway_;
        }

        [[nodiscard]] int GetDir() const
        {
            return dir_;
        }

        [[nodiscard]] int GetSeg() const
        {
            return seg_;
        }

        [[nodiscard]] long GetToll() const
        {
            return toll_;
        }

    private:
        int xway_;
        int dir_;
        int seg_;
        long toll_;
        uint64_t time_;
    };

    class LRBTollFinalReport {
    public:
        LRBTollFinalReport() : xway_(0), dir_(0), seg_(0), toll_(0l) {}

        explicit LRBTollFinalReport(int xway, int dir, int seg, long toll)
            : xway_(xway), dir_(dir), seg_(seg), toll_(toll)
        {
        }

        [[nodiscard]] int GetXway() const
        {
            return xway_;
        }

        [[nodiscard]] int GetDir() const
        {
            return dir_;
        }

        [[nodiscard]] int GetSeg() const
        {
            return seg_;
        }

        [[nodiscard]] long GetToll() const
        {
            return toll_;
        }

    private:
        int xway_;
        int dir_;
        int seg_;
        long toll_;
    };
}// namespace enjima::api::data_types


#endif//ENJIMA_LRB_INTERIM_EVENT_TYPES_H