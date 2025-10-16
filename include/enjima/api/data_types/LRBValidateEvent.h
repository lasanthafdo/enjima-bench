//
// Created by t86kim on 10/08/24.
//

#ifndef ENJIMA_LRB_VALIDATE_EVENT_TYPES_H
#define ENJIMA_LRB_VALIDATE_EVENT_TYPES_H

namespace enjima::api::data_types {
    class LRBVBProjectedEvent {
        public:
            LRBVBProjectedEvent() : 
                time_(0), vid_(0), speed_(0), xway_(0), lane_(0), dir_(0), seg_(0), pos_(0), id_(0) {}
            
            LRBVBProjectedEvent(uint64_t time, int vid, int speed, int xway, int lane, int dir, int seg, int pos, uint64_t id) : 
                time_(time), vid_(vid), speed_(speed), xway_(xway), lane_(lane), dir_(dir), seg_(seg), pos_(pos), id_(id) {}

            [[nodiscard]] uint64_t GetTimestamp() const {
                return time_;
            }

            [[nodiscard]] int GetVid() const {
                return vid_;
            }

            [[nodiscard]] int GetSpeed() const {
                return speed_;
            }

            [[nodiscard]] int GetXway() const {
                return xway_;
            }

            [[nodiscard]] int GetLane() const {
                return lane_;
            }

            [[nodiscard]] int GetDir() const {
                return dir_;
            }

            [[nodiscard]] int GetSeg() const {
                return seg_;
            }

            [[nodiscard]] int GetPos() const {
                return pos_;
            }

            [[nodiscard]] int GetId() const {
                return id_;
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
            uint64_t id_;
    };

    class LRBVBAccidentReport 
    {
        public:
            LRBVBAccidentReport() : type_(0), xway_(0), dir_(0), seg_(0), time_(0), pos_(0), id_(0) {}

            explicit LRBVBAccidentReport(int type, int xway, int dir, int seg, uint64_t time, int pos, uint64_t id) : 
                type_(type), xway_(xway), dir_(dir), seg_(seg), time_(time), pos_(pos), id_(id) {}
            
            [[nodiscard]] int GetType() const {
                return type_;
            }

            [[nodiscard]] int GetXway() const {
                return xway_;
            }

            [[nodiscard]] int GetDir() const {
                return dir_;
            }

            [[nodiscard]] int GetSeg() const {
                return seg_;
            }

            [[nodiscard]] uint64_t GetTimestamp() const {
                return time_;
            }

            [[nodiscard]] int GetPos() const {
                return pos_;
            }

            [[nodiscard]] uint64_t GetId() const {
                return id_;
            }

        private:
            int type_;   // 0: reporting accident, 1: reporting cleared accident
            int xway_;
            int dir_;
            int seg_;
            uint64_t time_;   // First time that vehicle was observed as stopped    
            int pos_;
            uint64_t id_;
    };

    class LRBVBCountReport
    {
        public:
            LRBVBCountReport() : xway_(0), dir_(0), seg_(0), count_(0), time_(0), id_(0) {}

            explicit LRBVBCountReport(int xway, int dir, int seg, int count, uint64_t time, uint64_t id) :
                xway_(xway), dir_(dir), seg_(seg), count_(count), time_(time), id_(id) {}
            
            [[nodiscard]] int GetXway() const {
                return xway_;
            }

            [[nodiscard]] int GetDir() const {
                return dir_;
            }

            [[nodiscard]] int GetSeg() const {
                return seg_;
            }

            [[nodiscard]] int GetCount() const {
                return count_;
            }

            [[nodiscard]] uint64_t GetTimestamp() const {
                return time_;
            }

            [[nodiscard]] uint64_t GetId() const {
                return id_;
            }

        private:
            int xway_;
            int dir_;
            int seg_;
            int count_;
            uint64_t time_;
            uint64_t id_;
    };

    class LRBVBSpeedReport
    {
        public:
            LRBVBSpeedReport() : xway_(0), dir_(0), seg_(0), avgspd_(0), time_(0), id_(0) {}

            explicit LRBVBSpeedReport(int xway, int dir, int seg, int avgspd, uint64_t time, uint64_t id) :
                xway_(xway), dir_(dir), seg_(seg), avgspd_(avgspd), time_(time), id_(id) {}

            [[nodiscard]] int GetXway() const {
                return xway_;
            }

            [[nodiscard]] int GetDir() const  {
                return dir_;
            }

            [[nodiscard]] int GetSeg() const {
                return seg_;
            }

            [[nodiscard]] int GetSpeed() const {
                return avgspd_;
            }

            [[nodiscard]] uint64_t GetTimestamp() const {
                return time_;
            }

            [[nodiscard]] uint64_t GetId() const {
                return id_;
            }

        private:
            int xway_;
            int dir_;
            int seg_;
            int avgspd_;
            uint64_t time_;
            uint64_t id_;
    };

    class LRBVBTollReport {
        public:
            LRBVBTollReport() : xway_(0), dir_(0), seg_(0), toll_(0), time_(0), id_(0) {}

            explicit LRBVBTollReport(int xway, int dir, int seg, int toll, int time, uint64_t id) :
                xway_(xway), dir_(dir), seg_(seg), toll_(toll), time_(time), id_(id) {}

            [[nodiscard]] uint64_t GetTimestamp() const {
                return time_;
            }

            [[nodiscard]] int GetXway() const {
                return xway_;
            }

            [[nodiscard]] int GetDir() const {
                return dir_;
            }

            [[nodiscard]] int GetSeg() const {
                return seg_;
            }

            [[nodiscard]] int GetToll() const {
                return toll_;
            }

            [[nodiscard]] uint64_t GetId() const {
                return id_;
            }

        private:
            int xway_;
            int dir_;
            int seg_;
            int toll_;
            uint64_t time_;
            uint64_t id_;

    };

    class LRBVBTollFinalReport 
    {
        public:
            LRBVBTollFinalReport() : xway_(0), dir_(0), seg_(0), toll_(0) {}

            explicit LRBVBTollFinalReport(int xway, int dir, int seg, int toll) :
                xway_(xway), dir_(dir), seg_(seg), toll_(toll) {}
        private:
            int xway_;
            int dir_;
            int seg_;
            int toll_;
    };
}


#endif //ENJIMA_LRB_VALIDATE_EVENT_TYPES_H