//
// Created by t86kim on 22/11/24
//

#ifndef ENJIMA_NYT_INTERIM_EVENT_TYPES_H
#define ENJIMA_NYT_INTERIM_EVENT_TYPES_H

namespace enjima::api::data_types {

    class CustomUUIDType {
    public:
        CustomUUIDType() = default;

        explicit CustomUUIDType(const std::string& uuidStr)
        {
            assert(uuidStr.size() == 32);
            SetUuid(uuidStr);
        }

        [[nodiscard]] uint64_t GetUuidHighPart() const
        {
            return uuidHighPart_;
        }

        [[nodiscard]] uint64_t GetUuidLowPart() const
        {
            return uuidLowPart_;
        }

        bool operator==(const CustomUUIDType& rhs) const
        {
            return uuidHighPart_ == rhs.uuidHighPart_ && uuidLowPart_ == rhs.uuidLowPart_;
        }

    private:
        void SetUuid(const std::string& uuidStr)
        {
            uuidHighPart_ |= (GetVal(uuidStr[0]) << 60);
            uuidHighPart_ |= (GetVal(uuidStr[1]) << 56);
            uuidHighPart_ |= (GetVal(uuidStr[2]) << 52);
            uuidHighPart_ |= (GetVal(uuidStr[3]) << 48);
            uuidHighPart_ |= (GetVal(uuidStr[4]) << 44);
            uuidHighPart_ |= (GetVal(uuidStr[5]) << 40);
            uuidHighPart_ |= (GetVal(uuidStr[6]) << 36);
            uuidHighPart_ |= (GetVal(uuidStr[7]) << 32);
            uuidHighPart_ |= (GetVal(uuidStr[8]) << 28);
            uuidHighPart_ |= (GetVal(uuidStr[9]) << 24);
            uuidHighPart_ |= (GetVal(uuidStr[10]) << 20);
            uuidHighPart_ |= (GetVal(uuidStr[11]) << 16);
            uuidHighPart_ |= (GetVal(uuidStr[12]) << 12);
            uuidHighPart_ |= (GetVal(uuidStr[13]) << 8);
            uuidHighPart_ |= (GetVal(uuidStr[14]) << 4);
            uuidHighPart_ |= GetVal(uuidStr[15]);

            uuidLowPart_ |= (GetVal(uuidStr[16]) << 60);
            uuidLowPart_ |= (GetVal(uuidStr[17]) << 56);
            uuidLowPart_ |= (GetVal(uuidStr[18]) << 52);
            uuidLowPart_ |= (GetVal(uuidStr[19]) << 48);
            uuidLowPart_ |= (GetVal(uuidStr[20]) << 44);
            uuidLowPart_ |= (GetVal(uuidStr[21]) << 40);
            uuidLowPart_ |= (GetVal(uuidStr[22]) << 36);
            uuidLowPart_ |= (GetVal(uuidStr[23]) << 32);
            uuidLowPart_ |= (GetVal(uuidStr[24]) << 28);
            uuidLowPart_ |= (GetVal(uuidStr[25]) << 24);
            uuidLowPart_ |= (GetVal(uuidStr[26]) << 20);
            uuidLowPart_ |= (GetVal(uuidStr[27]) << 16);
            uuidLowPart_ |= (GetVal(uuidStr[28]) << 12);
            uuidLowPart_ |= (GetVal(uuidStr[29]) << 8);
            uuidLowPart_ |= (GetVal(uuidStr[30]) << 4);
            uuidLowPart_ |= GetVal(uuidStr[31]);
        }

        inline static uint64_t GetVal(char c)
        {
            assert((c >= 48 && c <= 57) || (c >= 65 && c <= 70));
            if (c >= 65) {
                return c - 55;
            }
            else {
                return c - 48;
            }
        }

        uint64_t uuidHighPart_{0};
        uint64_t uuidLowPart_{0};
    };

    class NYTEventProjected {
    public:
        NYTEventProjected() : medallion_("00000000000000000000000000000000"), fareAmount_(0.0), tipAmount_(0.0) {}

        NYTEventProjected(CustomUUIDType medallion, int pickupCellWE, int pickupCellNS, int dropOffCellWE,
                int dropOffCellNS, double fareAmount, double tipAmount)
            : medallion_(medallion), pickupCellWE_(pickupCellWE), pickupCellNS_(pickupCellNS),
              dropOffCellWE_(dropOffCellWE), dropOffCellNS_(dropOffCellNS), fareAmount_(fareAmount),
              tipAmount_(tipAmount)
        {
        }

        [[nodiscard]] CustomUUIDType GetMedallionUuid() const
        {
            return medallion_;
        }

        [[nodiscard]] int GetPickupCellWE() const
        {
            return pickupCellWE_;
        }

        [[nodiscard]] int GetPickupCellNS() const
        {
            return pickupCellNS_;
        }

        [[nodiscard]] int GetDropOffCellWE() const
        {
            return dropOffCellWE_;
        }

        [[nodiscard]] int GetDropOffCellNS() const
        {
            return dropOffCellNS_;
        }

        [[nodiscard]] double GetFareAmount() const
        {
            return fareAmount_;
        }

        [[nodiscard]] double GetTipAmount() const
        {
            return tipAmount_;
        }

    private:
        CustomUUIDType medallion_;
        int pickupCellWE_{-1};
        int pickupCellNS_{-1};
        int dropOffCellWE_{-1};
        int dropOffCellNS_{-1};
        double fareAmount_;
        double tipAmount_;
    };

    class NYTEmptyTaxiReport {
    public:
        NYTEmptyTaxiReport() : medallion_("00000000000000000000000000000000"), dropOffCellWE_(0), dropOffCellNS_(0) {}

        NYTEmptyTaxiReport(CustomUUIDType medallion, int dropOffCellWE, int dropOffCellNS)
            : medallion_(medallion), dropOffCellWE_(dropOffCellWE), dropOffCellNS_(dropOffCellNS)
        {
        }

        [[nodiscard]] CustomUUIDType GetMedallion() const
        {
            return medallion_;
        }

        [[nodiscard]] int GetDropOffCellWE() const
        {
            return dropOffCellWE_;
        }

        [[nodiscard]] int GetDropOffCellNS() const
        {
            return dropOffCellNS_;
        }

    private:
        CustomUUIDType medallion_;
        int dropOffCellWE_;
        int dropOffCellNS_;
    };

    class NYTEmptyTaxiCountReport {
    public:
        NYTEmptyTaxiCountReport() : cellIdWE_(0), cellIdNS_(0), count_(0) {}

        NYTEmptyTaxiCountReport(int cellIdWE, int cellIdNS, int count)
            : cellIdWE_(cellIdWE), cellIdNS_(cellIdNS), count_(count)
        {
        }

        [[nodiscard]] int GetCellIdWE() const
        {
            return cellIdWE_;
        }

        [[nodiscard]] int GetCellIdNS() const
        {
            return cellIdNS_;
        }

        [[nodiscard]] int GetCount() const
        {
            return count_;
        }

    private:
        int cellIdWE_;
        int cellIdNS_;
        int count_;
    };

    class NYTProfitReport {
    public:
        NYTProfitReport() : cellIdWE_(0), cellIdNS_(0), profit_(0.0) {}

        NYTProfitReport(int cellIdWE, int cellIdNS, double profit)
            : cellIdWE_(cellIdWE), cellIdNS_(cellIdNS), profit_(profit)
        {
        }

        [[nodiscard]] int GetCellIdWE() const
        {
            return cellIdWE_;
        }

        [[nodiscard]] int GetCellIdNS() const
        {
            return cellIdNS_;
        }

        [[nodiscard]] double GetProfit() const
        {
            return profit_;
        }

    private:
        int cellIdWE_;
        int cellIdNS_;
        double profit_;
    };

    class NYTProfitabilityReport {
    public:
        NYTProfitabilityReport() : cellIdWE_(0), cellIdNS_(0), profitability_(0) {}

        NYTProfitabilityReport(int cellIdWE, int cellIdNS, double profitability)
            : cellIdWE_(cellIdWE), cellIdNS_(cellIdNS), profitability_(profitability)
        {
        }

        [[nodiscard]] int GetCellIdWE() const
        {
            return cellIdWE_;
        }

        [[nodiscard]] int GetCellIdNS() const
        {
            return cellIdNS_;
        }

        [[nodiscard]] double GetProfitability() const
        {
            return profitability_;
        }

    private:
        int cellIdWE_;
        int cellIdNS_;
        double profitability_;
    };
}//namespace enjima::api::data_types

#endif//ENJIMA_NYT_INTERIM_EVENT_TYPES_H