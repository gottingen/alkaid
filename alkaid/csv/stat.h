//
// Copyright (C) 2024 EA group inc.
// Author: Jeff.li lijippy@163.com
// All rights reserved.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//
//

#pragma once

#include <unordered_map>
#include <sstream>
#include <vector>
#include <alkaid/csv/reader.h>

namespace alkaid {
    /** Class for calculating statistics from CSV files and in-memory sources
     *
     *  **Example**
     *  \include programs/csv_stats.cpp
     *
     */
    class CSVStat {
    public:
        using FreqCount = std::unordered_map<std::string, size_t>;
        using TypeCount = std::unordered_map<DataType, size_t>;

        std::vector<long double> get_mean() const;
        std::vector<long double> get_variance() const;
        std::vector<long double> get_mins() const;
        std::vector<long double> get_maxes() const;
        std::vector<FreqCount> get_counts() const;
        std::vector<TypeCount> get_dtypes() const;

        std::vector<std::string> get_col_names() const {
            return this->reader.get_col_names();
        }

        CSVStat(std::string_view filename, CSVFormat format = CSVFormat::guess_csv());
        CSVStat(std::stringstream& source, CSVFormat format = CSVFormat());
    private:
        // An array of rolling averages
        // Each index corresponds to the rolling mean for the column at said index
        std::vector<long double> rolling_means;
        std::vector<long double> rolling_vars;
        std::vector<long double> mins;
        std::vector<long double> maxes;
        std::vector<FreqCount> counts;
        std::vector<TypeCount> dtypes;
        std::vector<long double> n;

        // Statistic calculators
        void variance(const long double&, const size_t&);
        void count(CSVField&, const size_t&);
        void min_max(const long double&, const size_t&);
        void dtype(CSVField&, const size_t&);

        void calc();
        void calc_chunk();
        void calc_worker(const size_t&);

        CSVReader reader;
        std::deque<CSVRow> records = {};
    };

    inline CSVStat::CSVStat(std::string_view filename, CSVFormat format) :
            reader(filename, format) {
        this->calc();
    }

    /** Calculate statistics for a CSV stored in a std::stringstream */
    inline CSVStat::CSVStat(std::stringstream& stream, CSVFormat format) :
            reader(stream, format) {
        this->calc();
    }

    /** Return current means */
    inline std::vector<long double> CSVStat::get_mean() const {
        std::vector<long double> ret;
        for (size_t i = 0; i < this->get_col_names().size(); i++) {
            ret.push_back(this->rolling_means[i]);
        }
        return ret;
    }

    /** Return current variances */
    inline std::vector<long double> CSVStat::get_variance() const {
        std::vector<long double> ret;
        for (size_t i = 0; i < this->get_col_names().size(); i++) {
            ret.push_back(this->rolling_vars[i]/(this->n[i] - 1));
        }
        return ret;
    }

    /** Return current mins */
    inline std::vector<long double> CSVStat::get_mins() const {
        std::vector<long double> ret;
        for (size_t i = 0; i < this->get_col_names().size(); i++) {
            ret.push_back(this->mins[i]);
        }
        return ret;
    }

    /** Return current maxes */
    inline std::vector<long double> CSVStat::get_maxes() const {
        std::vector<long double> ret;
        for (size_t i = 0; i < this->get_col_names().size(); i++) {
            ret.push_back(this->maxes[i]);
        }
        return ret;
    }

    /** Get counts for each column */
    inline std::vector<CSVStat::FreqCount> CSVStat::get_counts() const {
        std::vector<FreqCount> ret;
        for (size_t i = 0; i < this->get_col_names().size(); i++) {
            ret.push_back(this->counts[i]);
        }
        return ret;
    }

    /** Get data type counts for each column */
    inline std::vector<CSVStat::TypeCount> CSVStat::get_dtypes() const {
        std::vector<TypeCount> ret;
        for (size_t i = 0; i < this->get_col_names().size(); i++) {
            ret.push_back(this->dtypes[i]);
        }
        return ret;
    }

    inline void CSVStat::calc_chunk() {
        /** Only create stats counters the first time **/
        if (dtypes.empty()) {
            /** Go through all records and calculate specified statistics */
            for (size_t i = 0; i < this->get_col_names().size(); i++) {
                dtypes.push_back({});
                counts.push_back({});
                rolling_means.push_back(0);
                rolling_vars.push_back(0);
                mins.push_back(NAN);
                maxes.push_back(NAN);
                n.push_back(0);
            }
        }

        // Start threads
        std::vector<std::thread> pool;
        for (size_t i = 0; i < this->get_col_names().size(); i++)
            pool.push_back(std::thread(&CSVStat::calc_worker, this, i));

        // Block until done
        for (auto& th : pool)
            th.join();

        this->records.clear();
    }

    inline void CSVStat::calc() {
        constexpr size_t CALC_CHUNK_SIZE = 5000;

        for (auto& row : reader) {
            this->records.push_back(std::move(row));

            /** Chunk rows */
            if (this->records.size() == CALC_CHUNK_SIZE) {
                calc_chunk();
            }
        }

        if (!this->records.empty()) {
            calc_chunk();
        }
    }

    inline void CSVStat::calc_worker(const size_t &i) {
        /** Worker thread for CSVStat::calc() which calculates statistics for one column.
         * 
         *  @param[in] i Column index
         */

        auto current_record = this->records.begin();

        for (size_t processed = 0; current_record != this->records.end(); processed++) {
            if (current_record->size() == this->get_col_names().size()) {
                auto current_field = (*current_record)[i];

                // Optimization: Don't count() if there's too many distinct values in the first 1000 rows
                if (processed < 1000 || this->counts[i].size() <= 500)
                    this->count(current_field, i);

                this->dtype(current_field, i);

                // Numeric Stuff
                if (current_field.is_num()) {
                    long double x_n = current_field.get<long double>();

                    // This actually calculates mean AND variance
                    this->variance(x_n, i);
                    this->min_max(x_n, i);
                }
            }
            else if (this->reader.get_format().get_variable_column_policy() == VariableColumnPolicy::THROW) {
                throw std::runtime_error("Line has different length than the others " + internals::format_row(*current_record));
            }

            ++current_record;
        }
    }

    inline void CSVStat::dtype(CSVField& data, const size_t &i) {
        /** Given a record update the type counter
         *  @param[in]  record Data observation
         *  @param[out] i      The column index that should be updated
         */

        auto type = data.type();
        if (this->dtypes[i].find(type) !=
            this->dtypes[i].end()) {
            // Increment count
            this->dtypes[i][type]++;
        } else {
            // Initialize count
            this->dtypes[i].insert(std::make_pair(type, 1));
        }
    }

    inline void CSVStat::count(CSVField& data, const size_t &i) {
        /** Given a record update the frequency counter
         *  @param[in]  record Data observation
         *  @param[out] i      The column index that should be updated
         */

        auto item = data.get<std::string>();

        if (this->counts[i].find(item) !=
            this->counts[i].end()) {
            // Increment count
            this->counts[i][item]++;
        } else {
            // Initialize count
            this->counts[i].insert(std::make_pair(item, 1));
        }
    }

    inline void CSVStat::min_max(const long double &x_n, const size_t &i) {
        /** Update current minimum and maximum
         *  @param[in]  x_n Data observation
         *  @param[out] i   The column index that should be updated
         */
        if (std::isnan(this->mins[i]))
            this->mins[i] = x_n;
        if (std::isnan(this->maxes[i]))
            this->maxes[i] = x_n;

        if (x_n < this->mins[i])
            this->mins[i] = x_n;
        else if (x_n > this->maxes[i])
            this->maxes[i] = x_n;
    }

    inline void CSVStat::variance(const long double &x_n, const size_t &i) {
        /** Given a record update rolling mean and variance for all columns
         *  using Welford's Algorithm
         *  @param[in]  x_n Data observation
         *  @param[out] i   The column index that should be updated
         */
        long double& current_rolling_mean = this->rolling_means[i];
        long double& current_rolling_var = this->rolling_vars[i];
        long double& current_n = this->n[i];
        long double delta;
        long double delta2;

        current_n++;

        if (current_n == 1) {
            current_rolling_mean = x_n;
        } else {
            delta = x_n - current_rolling_mean;
            current_rolling_mean += delta/current_n;
            delta2 = x_n - current_rolling_mean;
            current_rolling_var += delta*delta2;
        }
    }

    /** Useful for uploading CSV files to SQL databases.
     *
     *  Return a data type for each column such that every value in a column can be
     *  converted to the corresponding data type without data loss.
     *  @param[in]  filename The CSV file
     *
     *  \return A mapping of column names to csv::DataType enums
     */
    inline std::unordered_map<std::string, DataType> csv_data_types(const std::string& filename) {
        CSVStat stat(filename);
        std::unordered_map<std::string, DataType> csv_dtypes;

        auto col_names = stat.get_col_names();
        auto temp = stat.get_dtypes();

        for (size_t i = 0; i < stat.get_col_names().size(); i++) {
            auto& col = temp[i];
            auto& col_name = col_names[i];

            if (col[DataType::CSV_STRING])
                csv_dtypes[col_name] = DataType::CSV_STRING;
            else if (col[DataType::CSV_INT64])
                csv_dtypes[col_name] = DataType::CSV_INT64;
            else if (col[DataType::CSV_INT32])
                csv_dtypes[col_name] = DataType::CSV_INT32;
            else if (col[DataType::CSV_INT16])
                csv_dtypes[col_name] = DataType::CSV_INT16;
            else if (col[DataType::CSV_INT8])
                csv_dtypes[col_name] = DataType::CSV_INT8;
            else
                csv_dtypes[col_name] = DataType::CSV_DOUBLE;
        }

        return csv_dtypes;
    }
}  // alkaid
