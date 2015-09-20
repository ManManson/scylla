/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <unordered_map>
#include "bytes.hh"
#include "types.hh"
#include "transport/messages/result_message_base.hh"

#pragma once

namespace cql3 {

class untyped_result_set {
public:
    class row {
    private:
        const std::vector<::shared_ptr<column_specification>> _columns;
        const std::unordered_map<sstring, bytes_opt> _data;
    public:
        row(const std::unordered_map<sstring, bytes_opt>&);
        row(const std::vector<::shared_ptr<column_specification>>&, std::vector<bytes_opt>);
        row(row&&) = default;
        row(const row&) = delete;

        bool has(const sstring&) const;
        bytes get_blob(const sstring& name) const {
            return *_data.at(name);
        }
        template<typename T>
        T get_as(const sstring& name) const {
            return boost::any_cast<T>(data_type_for<T>()->deserialize(get_blob(name)));
        }
        // this could maybe be done as an overload of get_as (or something), but that just
        // muddles things for no real gain. Let user (us) attempt to know what he is doing instead.
        template<typename K, typename V>
        std::unordered_map<K, V> get_map(const sstring& name) const {
            auto vec = boost::any_cast<const map_type_impl::native_type&>(
                    map_type_impl::get_instance(data_type_for<K>(),
                            data_type_for<V>(), false)->deserialize(
                            get_blob(name)));
            std::unordered_map<K, V> res;
            std::transform(vec.begin(), vec.end(),
                    std::inserter(res, res.end()), [](auto& p) {
                        return std::pair<K, V>(boost::any_cast<const K&>(p.first), boost::any_cast<const V&>(p.second));
                    });
            return res;
        }
        const std::vector<::shared_ptr<column_specification>>& get_columns() const {
            return _columns;
        }
    };

    typedef std::vector<row> rows_type;
    using const_iterator = rows_type::const_iterator;

    untyped_result_set(::shared_ptr<transport::messages::result_message>);
    untyped_result_set(untyped_result_set&&) = default;

    const_iterator begin() const {
        return _rows.begin();
    }
    const_iterator end() const {
        return _rows.end();
    }
    size_t size() const {
        return _rows.size();
    }
    bool empty() const {
        return _rows.empty();
    }
    const row& one() const;
    const row& at(size_t i) const {
        return _rows.at(i);
    }
    const row& front() const {
        return _rows.front();
    }
    const row& back() const {
        return _rows.back();
    }
private:
    rows_type _rows;
    untyped_result_set() = default;
public:
    static untyped_result_set make_empty() {
        return untyped_result_set();
    }
};

}
