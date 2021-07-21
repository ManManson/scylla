/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "seastarx.hh"

#include <optional>
#include <vector>
#include <stddef.h>

class schema;

namespace cql3 {

class column_identifier;
class column_specification;
namespace functions { class function_call; }

/**
 * A metadata class currently holding bind variables specifications and 
 * populated at "prepare" step of query execution.
 */
class raw_prepare_metadata final {
private:
    std::vector<shared_ptr<column_identifier>> _variable_names;
    std::vector<lw_shared_ptr<column_specification>> _specs;
    std::vector<lw_shared_ptr<column_specification>> _target_columns;
    // A list of pointers to prepared `function_call` AST nodes.
    // Used to set additional state for these calls at "prepare" step of a
    // statement life cycle.
    using function_calls_t = std::vector<::shared_ptr<cql3::functions::function_call>>;
    function_calls_t _fn_calls;

public:

    raw_prepare_metadata() = default;
    raw_prepare_metadata(const std::vector<::shared_ptr<column_identifier>>& variable_names);

    static lw_shared_ptr<raw_prepare_metadata> empty();

    size_t bound_variables_size() const;

    std::vector<lw_shared_ptr<column_specification>> get_variable_specifications() const &;

    std::vector<lw_shared_ptr<column_specification>> get_variable_specifications() &&;

    std::vector<uint16_t> get_partition_key_bind_indexes(const schema& schema) const;

    void add_variable_specification(int32_t bind_index, lw_shared_ptr<column_specification> spec);

    void set_bound_variables(const std::vector<shared_ptr<column_identifier>>& prepare_meta);

    function_calls_t& function_calls();
};

}
