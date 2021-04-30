#pragma once

#include <seastar/http/exception.hh>
#include "seastarx.hh"

namespace api {

class unimplemented_exception : public httpd::base_exception {
public:
    unimplemented_exception()
            : base_exception("API call is not supported yet", httpd::reply::status_type::internal_server_error) {
    }
};

inline void unimplemented() {
    throw unimplemented_exception();
}

} // namespace api