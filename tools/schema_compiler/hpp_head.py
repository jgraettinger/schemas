
hpp_head = """
#define BOOST_PYTHON_MAX_ARITY 35

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_upgradable_mutex.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/tuple/tuple_comparison.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/functional/hash/hash.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/python.hpp>

typedef long long _int64;

// common boost IPC types
typedef boost::interprocess::managed_shared_memory                   managed_memory_t;
typedef boost::interprocess::managed_shared_memory::segment_manager  segment_manager_t;
typedef boost::interprocess::allocator<void, segment_manager_t>      void_allocator_t;
typedef boost::interprocess::interprocess_upgradable_mutex           mutex_t;

// shared-memory string type
typedef boost::interprocess::allocator<
    char, segment_manager_t
> char_allocator_t;
typedef boost::interprocess::basic_string<
    char, std::char_traits<char>, char_allocator_t
> string_t;

inline string_t extract_string_t(const boost::python::object & o,
    const void_allocator_t & alloc)
{
    if(!PyString_Check(o.ptr()))
    {
        throw std::logic_error("extract_string_t():"
            " requires a string type");
    }
    
    const char * str = PyString_AS_STRING(o.ptr());
    size_t len = PyString_GET_SIZE(o.ptr());
    return string_t(str, str + len, alloc);
}

// generalized tuple hashing
template<typename Tuple>
struct _tuple_hash
{
    size_t operator()(const Tuple & t) const;
};
template<typename Tuple>
inline size_t _tuple_hash<Tuple>::operator()(const Tuple & t) const {
    size_t r = _tuple_hash<typename Tuple::tail_type>()(t.get_tail());
    
    // A little hackey, but use ADL to choose a hash implementation
    //  not using boost::hash<typename Tuple::head_type>() due to
    //  issues forwarding reference strings types. Cleaner alternative
    //  would wrap string references in boost::ref
    
    using namespace boost;
    using namespace boost::interprocess_container;
    return (r << 5) + (r >> 3) + hash_value(t.get_head());
}
template<>
inline size_t _tuple_hash<boost::tuples::null_type>::operator()(
    const boost::tuples::null_type &) const
{ return 0; }

namespace {
    // annon namespace => no linker collisions
    boost::python::object none_obj( 
        ((struct boost::python::detail::borrowed_reference_t *) ((void*) Py_None))
    );
};
"""
