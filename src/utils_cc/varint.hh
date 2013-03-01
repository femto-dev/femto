/*
  (*) 2008-2013 Michael Ferguson <michaelferguson@acm.org>

    * This is a work of the United States Government and is not protected by
      copyright in the United States.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

  femto/src/utils_cc/varint.hh
*/
#ifndef _VARINT_HH_
#define _VARINT_HH_

extern "C" {
  #include <inttypes.h>
  #include "bswap.h"
  #include "config.h"
}

// Read a (little-endian) variable-byte encoded number.
// Returns the length in bytes read
// Returns in val the value.
template<typename intT>
static inline short read_varbyte_ptr(const unsigned char* restrict data, intT& restrict val)
{
  short pos;
  short i;

  val = 0;
  pos = 0;
  // Read any numbers with the high bit set.
  for( i = 0; data[pos] & 0x80; i+=7, pos++ ) {
    // high bit is set - accumulate
    val |= (data[pos] & 0x7f) << i;
  }
  // Now we're to the one with no high bit - the last byte.
  val |= data[pos] << i;
  pos++;
  
  return pos;
}

// write a (little-endian) variable-byte encoded number.
// Returns the length written.
template<typename intT>
static inline short write_varbyte_ptr(unsigned char* restrict data, intT val)
{
  short pos;

  pos = 0;
  // Write until our number is 0.
  while( val > 0x7f ) {
    // set the high bit and output.
    data[pos++] = (val & 0x7f) | 0x80;
    val >>= 7;
  }

  // store the one with no high bit.
  data[pos++] = val & 0x7f;

  return pos;
}

// Classes for reading/writing Big Endian integers
// of differing sizes (including 8,16,24,32,40,64 bits)
template <typename IntType>
struct int_traits {
  typedef IntType register_type;
};

/* Template class to choose the appropriate implementation.
   The basic one is broken and can't be used. 
 */
template < int num_bytes >
class be_uint {
  private:
    be_uint() { }
};

template <int num_bytes>
struct int_traits< be_uint<num_bytes> > {
  typedef typename be_uint<num_bytes>::register_type register_type;
};

// Big-endian integers with varying bit-lengths.
// Types for integers of varying bit-lengths.
// These can implicitly be converted to and from register_type.
template<>
class be_uint<8> {
  private:
    uint64_t a;
  public:
    typedef uint64_t register_type;
    // constructor allowing implicit conversion (write)
    be_uint(register_type x)
      : a(hton_64(x))
    { }
    be_uint(){ }		// constructor with no arguments, to allow array-building
    // allow implicit conversion (read)
    operator register_type() const {
      return ntoh_64(a);
    }
};

template<>
class be_uint<7> {
  private:
    uint32_t a;
    uint16_t b;
    uint8_t c;
  public:
    typedef uint64_t register_type;
    // constructor allowing implicit conversion (write)
    be_uint(register_type x)
      : a(hton_32(x >> 24)), b(hton_16((uint16_t) (x >> 8))), c((uint8_t) x)
    { }
    be_uint(){ }		// constructor with no arguments, to allow array-building
    // allow implicit conversion (read)
    operator register_type() const {
      register_type ret;
      ret = ntoh_32(a);
      ret <<= 16;
      ret |= ntoh_16(b);
      ret <<= 8;
      ret |= c;
      return ret;
    }
} __attribute__((packed));

template<>
class be_uint<6> {
  private:
    uint32_t a;
    uint16_t b;
  public:
    typedef uint64_t register_type;
    // constructor allowing implicit conversion (write)
    be_uint(register_type x)
      : a(hton_32(x >> 16)), b(hton_16((uint16_t) x))
    { }
    be_uint(){ }		// constructor with no arguments, to allow array-building
    // allow implicit conversion (read)
    operator register_type() const {
      register_type ret;
      ret = ntoh_32(a);
      ret <<= 16;
      ret |= ntoh_16(b);
      return ret;
    }
} __attribute__((packed));

template<>
class be_uint<5> {
  private:
    uint32_t a;
    uint8_t b;
  public:
    typedef uint64_t register_type;
    // constructor allowing implicit conversion (write)
    be_uint(register_type x) 
      : a(hton_32(x >> 8)), b((uint8_t) x)
    { }
    be_uint(){ }		// constructor with no arguments, to allow array-building
    // allow implicit conversion (read)
    operator register_type() const {
      register_type ret;
      ret = ntoh_32(a);
      ret <<= 8;
      ret |= b;
      return ret;
    }
} __attribute__((packed));

template<>
class be_uint<4> {
  private:
    uint32_t a;
  public:
    typedef uint32_t register_type;
    // constructor allowing implicit conversion (write)
    be_uint(register_type x)
      : a(hton_32(x))
    { }
    be_uint(){ }		// constructor with no arguments, to allow array-building
    // allow implicit conversion (read)
    operator register_type() const {
      return ntoh_32(a);
    }
};

template<>
class be_uint<3> {
  private:
    uint16_t a;
    uint8_t b;
  public:
    typedef uint32_t register_type;
    // constructor allowing implicit conversion (write)
    be_uint(register_type x) 
      : a(hton_16(x >> 8)), b((uint8_t) x)
    { }
    be_uint(){ }		// constructor with no arguments, to allow array-building
    // allow implicit conversion (read)
    operator register_type() const {
      register_type ret;
      ret = ntoh_16(a);
      ret <<= 8;
      ret |= b;
      return ret;
    }
} __attribute__((packed));

template<>
class be_uint<2> {
  private:
    uint16_t a;
  public:
    typedef uint16_t register_type;
    // constructor allowing implicit conversion (write)
    be_uint(register_type x)
      : a(hton_16(x))
    { }
    be_uint(){ }		// constructor with no arguments, to allow array-building
    // allow implicit conversion (read)
    operator register_type() const {
      return ntoh_16(a);
    }
};

template<>
class be_uint<1> {
  private:
    uint8_t a;
  public:
    typedef uint8_t register_type;
    // constructor allowing implicit conversion (write)
    be_uint(register_type x)
      : a(x)
    { }
    be_uint(){ }		// constructor with no arguments, to allow array-building
    // allow implicit conversion (read)
    operator register_type() const {
      return a;
    }
};

typedef be_uint<8> be_uint64;
typedef be_uint<7> be_uint56;
typedef be_uint<6> be_uint48;
typedef be_uint<5> be_uint40;
typedef be_uint<4> be_uint32;
typedef be_uint<3> be_uint24;
typedef be_uint<2> be_uint16;
typedef be_uint<1> be_uint8;


template < int num_bytes >
class min_word_getter {
  private:
    typedef uint64_t register_type; // error!
};

template <>
class min_word_getter<8> {
  public:
    typedef uint64_t register_type;
};
template <>
class min_word_getter<7> {
  public:
    typedef uint64_t register_type;
};
template <>
class min_word_getter<6> {
  public:
    typedef uint64_t register_type;
};
template <>
class min_word_getter<5> {
  public:
    typedef uint64_t register_type;
};

template <>
class min_word_getter<4> {
  public:
    typedef uint32_t register_type;
};
template <>
class min_word_getter<3> {
  public:
    typedef uint32_t register_type;
};


template <>
class min_word_getter<2> {
  public:
    typedef uint16_t register_type;
};

template <>
class min_word_getter<1> {
  public:
    typedef uint8_t register_type;
};

template <int num_bytes>
struct fast_uint {
  public:
    typedef typename min_word_getter<num_bytes>::register_type register_type;
  private:
    register_type a;
  public:
    // constructor allowing implicit conversion (write)
    fast_uint(register_type x)
      : a(x)
    { }
    fast_uint(){ } // constructor with no arguments, to allow array-building
    // allow implicit conversion (read)
    operator register_type() const {
      return a;
    }
};

template <int num_bytes>
struct int_traits< fast_uint<num_bytes> > {
  typedef typename fast_uint<num_bytes>::register_type register_type;
};

#endif
