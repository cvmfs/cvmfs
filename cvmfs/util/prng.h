/**
 * This file is part of the CernVM File System.
 *
 * A simple linear congruential pseudo number generator.  Thread-safe since
 * there is no global state like with random().
 */

#ifndef CVMFS_UTIL_PRNG_H_
#define CVMFS_UTIL_PRNG_H_

#include <stdint.h>
#include <sys/time.h>

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <limits>  // NOLINT


#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Pseudo Random Number Generator.  See: TAoCP, volume 2
 */
class Prng {
 public:
  // Cannot throw an exception
  Prng() throw() { state_ = 0; }

  void InitSeed(const uint64_t seed) { state_ = seed; }

  void InitLocaltime() {
    struct timeval tv_now;
    int retval = gettimeofday(&tv_now, NULL);
    assert(retval == 0);
    state_ = tv_now.tv_usec;
  }

  /**
   * Returns random number in [0..boundary-1]
   */
  uint32_t Next(const uint64_t boundary) {
    state_ = a * state_ + c;
    double scaled_val = static_cast<double>(state_)
                        * static_cast<double>(boundary)
                        / static_cast<double>(18446744073709551616.0);
    return static_cast<uint32_t>(static_cast<uint64_t>(scaled_val) % boundary);
  }

  /**
   * Returns random double in range [0, 1]
   */
  double NextDouble() {
    state_ = a * state_ + c;
    double unit_val = static_cast<double>(state_)
                      / static_cast<double>(18446744073709551616.0);
    return unit_val;
  }
  /**
   * Returns normally distributed random numbers
   * with mean 0 and variance 1 using the
   * Box-Muller transform algorithm
   */
  double NextNormal() {
    double z, u1, u2;
    double pi = atan(1) * 4;
    u1 = NextDouble();
    u2 = NextDouble();
    z = sqrt(-2.0 * log(u1)) * cos(2 * pi * u2);
    return z;
  }


 private:
  // Magic numbers from MMIX
  // static const uint64_t m = 2^64;
  static const uint64_t a = 6364136223846793005LLU;
  static const uint64_t c = 1442695040888963407LLU;
  uint64_t state_;
};  // class Prng

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_PRNG_H_
