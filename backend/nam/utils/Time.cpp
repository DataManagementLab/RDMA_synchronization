#include "Time.hpp"
// -------------------------------------------------------------------------------------

namespace nam
{
namespace utils
{
uint64_t getTimePoint()
{
      using namespace std::chrono;
      auto now = system_clock::now();
      auto now_micros = time_point_cast<microseconds>(now);
      auto value = now_micros.time_since_epoch();
      return value.count();
}
         
uint64_t getTimePointNanoseconds()
{
      using namespace std::chrono;
      auto now = system_clock::now();
      auto now_nanos = time_point_cast<nanoseconds>(now);
      auto value = now_nanos.time_since_epoch();
      return value.count();
}
}  // utils
}  // namespace utils
