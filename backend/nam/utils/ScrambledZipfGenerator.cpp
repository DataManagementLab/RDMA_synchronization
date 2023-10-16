#include "ScrambledZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace nam
{
namespace utils
{

// -------------------------------------------------------------------------------------
u64 ScrambledZipfGenerator::rand()
{
   u64 zipf_value = zipf_generator(gen);
   return min + (nam::utils::FNV::hash(zipf_value) % n);
}
// -------------------------------------------------------------------------------------
u64 ScrambledZipfGenerator::rand(u64 offset)
{
   u64 zipf_value = zipf_generator(gen);
   return (min + ((nam::utils::FNV::hash(zipf_value + offset)) % n));
}

}  // namespace utils
}  // namespace nam
