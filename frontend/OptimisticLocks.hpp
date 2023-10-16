#pragma once
#include <iostream>

#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "nam/Compute.hpp"
#include "nam/Config.hpp"
#include "nam/Storage.hpp"
#include "nam/profiling/ProfilingThread.hpp"
#include "nam/profiling/counters/WorkerCounters.hpp"
#include "nam/rdma/CommunicationManager.hpp"
#include "nam/threads/Concurrency.hpp"
#include "nam/utils/RandomGenerator.hpp"
#include "nam/utils/Time.hpp"
#include "nam/utils/crc64.hpp"

using namespace nam;
struct OLRestartException {};

constexpr uint64_t CL = 64;

static constexpr uint64_t W_LOCKED = 1;
static constexpr uint64_t W_UNLOCKED = 0;

static constexpr uint64_t EXCLUSIVE_LOCKED = 0x1000000000000000;
static constexpr uint64_t EXCLUSIVE_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF - EXCLUSIVE_LOCKED + 1;
static constexpr uint64_t UNLOCKED = 0;
static constexpr uint64_t MASKED_SHARED_LOCKS = 0x1000000000000000;
static constexpr uint64_t SHARED_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF;
// -------------------------------------------------------------------------------------
// Protected region is just a memory buffer
// FaRM Footer layout:[V]...[V]...[V]...[L]
// FaRM Header [V]CAS[L] ..... [V] ... [V]....
// CRC Footer [V][CRC] .......[L]
// CRC HEADER [V][L][CRC]....
// V2 HEADER  [V][L].........
// V2 HEADER  .........[V][L]
// Version always at beginning of CL and latch directly behind or at the end
// -------------------------------------------------------------------------------------
struct FaRM {
   void generateConsistencyProof([[maybe_unused]] uint64_t* buffer, [[maybe_unused]] uint64_t bytes) {
      // only start at second cacheline since in the first we have the latch and the version
      for (uint64_t cl_i = 8; cl_i < (bytes / sizeof(uint64_t)); cl_i = cl_i + CL / sizeof(uint64_t)) {
         buffer[cl_i]++;
      }
   }
   void checkConsistencyProof([[maybe_unused]] uint64_t* buffer, [[maybe_unused]] uint64_t bytes) {
      // loop over all versions including the first latch version!
      uint64_t prev = buffer[0];
      for (uint64_t cl_i = 0; cl_i < (bytes / sizeof(uint64_t)); cl_i = cl_i + CL / sizeof(uint64_t)) {
         if (prev != buffer[cl_i]) { throw OLRestartException(); }
      }
   }
};
struct CRC {
   void generateConsistencyProof([[maybe_unused]] uint64_t* buffer, [[maybe_unused]] uint64_t bytes) {
      // create CRC remember to exclude the CRC itself from the calculation
      // exclude version, CRC and lock?
      auto* content = buffer + 1;
      auto length = bytes - sizeof(uint64_t);
      uint64_t& crc_v = buffer[0];
      crc_v = crc64(0, (unsigned char*)(content), length); 
   }
   void checkConsistencyProof([[maybe_unused]] uint64_t* buffer, [[maybe_unused]] uint64_t bytes) {
      auto* content = buffer + 1;
      auto length = bytes - sizeof(uint64_t);
      uint64_t& crc_v = buffer[0];
      auto crc_v2 = crc64(0, (unsigned char*)(content), length);
      if (crc_v != crc_v2) {
         throw OLRestartException();
      }
   }
};

struct V2 {
   void generateConsistencyProof([[maybe_unused]] uint64_t* buffer, [[maybe_unused]] uint64_t bytes) {
      // no op
   }
   void checkConsistencyProof([[maybe_unused]] uint64_t* buffer, [[maybe_unused]] uint64_t bytes) {}
};

struct Broken {
   void generateConsistencyProof([[maybe_unused]] uint64_t* buffer, [[maybe_unused]] uint64_t bytes) {
      // no op
   }
   void checkConsistencyProof([[maybe_unused]] uint64_t* buffer, [[maybe_unused]] uint64_t bytes) {}
};

struct AbstractLock {
   void lock(nam::rdma::RdmaContext& rctx,
             uintptr_t lockAddr,
             uint64_t* lock_buffer,
             uintptr_t tupleAddr,
             uint64_t* tuple_buffer,
             size_t bytes) {
      volatile uint64_t& x_locked = lock_buffer[0];
      rdma::postCompareSwap(W_UNLOCKED, W_LOCKED, lock_buffer, rctx, rdma::completion::unsignaled, (size_t)lockAddr);
      // do not read lock value again since could be f&a
      rdma::postRead(tuple_buffer, rctx, rdma::completion::signaled, tupleAddr, bytes, 0);
      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         _mm_pause();
         comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
      }
      // -------------------------------------------------------------------------------------
      if (x_locked != W_UNLOCKED) throw OLRestartException();
   }
};

struct FooterLock : public AbstractLock {
   void unlock(nam::rdma::RdmaContext& rctx,
               [[maybe_unused]] uintptr_t lockAddr,
               [[maybe_unused]] uint64_t* lock_buffer,
               uintptr_t tupleAddr,
               uint64_t* tuple_buffer,
               size_t bytes) {
      auto lock_idx = (bytes / sizeof(uint64_t)) - 1;
      tuple_buffer[lock_idx] = 0;
      rdma::postWrite(tuple_buffer, rctx, rdma::completion::unsignaled, tupleAddr, bytes);
   }
};

struct HeaderLock : public AbstractLock {
   void unlock(nam::rdma::RdmaContext& rctx,
               uintptr_t lockAddr,
               uint64_t* lock_buffer,
               uintptr_t tupleAddr,
               uint64_t* tuple_buffer,
               size_t bytes) {
      rdma::postWrite(tuple_buffer, rctx, rdma::completion::unsignaled, tupleAddr, bytes);
      rdma::postFetchAdd(int64_t{-1}, lock_buffer, rctx, rdma::completion::unsignaled, lockAddr, true);
   }
};

// // 2V -> 1V+1D ... 1V
// // CRC/FaRM -> 1VD -> checkConsistency ... 1V
// // version is oblivious to lock position but we can check it to detect concurrent conflicts
template <typename Consistency, typename LockType>
struct OptimisticLock {
   // -------------------------------------------------------------------------------------
   Consistency c;
   LockType l;
   // -------------------------------------------------------------------------------------
   nam::rdma::RdmaContext& rctx;
   uintptr_t remote_address;
   uint64_t* tuple_buffer;
   size_t bytes;
   uint64_t prev_version =0;
   // -------------------------------------------------------------------------------------

   OptimisticLock(nam::rdma::RdmaContext& rctx, uintptr_t remote_address, uint64_t* tuple_buffer, size_t bytes)
      : rctx(rctx), remote_address(remote_address), tuple_buffer(tuple_buffer), bytes(bytes){};
   // -------------------------------------------------------------------------------------
   void checkLock() {
      uint64_t* lck = nullptr;
      if constexpr (std::is_same_v<HeaderLock, LockType>) {
         lck = (tuple_buffer + 1);
      } else {
         // for V2 and FARM it is the last slot
         lck = tuple_buffer + ((bytes / sizeof(uint64_t)) - 1);
      }
      if (*lck != W_UNLOCKED) throw OLRestartException();
   }
   // -------------------------------------------------------------------------------------
   void lock() {
      if constexpr (std::is_same_v<V2, Consistency>) {
         // Header read version first and latch!
         if constexpr (std::is_same_v<HeaderLock, LockType>) {
            rdma::postReadFenced(&tuple_buffer[0], rctx, rdma::completion::signaled, remote_address, 16, 0);
            {
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  _mm_pause();
                  comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
               }
            }
            
            rdma::postRead(&tuple_buffer[2], rctx, rdma::completion::signaled, remote_address + 16, bytes - 16, 0);
            {
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  _mm_pause();
                  comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
               }
            }
            prev_version = tuple_buffer[0];
            // -------------------------------------------------------------------------------------
            checkLock();
            // -------------------------------------------------------------------------------------
         } else {
            // calculate footer header and version
            auto byte_offset = (bytes - 16);
            auto index = (byte_offset) / sizeof(uint64_t);
            tuple_buffer[index] = 0;
            tuple_buffer[index+1] = 0;
            rdma::postReadFenced(&tuple_buffer[index], rctx, rdma::completion::signaled, remote_address + byte_offset, 16, 0);
            {
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  _mm_pause();
                  comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
               }
               prev_version = tuple_buffer[index];
            }
            rdma::postRead(&tuple_buffer[0], rctx, rdma::completion::signaled, remote_address, bytes - 16, 0);
            {
               int comp{0};
               ibv_wc wcReturn;
               while (comp == 0) {
                  _mm_pause();
                  comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
               }
            }
            prev_version = tuple_buffer[index];
            // -------------------------------------------------------------------------------------
            checkLock();
            // -------------------------------------------------------------------------------------
         }

      } else if constexpr (std::is_same_v<Broken, Consistency>) {
         // read version first
         rdma::postRead(&tuple_buffer[0], rctx, rdma::completion::signaled, remote_address, bytes, 0);
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            _mm_pause();
            comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
         }
         prev_version = tuple_buffer[0];
         // -------------------------------------------------------------------------------------
         checkLock();
         // -------------------------------------------------------------------------------------
      } else {
         rdma::postRead(tuple_buffer, rctx, rdma::completion::signaled, remote_address, bytes, 0);
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            _mm_pause();
            comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
         }
         prev_version = tuple_buffer[0];
         // -------------------------------------------------------------------------------------
         checkLock();
         // -------------------------------------------------------------------------------------
         if constexpr (std::is_same_v<CRC, Consistency>) {
            if constexpr (std::is_same_v<HeaderLock, LockType>) {
               auto* t = tuple_buffer + 2;
               c.checkConsistencyProof(t, bytes - 16);
            } else {
               auto* t = tuple_buffer + 1;
               c.checkConsistencyProof(t, bytes - 16);
            }
         } else {
            c.checkConsistencyProof(tuple_buffer, bytes);
         }
      }
   }
   // -------------------------------------------------------------------------------------
   // validates and can throw in case of restart
   std::pair<uint64_t,uint64_t> unlock() {
      // read only version again
      volatile uint64_t* v_version = &tuple_buffer[0];  // this is going to be changed

      if constexpr (std::is_same_v<V2, Consistency>) {
         // Header read version first and latch!
         if constexpr (std::is_same_v<HeaderLock, LockType>) {
            rdma::postRead(&tuple_buffer[0], rctx, rdma::completion::signaled, remote_address, 8, 0);
         } else {
            // calculate footer header and version
            auto byte_offset = (bytes - 16);
            auto index = (byte_offset) / sizeof(uint64_t);
            v_version = &tuple_buffer[index];
            rdma::postRead(&tuple_buffer[index], rctx, rdma::completion::signaled, remote_address + byte_offset, 16, 0);
         }
      }else{
         rdma::postRead(tuple_buffer, rctx, rdma::completion::signaled, remote_address, 8, 0);
      }

      int comp{0};
      ibv_wc wcReturn;
      while (comp == 0) {
         _mm_pause();
         comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
      }
      // -------------------------------------------------------------------------------------
      if (prev_version != *v_version){
         throw OLRestartException();
      }
      // -------------------------------------------------------------------------------------
      checkLock();
      // -------------------------------------------------------------------------------------
      return {prev_version, *v_version};
   }
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
template <typename Consistency, typename LockType>
struct ExclusiveLock {
   // -------------------------------------------------------------------------------------
   Consistency c;
   LockType l;
   // -------------------------------------------------------------------------------------
   nam::rdma::RdmaContext& rctx;
   uintptr_t remote_address;
   uint64_t* lock_buffer;
   uint64_t* tuple_buffer;
   size_t bytes;
   // -------------------------------------------------------------------------------------
   // depening on the type traits we have different positingitions
   // return remoteaddress of latch position
   uintptr_t getLockPosition() {
      if constexpr (std::is_same_v<HeaderLock, LockType>) {
         return (remote_address + 8);
      } else {
         // for V2 and FARM it is the last slot
         return remote_address + ((bytes)-8);
      }
   }
   // -------------------------------------------------------------------------------------
   ExclusiveLock(nam::rdma::RdmaContext& rctx, uintptr_t remote_address, uint64_t* lock_buffer, uint64_t* tuple_buffer, size_t bytes)
       : rctx(rctx), remote_address(remote_address), lock_buffer(lock_buffer), tuple_buffer(tuple_buffer), bytes(bytes){};
   // -------------------------------------------------------------------------------------
   // input position and buffer
   void lock() {
      // offets to both?
      l.lock(rctx, getLockPosition(), lock_buffer, remote_address, tuple_buffer, bytes);
   }
   // -------------------------------------------------------------------------------------
   void unlock() {
      if constexpr (std::is_same_v<CRC, Consistency>) {
         if constexpr (std::is_same_v<HeaderLock, LockType>) {
            auto* t = tuple_buffer + 2;
            c.generateConsistencyProof(t, bytes - 16);
         } else {
            auto* t = tuple_buffer + 1;
            c.generateConsistencyProof(t, bytes - 16);
         }
      } else {
         c.generateConsistencyProof(tuple_buffer, bytes);
      }
      // increment version
      if constexpr (std::is_same_v<V2, Consistency> && std::is_same_v<FooterLock, LockType>) {
         // Header read version first and latch!
         auto byte_offset = (bytes - 16);
         auto index = (byte_offset) / sizeof(uint64_t);
         tuple_buffer[index]++;
      }else{
         tuple_buffer[0]++;
      }

      l.unlock(rctx, getLockPosition(), lock_buffer, remote_address, tuple_buffer, bytes);
   }
};

   struct ReaderWriterLock {
      // -------------------------------------------------------------------------------------
      nam::rdma::RdmaContext& rctx;
      uintptr_t remote_address;
      uint64_t* lock_buffer;
      uint64_t* tuple_buffer;
      size_t bytes;
      // -------------------------------------------------------------------------------------
      ReaderWriterLock(nam::rdma::RdmaContext& rctx, uintptr_t remote_address, uint64_t* lock_buffer, uint64_t* tuple_buffer, size_t bytes)
          : rctx(rctx), remote_address(remote_address), lock_buffer(lock_buffer), tuple_buffer(tuple_buffer), bytes(bytes){};
      // -------------------------------------------------------------------------------------
      // input position and buffer
      void lockExclusive() {
         volatile uint64_t& x_locked = lock_buffer[0];
         rdma::postCompareSwap(UNLOCKED, EXCLUSIVE_LOCKED, lock_buffer, rctx, rdma::completion::unsignaled, remote_address);
         // do not read lock value again since could be f&a
         rdma::postRead(tuple_buffer, rctx, rdma::completion::signaled, remote_address + 8, bytes - 8, 0);
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            _mm_pause();
            comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
         }
         // -------------------------------------------------------------------------------------
         if (x_locked != UNLOCKED) throw OLRestartException();
      }
      // -------------------------------------------------------------------------------------
      void unlockExclusive() {
         rdma::postWrite(tuple_buffer, rctx, rdma::completion::unsignaled, remote_address + 8, bytes - 8);
         rdma::postFetchAdd(EXCLUSIVE_UNLOCK_TO_BE_ADDED, lock_buffer, rctx, rdma::completion::unsignaled, remote_address, true);
      }
      // -------------------------------------------------------------------------------------
      void lockShared() {
         volatile uint64_t& s_locked = lock_buffer[0];
         rdma::postFetchAdd(1, lock_buffer, rctx, rdma::completion::unsignaled, remote_address);
         rdma::postRead(tuple_buffer, rctx, rdma::completion::signaled, remote_address + 8, bytes - 8, 0);
         int comp{0};
         ibv_wc wcReturn;
         while (comp == 0) {
            _mm_pause();
            comp = rdma::pollCompletion(rctx.id->qp->send_cq, 1, &wcReturn);
         }
         // -------------------------------------------------------------------------------------
         if (s_locked >= EXCLUSIVE_LOCKED) {
            unlockShared();
            throw OLRestartException();
         }
      }
      // -------------------------------------------------------------------------------------
      void unlockShared() { rdma::postFetchAdd(int64_t{-1}, lock_buffer, rctx, rdma::completion::unsignaled, remote_address, true); }
   };
