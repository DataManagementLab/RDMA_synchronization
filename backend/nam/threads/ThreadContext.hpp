#pragma once 
// -------------------------------------------------------------------------------------
namespace nam {
namespace threads {
// ensure that every thread in nam gets this thread context initialized 
struct ThreadContext{
   // -------------------------------------------------------------------------------------
   static inline thread_local ThreadContext* tlsPtr = nullptr;
   static inline ThreadContext& my() {
      return *ThreadContext::tlsPtr;
   }
   // -------------------------------------------------------------------------------------

   /*
   struct DebugInfo{
      std::string_view msg;
      PID pid;
      uint64_t version;
      uint64_t g_epoch;
      uintptr_t bf_ptr;
   };

   
   template <typename T, size_t N>
   struct DebugStack {
      void push(const T& e)
         {
            assert(size <= N);
            if (full()) {
               size = 0; // overwrite
            }
            buffer[size++] = e;
         }

      [[nodiscard]] bool try_pop(T& e)
         {
            if (empty()) {
               return false;
            }
            e = buffer[--size];
            return true;
         }
      bool empty() { return size == 0; }
      bool full() { return size == (N); }
      uint64_t get_size() { return size; }
      void reset() { size = 0; }

     private:
      uint64_t size = 0;
      std::array<T, N> buffer{};
   }; 
   DebugStack<DebugInfo,128> debug_stack;
   */
};

}  // threads
}  // nam
