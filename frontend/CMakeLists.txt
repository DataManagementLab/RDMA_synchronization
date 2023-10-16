add_executable(frontend frontend.cpp)
add_dependencies(frontend nam)
target_link_libraries(frontend nam numa)


add_executable(ordering_fixed ordering_fixed.cpp)
add_dependencies(ordering_fixed nam)
target_link_libraries(ordering_fixed nam numa)



add_executable(broken_remote_write broken_remote_write.cpp)
add_dependencies(broken_remote_write nam)
target_link_libraries(broken_remote_write nam numa)


add_executable(atomic_benchmark atomic_benchmark.cpp)
add_dependencies(atomic_benchmark nam)
target_link_libraries(atomic_benchmark nam numa)

add_executable(cas_benchmark cas_benchmark.cpp)
add_dependencies(cas_benchmark nam)
target_link_libraries(cas_benchmark nam numa)

add_executable(fa_benchmark fa_benchmark.cpp)
add_dependencies(fa_benchmark nam)
target_link_libraries(fa_benchmark nam numa)

add_executable(read_benchmark read_benchmark.cpp)
add_dependencies(read_benchmark nam)
target_link_libraries(read_benchmark nam numa)



add_executable(atomic_alignment atomic_alignment.cpp)
add_dependencies(atomic_alignment nam)
target_link_libraries(atomic_alignment nam numa)

add_executable(atomic_alignment_ws atomic_alignment_ws.cpp)
add_dependencies(atomic_alignment_ws nam)
target_link_libraries(atomic_alignment_ws nam numa)


add_executable(torn_writes torn_writes.cpp)
add_dependencies(torn_writes nam)
target_link_libraries(torn_writes nam numa)


add_executable(lock lock.cpp)
add_dependencies(lock nam)
target_link_libraries(lock nam numa)

add_executable(locking_benchmark locking_benchmark.cpp)
add_dependencies(locking_benchmark nam)
target_link_libraries(locking_benchmark nam numa)

add_executable(no_locking_benchmark no_locking_benchmark.cpp)
add_dependencies(no_locking_benchmark nam)
target_link_libraries(no_locking_benchmark nam numa)


add_executable(locking_benchmark_tail locking_benchmark_tail.cpp)
add_dependencies(locking_benchmark_tail nam)
target_link_libraries(locking_benchmark_tail nam numa)

add_executable(atomic_visibility atomic_visibility.cpp)
add_dependencies(atomic_visibility nam)
target_link_libraries(atomic_visibility nam numa)

add_executable(batched_reads batched_reads.cpp)
add_dependencies(batched_reads nam)
target_link_libraries(batched_reads nam numa)

add_executable(nam_experiment nam_experiment.cpp)
add_dependencies(nam_experiment nam)
target_link_libraries(nam_experiment nam numa)


add_executable(batched_atomics batched_atomics.cpp)
add_dependencies(batched_atomics nam)
target_link_libraries(batched_atomics nam numa)

add_executable(contention_reads_atomics contention_reads_atomics.cpp)
add_dependencies(contention_reads_atomics nam)
target_link_libraries(contention_reads_atomics nam numa)

add_executable(pause_effect pause_effect.cpp)
add_dependencies(pause_effect nam)
target_link_libraries(pause_effect nam numa)

add_executable(btree btree.cpp)
add_dependencies(btree nam)
target_link_libraries(btree nam numa)


add_executable(optimistic_st optimistic_st.cpp)
add_dependencies(optimistic_st nam)
target_link_libraries(optimistic_st nam numa)


add_executable(optmistic_benchmark optmistic_benchmark.cpp)
add_dependencies(optmistic_benchmark nam)
target_link_libraries(optmistic_benchmark nam numa)
target_link_libraries(optmistic_benchmark ${CMAKE_DL_LIBS})


add_executable(optdb_experiment optdb_experiment.cpp)
add_dependencies(optdb_experiment nam)
target_link_libraries(optdb_experiment nam numa)
target_link_libraries(optdb_experiment ${CMAKE_DL_LIBS})


add_executable(opt_btree opt_btree.cpp)
add_dependencies(opt_btree nam)
target_link_libraries(opt_btree nam numa)
target_link_libraries(opt_btree ${CMAKE_DL_LIBS})
