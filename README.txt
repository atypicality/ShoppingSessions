ShoppingSessions hybrid radix sort experiment
===========================================

Archive contains cpp/main.cpp, a complete replacement based on your uploaded
original code. No other pipeline processing code was changed.

Changes:
 - Hybrid LSD radix sort for per-user events with >=1024 timestamps.
 - Hybrid LSD radix sort for per-user clicks with >=1024 timestamps.
 - Skips constant timestamp bytes, including sign handling for negatives.
 - Promotes the minimum click ID to the first position of each equal timestamp
   group. That preserves last-touch ID tie-breaking without string sorting.
 - Retains per-thread scratch buffers and std::sort for smaller users.

Build-time experiment macros (add -D<...> to g++ flags in Dockerfile):
 SHOPPING_RADIX_THRESHOLD=1024 (try 256,512,2048,4096)
 SHOPPING_RADIX_EVENTS=1 (set 0 for original event sort)
 SHOPPING_RADIX_CLICKS=1 (set 0 for original click sort)

In WSL (after saving ZIP to Windows Downloads):
 cd /mnt/c/Users/langd4/Documents/GitHub/ShoppingSessions
 cp cpp/main.cpp /tmp/shopping-sort-original.cpp
 unzip -o /mnt/c/Users/langd4/Downloads/shopping_hybrid_radix_complete.zip -d .
 docker build -t shopping-sessions:ci .
 IMAGE=shopping-sessions:ci CPUS=4 MEM=8g PLATFORM=linux/amd64 python scripts/run_fixtures.py fixtures -- scripts/docker_run.sh
 bash test_and_bench.sh /tmp/bench-20m

Restore:
 cp /tmp/shopping-sort-original.cpp cpp/main.cpp
 docker build -t shopping-pipeline .

IMPORTANT: This is an experimental candidate. No Docker/Arrow installed in this
backend; real Docker build, 36 fixtures, CI A10 and speed improvement remain to
be verified on your machine. Do not push until those checks succeed.
