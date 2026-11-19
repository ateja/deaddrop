// C++ consumer agent. Build (Linux/macOS, after `cargo build --release`):
//   c++ -std=c++17 -I crates/deaddrop-ffi/include
//       crates/deaddrop-ffi/examples/consumer.cpp
//       -L target/release -ldeaddrop -o consumer_cpp
// Usage: consumer_cpp <dir> <expected_events> [--current-user]
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>

#include "deaddrop.hpp"

int main(int argc, char **argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s <dir> <expected_events> [--current-user]\n", argv[0]);
        return 2;
    }
    const std::string dir = argv[1];
    const long expected = std::strtol(argv[2], nullptr, 10);
    const bool current_user = argc > 3 && std::strcmp(argv[3], "--current-user") == 0;

    try {
        // 1. Initialize targeting the agreed-upon directory
        deaddrop::Consumer consumer(dir, current_user ? DD_OWNER_CURRENT_USER : DD_OWNER_PRIVILEGED);

        std::mutex m;
        std::condition_variable cv;
        long received = 0;

        // 2. Register the handler triggered asynchronously by the OS watcher
        consumer.register_event_listener([&](const deaddrop::DetectionEvent &e) {
            std::printf("Received event %llu: %s %s\n", static_cast<unsigned long long>(e.seq_id),
                        e.app_name.c_str(), e.metadata_json.c_str());
            std::fflush(stdout);
            std::lock_guard<std::mutex> lk(m);
            ++received;
            cv.notify_all();
        });

        // 3. Start the OS-native watcher
        consumer.start_watching();
        std::printf("ready\n");
        std::fflush(stdout);

        std::unique_lock<std::mutex> lk(m);
        bool ok = cv.wait_for(lk, std::chrono::seconds(10), [&] { return received >= expected; });
        lk.unlock();

        // 4. Stop the watcher and release handles
        consumer.shutdown();
        return ok ? 0 : 1;
    } catch (const deaddrop::Error &e) {
        std::fprintf(stderr, "deaddrop error %d: %s\n", static_cast<int>(e.status()), e.what());
        return 1;
    }
}
