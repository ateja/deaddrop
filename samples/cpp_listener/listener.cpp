// Sample consumer: listens for detection events, prints each one as it
// arrives, collects them, and prints a summary on exit.
//
//   listener [--dir PATH] [--count N] [--timeout SECONDS] [--trust MODE] [--new-only] [--debug]
//
// Stops after N events (--count), after --timeout seconds, or on Ctrl+C.
// Exit code: 0 on success, 1 on error, 2 if --count was not reached in time.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "deaddrop.hpp"

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

namespace {

std::atomic<bool> g_stop{false};

extern "C" void on_signal(int) { g_stop.store(true); }

struct Options {
    std::string dir = (fs::temp_directory_path() / "deaddrop-demo" / "events").string();
    long count = 0;          // 0 = unlimited
    double timeout_s = 0.0;  // 0 = no timeout
    dd_trusted_owner trust = DD_OWNER_CURRENT_USER;
    bool replay_existing = true;
    bool debug = false;
};

[[noreturn]] void usage(const char *argv0, int code) {
    std::fprintf(code ? stderr : stdout,
                 "usage: %s [--dir PATH] [--count N] [--timeout SECONDS]\n"
                 "          [--trust privileged|current-user|any] [--new-only] [--debug]\n"
                 "\n"
                 "  --dir       shared directory (default: <temp>/deaddrop-demo/events)\n"
                 "  --count     stop after N events (default: run until Ctrl+C)\n"
                 "  --timeout   stop after this many seconds\n"
                 "  --trust     who must own the directory (default: current-user;\n"
                 "              production deployments should use privileged)\n"
                 "  --new-only  ignore events already on disk at startup\n"
                 "  --debug     print the library's internal steps to stderr\n"
                 "              (same as DEADDROP_LOG=debug)\n",
                 argv0);
    std::exit(code);
}

Options parse(int argc, char **argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc) usage(argv[0], 1);
            return argv[++i];
        };
        if (a == "--dir") {
            o.dir = value();
        } else if (a == "--count") {
            o.count = std::strtol(value().c_str(), nullptr, 10);
        } else if (a == "--timeout") {
            o.timeout_s = std::strtod(value().c_str(), nullptr);
        } else if (a == "--trust") {
            const std::string t = value();
            if (t == "privileged") o.trust = DD_OWNER_PRIVILEGED;
            else if (t == "current-user") o.trust = DD_OWNER_CURRENT_USER;
            else if (t == "any") o.trust = DD_OWNER_ANY;
            else usage(argv[0], 1);
        } else if (a == "--new-only") {
            o.replay_existing = false;
        } else if (a == "--debug") {
            o.debug = true;
        } else if (a == "-h" || a == "--help") {
            usage(argv[0], 0);
        } else {
            usage(argv[0], 1);
        }
    }
    return o;
}

}  // namespace

int main(int argc, char **argv) {
    const Options opt = parse(argc, argv);
    if (opt.debug) deaddrop::set_log_level("debug");
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    const auto started = Clock::now();
    auto timed_out = [&] {
        return opt.timeout_s > 0 &&
               std::chrono::duration<double>(Clock::now() - started).count() >= opt.timeout_s;
    };

    // The publisher creates the directory. Wait for it so the two programs
    // can be started in either order.
    if (!fs::exists(opt.dir)) {
        std::printf("[listener] waiting for %s to be created by the publisher...\n", opt.dir.c_str());
        std::fflush(stdout);
        while (!fs::exists(opt.dir)) {
            if (g_stop || timed_out()) {
                std::fprintf(stderr, "[listener] directory never appeared\n");
                return 2;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    std::mutex mu;
    std::condition_variable cv;
    std::vector<deaddrop::DetectionEvent> collected;

    try {
        deaddrop::Consumer consumer(opt.dir, opt.trust, opt.replay_existing);

        // Runs on the library's watcher thread, one event at a time.
        consumer.register_event_listener([&](const deaddrop::DetectionEvent &e) {
            std::lock_guard<std::mutex> lk(mu);
            collected.push_back(e);
            std::printf("[listener] recv #%zu seq=%llu app='%s' at=%s metadata=%s\n", collected.size(),
                        static_cast<unsigned long long>(e.seq_id), e.app_name.c_str(), e.timestamp_utc.c_str(),
                        e.metadata_json.c_str());
            std::fflush(stdout);
            cv.notify_all();
        });

        consumer.start_watching();
        std::printf("[listener] watching %s (Ctrl+C to stop)\n", opt.dir.c_str());
        std::fflush(stdout);

        {
            std::unique_lock<std::mutex> lk(mu);
            while (!g_stop && !timed_out() &&
                   !(opt.count > 0 && static_cast<long>(collected.size()) >= opt.count)) {
                cv.wait_for(lk, std::chrono::milliseconds(100));
            }
        }

        // Joins the watcher thread: no callback runs after this returns.
        consumer.shutdown();
    } catch (const deaddrop::Error &e) {
        std::fprintf(stderr, "[listener] deaddrop error %d: %s\n", static_cast<int>(e.status()), e.what());
        return 1;
    }

    // Summary of everything collected.
    std::map<std::string, int> per_app;
    for (const auto &e : collected) ++per_app[e.app_name];
    std::printf("[listener] collected %zu event(s)\n", collected.size());
    if (!collected.empty()) {
        auto [lo, hi] = std::minmax_element(collected.begin(), collected.end(),
                                            [](const auto &a, const auto &b) { return a.seq_id < b.seq_id; });
        std::printf("[listener]   seq range: %llu .. %llu\n", static_cast<unsigned long long>(lo->seq_id),
                    static_cast<unsigned long long>(hi->seq_id));
        for (const auto &[app, n] : per_app) std::printf("[listener]   %-18s %d\n", app.c_str(), n);
    }
    std::fflush(stdout);

    const bool reached = opt.count == 0 || static_cast<long>(collected.size()) >= opt.count;
    return reached ? 0 : 2;
}
