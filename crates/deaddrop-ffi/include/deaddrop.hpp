// deaddrop.hpp — header only C++17 RAII wrapper over deaddrop.h.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "deaddrop.h"

namespace deaddrop {

class Error : public std::runtime_error {
public:
    Error(dd_status status, const std::string &msg) : std::runtime_error(msg), status_(status) {}
    dd_status status() const noexcept { return status_; }

private:
    dd_status status_;
};

inline void check(dd_status s) {
    if (s != DD_OK) throw Error(s, dd_last_error());
}

// "off", "error", "warn" (default), "info", "debug" or "trace". Overrides the
// DEADDROP_LOG environment variable. Lines go to stderr.
inline void set_log_level(const std::string &level) { check(dd_set_log_level(level.c_str())); }

// Owned copy of an event, safe to keep after the callback returns.
struct DetectionEvent {
    std::string schema_version;
    std::uint64_t seq_id = 0;
    std::string timestamp_utc;
    std::string app_name;
    std::string metadata_json;
    std::string event_json;

    static DetectionEvent from_view(const dd_event_view &v) {
        return {v.schema_version, v.seq_id, v.timestamp_utc, v.app_name, v.metadata_json, v.event_json};
    }
};

class Producer {
public:
    explicit Producer(const std::string &dir) { check(dd_producer_init(dir.c_str(), &p_)); }
    ~Producer() { dd_producer_free(p_); }
    Producer(const Producer &) = delete;
    Producer &operator=(const Producer &) = delete;

    void publish(std::uint64_t seq_id, const std::string &app_name, const std::string &metadata_json = "{}") {
        check(dd_producer_publish(p_, seq_id, app_name.c_str(), metadata_json.c_str()));
    }
    void publish_json(const std::string &json) { check(dd_producer_publish_json(p_, json.data(), json.size())); }
    void shutdown() { check(dd_producer_shutdown(p_)); }
    // e.g. "file: /path" or "ipc: named pipe \\.\pipe\name"
    std::string endpoint() const { return dd_producer_endpoint(p_); }

private:
    dd_producer *p_ = nullptr;
};

class Consumer {
public:
    using Listener = std::function<void(const DetectionEvent &)>;

    explicit Consumer(const std::string &dir, dd_trusted_owner owner = DD_OWNER_PRIVILEGED,
                      bool replay_existing = true, std::uint32_t rescan_interval_ms = 5000) {
        check(dd_consumer_init_ex(dir.c_str(), owner, replay_existing ? 1 : 0, rescan_interval_ms, &c_));
    }
    // Shutdown joins the watcher thread before listeners are destroyed.
    ~Consumer() {
        if (c_) {
            dd_consumer_shutdown(c_);
            dd_consumer_free(c_);
        }
    }
    Consumer(const Consumer &) = delete;
    Consumer &operator=(const Consumer &) = delete;

    // Listener runs on the watcher thread. Exceptions are caught and dropped.
    void register_event_listener(Listener fn) {
        listeners_.push_back(std::make_unique<Listener>(std::move(fn)));
        check(dd_consumer_register_listener(c_, &Consumer::trampoline, listeners_.back().get()));
    }
    void start_watching() { check(dd_consumer_start(c_)); }
    // e.g. "file: /path" or "ipc: XPC service com.example"
    std::string endpoint() const { return dd_consumer_endpoint(c_); }
    void shutdown() { check(dd_consumer_shutdown(c_)); }

private:
    static void trampoline(const dd_event_view *v, void *ud) noexcept {
        try {
            (*static_cast<Listener *>(ud))(DetectionEvent::from_view(*v));
        } catch (...) {
            // Never unwind into Rust.
        }
    }

    dd_consumer *c_ = nullptr;
    std::vector<std::unique_ptr<Listener>> listeners_;
};

}  // namespace deaddrop
