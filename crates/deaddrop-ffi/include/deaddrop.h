/*
 * deaddrop.h — C ABI for the deaddrop Rust library.
 *
 * Link against libdeaddrop (.dylib/.so/.dll, or the static .a/.lib).
 * Static linking also needs the platform system libraries:
 *   macOS:   -framework CoreServices -framework CoreFoundation
 *   Windows: advapi32.lib kernel32.lib ntdll.lib userenv.lib ws2_32.lib bcrypt.lib
 *   Linux:   -lpthread -ldl -lm
 *
 * Conventions
 *   - Functions return dd_status; on failure dd_last_error() describes
 *     the problem (thread local, valid until the next call on that thread).
 *   - Strings are UTF-8, NUL terminated.
 *   - Callbacks run on the library's watcher thread, one at a time.
 *     Anything reachable from user_data must be safe to use from there.
 *   - dd_event_view and every string in it are only valid during the
 *     callback. Copy what you need.
 */
#ifndef DEADDROP_H
#define DEADDROP_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum dd_status {
    DD_OK = 0,
    DD_ERR_INVALID_ARGUMENT = 1,
    DD_ERR_IO = 2,
    DD_ERR_INVALID_EVENT = 3,
    DD_ERR_INSECURE_DIRECTORY = 4,
    DD_ERR_WATCHER = 5,
    DD_ERR_INVALID_STATE = 6,
    DD_ERR_SERIALIZATION = 7,
    DD_ERR_PANIC = 8
} dd_status;

typedef enum dd_trusted_owner {
    /* root on Unix; SYSTEM or BUILTIN\Administrators on Windows (default) */
    DD_OWNER_PRIVILEGED = 0,
    /* the account this process runs as */
    DD_OWNER_CURRENT_USER = 1,
    /* no owner check, development only */
    DD_OWNER_ANY = 2
} dd_trusted_owner;

typedef struct dd_producer dd_producer;
typedef struct dd_consumer dd_consumer;

typedef struct dd_event_view {
    const char *schema_version;
    uint64_t seq_id;
    const char *timestamp_utc;
    const char *app_name;
    const char *metadata_json; /* JSON object, e.g. {"confidence":98} */
    const char *event_json;    /* full event as JSON */
} dd_event_view;

typedef void (*dd_event_callback)(const dd_event_view *event, void *user_data);

const char *dd_version(void);
const char *dd_last_error(void);

/* Diagnostic logging to stderr. Level: "off", "error", "warn" (default),
 * "info", "debug" or "trace". The DEADDROP_LOG environment variable sets the
 * initial level; this call overrides it. Lines start with
 * "[deaddrop:producer]" or "[deaddrop:consumer]". */
dd_status dd_set_log_level(const char *level);

/* Producer ------------------------------------------------------------ */

dd_status dd_producer_init(const char *dir, dd_producer **out);
dd_status dd_producer_publish(const dd_producer *producer, uint64_t seq_id,
                                  const char *app_name, const char *metadata_json /* nullable */);
dd_status dd_producer_publish_json(const dd_producer *producer, const char *json, size_t len);
dd_status dd_producer_shutdown(const dd_producer *producer);
void dd_producer_free(dd_producer *producer);

/* Consumer ------------------------------------------------------------ */

dd_status dd_consumer_init(const char *dir, dd_consumer **out);
/* rescan_interval_ms == 0 disables the periodic safety net rescan. */
dd_status dd_consumer_init_ex(const char *dir, dd_trusted_owner trusted_owner,
                                  int32_t replay_existing, uint32_t rescan_interval_ms,
                                  dd_consumer **out);
dd_status dd_consumer_register_listener(dd_consumer *consumer, dd_event_callback callback,
                                            void *user_data);
dd_status dd_consumer_start(dd_consumer *consumer);
/* Joins the watcher thread: when it returns, no callback is running. */
dd_status dd_consumer_shutdown(dd_consumer *consumer);
void dd_consumer_free(dd_consumer *consumer);

#ifdef __cplusplus
}
#endif

#endif /* DEADDROP_H */
