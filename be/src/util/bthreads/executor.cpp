// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "util/bthreads/executor.h"

#include "common/config.h"
#include "util/threadpool.h"

namespace starrocks::bthreads {

ThreadPoolExecutor::~ThreadPoolExecutor() {
    if (_ownership == kTakesOwnership) delete _thread_pool;
}

int ThreadPoolExecutor::submit(void* (*fn)(void*), void* args) {
    Status st;
    while (true) {
        st = _thread_pool->submit_func([=]() { fn(args); });
        if (!st.is_service_unavailable()) break;

        // There are two scenarios that will return service unaviable
        // 1. The first scenario is that there is a lot of concurrency and tablet,
        //    write speed is slower than send speed,
        //    so that we can sleep here back pressure sender(rpc request memory consume will trace by MemTrack)
        // 2. The second scenrio is that storage hang, task will be blocked continuously
        //    We will log FATAL after be_exit_after_disk_write_hang_second,
        //    eliminate the long tail of writes while keeping users aware of storage failures
        MonoTime now_timestamp = MonoTime::Now();
        if (now_timestamp.GetDeltaSince(_thread_pool->last_active_timestamp())
                    .MoreThan(MonoDelta::FromSeconds(starrocks::config::be_exit_after_disk_write_hang_second))) {
            LOG(WARNING) << "write hang after " << starrocks::config::be_exit_after_disk_write_hang_second << " second";
            break;
        }
        LOG(INFO) << "async_delta_writer is busy, retry after " << _busy_sleep_ms << "ms";
        bthread_usleep(_busy_sleep_ms * 1000);
    }
    LOG_IF(FATAL, !st.ok()) << "BE exit since submit write fail err=" << st;
    return st.ok() ? 0 : -1;
}

} // namespace starrocks::bthreads
