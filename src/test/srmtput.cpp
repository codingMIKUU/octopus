#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif

#include "nrfs.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <thread>
#include <vector>

static constexpr uint64_t kReportOpsThreshold = 1024ULL * 1024ULL;

struct ThreadStats {
    std::atomic<uint64_t> total_bytes;
    std::atomic<uint64_t> total_ops;
    std::atomic<uint64_t> errors;
    std::atomic<double> inst_ops_s;
    std::atomic<double> inst_gbps;
    std::atomic<double> avg_ops_s;
    std::atomic<double> avg_gbps;

    ThreadStats()
        : total_bytes(0),
          total_ops(0),
          errors(0),
          inst_ops_s(0.0),
          inst_gbps(0.0),
          avg_ops_s(0.0),
          avg_gbps(0.0) {}
};

struct WorkerArg {
    int tid;
    int total_threads;
    uint64_t io_size;
    uint64_t max_offsets;
    int run_seconds;
    bool use_raw;
    std::atomic<int>* connect_turn;
    std::atomic<bool>* start_gate;
    std::atomic<int>* finished_threads;
    ThreadStats* all_stats;
    ThreadStats* stats;
    std::atomic<int>* ready_threads;
};

static void worker_main(WorkerArg arg) {
    fprintf(stderr, "[srmtput][tid=%d] worker start, connecting...\n", arg.tid);

    while (arg.connect_turn->load(std::memory_order_acquire) != arg.tid) {
        std::this_thread::yield();
    }

    nrfs fs = nrfsConnect("default", arg.total_threads, 0);

    fprintf(stderr, "[srmtput][tid=%d] connect done\n", arg.tid);

    char path[128];
    snprintf(path, sizeof(path), "/fcscale_tput_file_%d", arg.tid);
    fprintf(stderr, "[srmtput][tid=%d] opening file %s ...\n", arg.tid, path);
    nrfsFile file = nrfsOpenFile(fs, path, O_CREAT | O_RDWR);
    if (file == nullptr) {
        fprintf(stderr, "[srmtput][tid=%d] open file failed\n", arg.tid);
        arg.stats->errors.fetch_add(1, std::memory_order_relaxed);
        arg.ready_threads->fetch_add(1, std::memory_order_acq_rel);
        arg.connect_turn->store(arg.tid + 1, std::memory_order_release);
        nrfsDisconnect(fs);
        return;
    }
    fprintf(stderr, "[srmtput][tid=%d] open file ok\n", arg.tid);

    arg.ready_threads->fetch_add(1, std::memory_order_acq_rel);
    arg.connect_turn->store(arg.tid + 1, std::memory_order_release);

    while (!arg.start_gate->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    std::vector<char> buf(arg.io_size, 'a');
    uint64_t op_index = 0;
    uint64_t local_err = 0;
    uint64_t rolling_ops = 0;
    uint64_t rolling_bytes = 0;

    auto start = std::chrono::steady_clock::now();
    auto msr_start = start;

    while (true) {
        auto now = std::chrono::steady_clock::now();
        double elapsed_total = std::chrono::duration<double>(now - start).count();
        if (elapsed_total >= static_cast<double>(arg.run_seconds)) {
            break;
        }

        uint64_t offset = (op_index % arg.max_offsets) * arg.io_size;
        int ret;
        if (arg.use_raw) {
            ret = nrfsRawRead(fs, file, buf.data(), arg.io_size, offset);
            if (ret == 0) ret = static_cast<int>(arg.io_size);
        } else {
            ret = nrfsWrite(fs, file, buf.data(), arg.io_size, offset);
        }

        if (ret < 0) {
            arg.stats->errors.fetch_add(1, std::memory_order_relaxed);
            local_err++;
            if (local_err <= 8 || (local_err % 1000 == 0)) {
                fprintf(stderr,
                        "[srmtput][tid=%d] write failed ret=%d op=%lu off=%lu io=%lu total_err=%lu\n",
                        arg.tid, ret, op_index, offset, arg.io_size, local_err);
            }
            continue;
        }

        arg.stats->total_bytes.fetch_add(static_cast<uint64_t>(ret), std::memory_order_relaxed);
        arg.stats->total_ops.fetch_add(1, std::memory_order_relaxed);
        op_index++;
        rolling_ops += 1;
        rolling_bytes += static_cast<uint64_t>(ret);

        if (rolling_ops >= kReportOpsThreshold) {
            auto msr_end = std::chrono::steady_clock::now();
            double msr_seconds =
                std::chrono::duration<double>(msr_end - msr_start).count();
            double inst_ops_s = (msr_seconds > 0.0) ? (rolling_ops / msr_seconds) : 0.0;
            double inst_gbps =
                (msr_seconds > 0.0) ? (rolling_bytes / msr_seconds * 8.0 / 1e9) : 0.0;
            double avg_ops_s = (elapsed_total > 0.0) ? (op_index / elapsed_total) : 0.0;
            double avg_gbps =
                (elapsed_total > 0.0)
                    ? (arg.stats->total_bytes.load(std::memory_order_relaxed) / elapsed_total * 8.0 / 1e9)
                    : 0.0;

            arg.stats->inst_ops_s.store(inst_ops_s, std::memory_order_relaxed);
            arg.stats->inst_gbps.store(inst_gbps, std::memory_order_relaxed);
            arg.stats->avg_ops_s.store(avg_ops_s, std::memory_order_relaxed);
            arg.stats->avg_gbps.store(avg_gbps, std::memory_order_relaxed);

            printf("main: Thread %d: %.2f ops/s, %.2f Gbps. Seconds = %.1f of %d.\n",
                   arg.tid,
                   inst_ops_s,
                   inst_gbps,
                   elapsed_total,
                   arg.run_seconds);

            if (arg.tid == 0) {
                double total_inst_ops = 0.0;
                double total_inst_gbps = 0.0;
                uint64_t total_ops = 0;
                uint64_t total_bytes = 0;
                uint64_t total_err = 0;
                for (int i = 0; i < arg.total_threads; i++) {
                    total_inst_ops += arg.all_stats[static_cast<size_t>(i)].inst_ops_s.load(std::memory_order_relaxed);
                    total_inst_gbps += arg.all_stats[static_cast<size_t>(i)].inst_gbps.load(std::memory_order_relaxed);
                    total_ops += arg.all_stats[static_cast<size_t>(i)].total_ops.load(std::memory_order_relaxed);
                    total_bytes += arg.all_stats[static_cast<size_t>(i)].total_bytes.load(std::memory_order_relaxed);
                    total_err += arg.all_stats[static_cast<size_t>(i)].errors.load(std::memory_order_relaxed);
                }

                printf("Total tput = %.2f ops/s, %.2f Gbps (ops=%lu bytes=%lu errors=%lu)\n",
                       total_inst_ops,
                       total_inst_gbps,
                       total_ops,
                       total_bytes,
                       total_err);
            }

            rolling_ops = 0;
            rolling_bytes = 0;
            msr_start = msr_end;
        }
    }

    auto end = std::chrono::steady_clock::now();
    double elapsed_total = std::chrono::duration<double>(end - start).count();
    double final_avg_ops = (elapsed_total > 0.0) ? (op_index / elapsed_total) : 0.0;
    double final_avg_gbps =
        (elapsed_total > 0.0)
            ? (arg.stats->total_bytes.load(std::memory_order_relaxed) / elapsed_total * 8.0 / 1e9)
            : 0.0;
    arg.stats->avg_ops_s.store(final_avg_ops, std::memory_order_relaxed);
    arg.stats->avg_gbps.store(final_avg_gbps, std::memory_order_relaxed);

    int finished = arg.finished_threads->fetch_add(1, std::memory_order_acq_rel) + 1;
    fprintf(stderr, "[srmtput][tid=%d] stopping, ops=%lu finished=%d/%d\n",
            arg.tid, op_index, finished, arg.total_threads);

    if (arg.tid == 0) {
        while (arg.finished_threads->load(std::memory_order_acquire) < arg.total_threads) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        double total_avg_ops = 0.0;
        double total_avg_gbps = 0.0;
        uint64_t total_ops = 0;
        uint64_t total_bytes = 0;
        uint64_t total_err = 0;
        for (int i = 0; i < arg.total_threads; i++) {
            total_avg_ops += arg.all_stats[static_cast<size_t>(i)].avg_ops_s.load(std::memory_order_relaxed);
            total_avg_gbps += arg.all_stats[static_cast<size_t>(i)].avg_gbps.load(std::memory_order_relaxed);
            total_ops += arg.all_stats[static_cast<size_t>(i)].total_ops.load(std::memory_order_relaxed);
            total_bytes += arg.all_stats[static_cast<size_t>(i)].total_bytes.load(std::memory_order_relaxed);
            total_err += arg.all_stats[static_cast<size_t>(i)].errors.load(std::memory_order_relaxed);
        }
        printf("[tid=0][final-total] avg=%.2f Gbps %.2f ops/s total_ops=%lu total_bytes=%lu errors=%lu\n",
               total_avg_gbps, total_avg_ops, total_ops, total_bytes, total_err);
    }

    nrfsCloseFile(fs, file);
    nrfsDisconnect(fs);
    fprintf(stderr, "[srmtput][tid=%d] disconnected\n", arg.tid);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr,
                "Usage: %s <threads> <io_size_bytes> [run_seconds=10] [use_raw=0] [max_offsets=1024]\n",
                argv[0]);
        return -1;
    }

    int threads = atoi(argv[1]);
    uint64_t io_size = static_cast<uint64_t>(strtoull(argv[2], nullptr, 10));
    int run_seconds = (argc >= 4) ? atoi(argv[3]) : 10;
    bool use_raw = (argc >= 5) ? (atoi(argv[4]) != 0) : false;
    uint64_t max_offsets = (argc >= 6) ? static_cast<uint64_t>(strtoull(argv[5], nullptr, 10)) : 1024;

    if (threads <= 0 || io_size == 0 || run_seconds <= 0 || max_offsets == 0) {
        fprintf(stderr, "Invalid arguments.\n");
        return -1;
    }

    printf("srmtput start: threads=%d io_size=%lu bytes run=%d s report_ops=%lu use_raw=%d max_offsets=%lu\n",
           threads,
           io_size,
           run_seconds,
           kReportOpsThreshold,
           use_raw ? 1 : 0,
           max_offsets);

    std::atomic<int> connect_turn(0);
    std::atomic<bool> start_gate(false);
    std::atomic<int> finished_threads(0);
    std::atomic<int> ready_threads(0);
    std::vector<ThreadStats> stats(static_cast<size_t>(threads));
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(threads));

    for (int i = 0; i < threads; i++) {
        WorkerArg arg;
        arg.tid = i;
        arg.total_threads = threads;
        arg.io_size = io_size;
        arg.max_offsets = max_offsets;
        arg.run_seconds = run_seconds;
        arg.use_raw = use_raw;
        arg.connect_turn = &connect_turn;
        arg.ready_threads = &ready_threads;
        arg.start_gate = &start_gate;
        arg.finished_threads = &finished_threads;
        arg.all_stats = stats.data();
        arg.stats = &stats[static_cast<size_t>(i)];
        workers.emplace_back(worker_main, arg);
    }

    while (ready_threads.load(std::memory_order_acquire) < threads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    start_gate.store(true, std::memory_order_release);
    for (auto& th : workers) {
        th.join();
    }

    return 0;
}
