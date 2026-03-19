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

struct ThreadStats {
    std::atomic<uint64_t> bytes;
    std::atomic<uint64_t> ops;
    std::atomic<uint64_t> errors;
    ThreadStats() : bytes(0), ops(0), errors(0) {}
};

struct WorkerArg {
    int tid;
    int total_threads;
    uint64_t io_size;
    uint64_t max_offsets;
    bool use_raw;
    std::atomic<bool>* stop;
    ThreadStats* stats;
};

static void worker_main(WorkerArg arg) {
    fprintf(stderr, "[srmtput][tid=%d] worker start, connecting...\n", arg.tid);
    nrfs fs = nrfsConnect("default", arg.total_threads, 0);
    fprintf(stderr, "[srmtput][tid=%d] connect done\n", arg.tid);

    char path[128];
    snprintf(path, sizeof(path), "/fcscale_tput_file_%d", arg.tid);
    fprintf(stderr, "[srmtput][tid=%d] opening file %s ...\n", arg.tid, path);
    nrfsFile file = nrfsOpenFile(fs, path, O_CREAT | O_RDWR);
    if (file == nullptr) {
        fprintf(stderr, "[srmtput][tid=%d] open file failed\n", arg.tid);
        arg.stats->errors.fetch_add(1, std::memory_order_relaxed);
        nrfsDisconnect(fs);
        return;
    }
    fprintf(stderr, "[srmtput][tid=%d] open file ok\n", arg.tid);

    std::vector<char> buf(arg.io_size, 'a');
    uint64_t op_index = 0;
    uint64_t local_err = 0;
    auto last_progress = std::chrono::steady_clock::now();

    while (!arg.stop->load(std::memory_order_relaxed)) {
        uint64_t offset = (op_index % arg.max_offsets) * arg.io_size;
        int ret;
        if (arg.use_raw) {
            ret = nrfsRawWrite(fs, file, buf.data(), arg.io_size, offset);
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

        arg.stats->bytes.fetch_add(static_cast<uint64_t>(ret), std::memory_order_relaxed);
        arg.stats->ops.fetch_add(1, std::memory_order_relaxed);
        op_index++;

        auto now = std::chrono::steady_clock::now();
        double sec = std::chrono::duration<double>(now - last_progress).count();
        if (sec >= 1.0) {
            fprintf(stderr,
                    "[srmtput][tid=%d] alive ops=%lu bytes=%lu last_ret=%d\n",
                    arg.tid, op_index,
                    static_cast<unsigned long>(op_index * arg.io_size), ret);
            last_progress = now;
        }
    }

    fprintf(stderr, "[srmtput][tid=%d] stopping, ops=%lu\n", arg.tid, op_index);
    nrfsCloseFile(fs, file);
    nrfsDisconnect(fs);
    fprintf(stderr, "[srmtput][tid=%d] disconnected\n", arg.tid);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr,
                "Usage: %s <threads> <io_size_bytes> [run_seconds=10] [report_ms=1000] [use_raw=0] [max_offsets=1024]\n",
                argv[0]);
        return -1;
    }

    int threads = atoi(argv[1]);
    uint64_t io_size = static_cast<uint64_t>(strtoull(argv[2], nullptr, 10));
    int run_seconds = (argc >= 4) ? atoi(argv[3]) : 10;
    int report_ms = (argc >= 5) ? atoi(argv[4]) : 1000;
    bool use_raw = (argc >= 6) ? (atoi(argv[5]) != 0) : false;
    uint64_t max_offsets = (argc >= 7) ? static_cast<uint64_t>(strtoull(argv[6], nullptr, 10)) : 1024;

    if (threads <= 0 || io_size == 0 || run_seconds <= 0 || report_ms <= 0 || max_offsets == 0) {
        fprintf(stderr, "Invalid arguments.\n");
        return -1;
    }

    printf("srmtput start: threads=%d io_size=%lu bytes run=%d s report=%d ms use_raw=%d max_offsets=%lu\n",
           threads, io_size, run_seconds, report_ms, use_raw ? 1 : 0, max_offsets);

    std::atomic<bool> stop(false);
    std::vector<ThreadStats> stats(static_cast<size_t>(threads));
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(threads));

    for (int i = 0; i < threads; i++) {
        WorkerArg arg;
        arg.tid = i;
        arg.total_threads = threads;
        arg.io_size = io_size;
        arg.max_offsets = max_offsets;
        arg.use_raw = use_raw;
        arg.stop = &stop;
        arg.stats = &stats[static_cast<size_t>(i)];
        workers.emplace_back(worker_main, arg);
    }

    auto start = std::chrono::steady_clock::now();
    auto last = start;
    uint64_t last_bytes = 0;
    uint64_t last_ops = 0;

    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(report_ms));
        auto now = std::chrono::steady_clock::now();
        double elapsed_total = std::chrono::duration<double>(now - start).count();
        double elapsed_window = std::chrono::duration<double>(now - last).count();

        uint64_t total_bytes = 0;
        uint64_t total_ops = 0;
        uint64_t total_err = 0;
        for (int i = 0; i < threads; i++) {
            total_bytes += stats[static_cast<size_t>(i)].bytes.load(std::memory_order_relaxed);
            total_ops += stats[static_cast<size_t>(i)].ops.load(std::memory_order_relaxed);
            total_err += stats[static_cast<size_t>(i)].errors.load(std::memory_order_relaxed);
        }

        uint64_t win_bytes = total_bytes - last_bytes;
        uint64_t win_ops = total_ops - last_ops;

        double inst_mbps = (elapsed_window > 0.0) ? (win_bytes / elapsed_window / 1024.0 / 1024.0) : 0.0;
        double inst_ops = (elapsed_window > 0.0) ? (win_ops / elapsed_window) : 0.0;
        double avg_mbps = (elapsed_total > 0.0) ? (total_bytes / elapsed_total / 1024.0 / 1024.0) : 0.0;
        double avg_ops = (elapsed_total > 0.0) ? (total_ops / elapsed_total) : 0.0;

        printf("[%.2fs] inst=%.2f MB/s %.2f ops/s | avg=%.2f MB/s %.2f ops/s | total_bytes=%lu total_ops=%lu errors=%lu\n",
               elapsed_total, inst_mbps, inst_ops, avg_mbps, avg_ops,
               total_bytes, total_ops, total_err);

        last = now;
        last_bytes = total_bytes;
        last_ops = total_ops;

        if (elapsed_total >= static_cast<double>(run_seconds)) {
            break;
        }
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto& th : workers) {
        th.join();
    }

    uint64_t total_bytes = 0;
    uint64_t total_ops = 0;
    uint64_t total_err = 0;
    for (int i = 0; i < threads; i++) {
        total_bytes += stats[static_cast<size_t>(i)].bytes.load(std::memory_order_relaxed);
        total_ops += stats[static_cast<size_t>(i)].ops.load(std::memory_order_relaxed);
        total_err += stats[static_cast<size_t>(i)].errors.load(std::memory_order_relaxed);
    }

    double elapsed_total = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    double avg_mbps = (elapsed_total > 0.0) ? (total_bytes / elapsed_total / 1024.0 / 1024.0) : 0.0;
    double avg_ops = (elapsed_total > 0.0) ? (total_ops / elapsed_total) : 0.0;
    printf("final: avg=%.2f MB/s %.2f ops/s total_bytes=%lu total_ops=%lu errors=%lu\n",
           avg_mbps, avg_ops, total_bytes, total_ops, total_err);

    return 0;
}
