// tm_cuckoo.cpp
#include <vector>
#include <optional>
#include <functional>
#include <random>
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <cassert>

// Compile with: g++ -std=c++17 -O2 -fgnu-tm -pthread tm_cuckoo.cpp -o tm_cuckoo

template <typename T>
class TxCuckooHashSet {
private:
    int LIMIT;                       // Max displacements before resize
    int table_size;                  // number of buckets (per table) - plain int so TM can read it
    std::vector<std::optional<T>> table0;
    std::vector<std::optional<T>> table1;

    // seeds for second hash
    size_t seed, seed1;
    std::mt19937 rng;
    std::hash<T> hasher;

    // Resize coordination - atomic used only outside transactions
    std::atomic<bool> resizing;
    std::mutex resize_mutex;

    inline int hash0(const T& x, int sz) const {
        size_t h = hasher(x);
        return static_cast<int>(h % static_cast<size_t>(sz));
    }
    inline int hash1(const T& x, int sz) const {
        size_t h = hasher(x) ^ seed1;
        return static_cast<int>(h % static_cast<size_t>(sz));
    }

public:
    TxCuckooHashSet(int size = 1024, int limit = 100)
        : LIMIT(limit),
          table_size(size),
          table0(size),
          table1(size),
          rng(std::mt19937(std::random_device{}())),
          resizing(false)
    {
        std::uniform_int_distribution<size_t> dist;
        seed = dist(rng);
        seed1 = dist(rng);
    }

    // Non-transactional quick check: returns true if resizing is in progress
    bool is_resizing() const { return resizing.load(std::memory_order_acquire); }

    // contains wrapped in a small transaction
    bool contains(const T& x) {
        // If a resize is happening, wait until done (simple backoff)
        while (is_resizing()) std::this_thread::yield();

        bool found = false;
        __transaction_atomic {
            int sz = table_size;              // plain read (not atomic)
            int h0 = hash0(x, sz);
            int h1 = hash1(x, sz);
            if (table0[h0].has_value() && table0[h0].value() == x) {
                found = true;
            } else if (table1[h1].has_value() && table1[h1].value() == x) {
                found = true;
            }
        }
        return found;
    }

    // add implemented as repeated short transactions (one swap/place per transaction)
    bool add(const T& x) {
        // wait if resize in progress
        while (is_resizing()) std::this_thread::yield();

        // quick check (non-transactional) to avoid trivial repeated txns
        if (contains(x)) return false;

        T cur = x;
        for (int iter = 0; iter < LIMIT; ++iter) {
            // if resize started, back off and retry
            while (is_resizing()) std::this_thread::yield();

            bool placed = false;
            bool already_present = false;
            __transaction_atomic {
                int sz = table_size;           // plain read inside transaction
                int h0 = hash0(cur, sz);
                int h1 = hash1(cur, sz);

                // duplicate check (this tx reads both)
                if (table0[h0].has_value() && table0[h0].value() == cur) {
                    already_present = true;
                    placed = false;
                } else if (table1[h1].has_value() && table1[h1].value() == cur) {
                    already_present = true;
                    placed = false;
                } else if (!table0[h0].has_value()) {
                    table0[h0] = cur;
                    placed = true;
                } else if (!table1[h1].has_value()) {
                    table1[h1] = cur;
                    placed = true;
                } else {
                    // Evict from table0 at h0 (simple victim choice)
                    T victim = table0[h0].value();
                    table0[h0] = cur;
                    cur = victim;
                    placed = false;
                }
            } // end transaction

            if (already_present) return false;
            if (placed) return true;
            // else continue with new cur (evicted value)
        }

        // too many displacements, attempt to resize (non-transactional)
        // only one thread does the resize at a time (coordinated by resizing atomic + mutex)
        bool expected = false;
        if (resizing.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            // We "won" the right to resize
            std::lock_guard<std::mutex> guard(resize_mutex);

            int old_sz = table_size;
            int new_sz = old_sz * 2;

            // Move old elements out (non-transactionally)
            std::vector<std::optional<T>> old0 = std::move(table0);
            std::vector<std::optional<T>> old1 = std::move(table1);

            // allocate new tables (non-transactionally)
            table0.assign(new_sz, std::nullopt);
            table1.assign(new_sz, std::nullopt);

            // update table_size (plain write)
            table_size = new_sz;

            // reseed hash1 to change second hashing mapping slightly
            std::uniform_int_distribution<size_t> dist;
            seed1 = dist(rng);

            // Reinsert all previous elements *non-transactionally*.
            // Because resizing flag is set, other threads will back off in their loops.
            for (auto &opt : old0) {
                if (opt.has_value()) add(opt.value()); // will retry and eventually place
            }
            for (auto &opt : old1) {
                if (opt.has_value()) add(opt.value());
            }

            // finished resizing
            resizing.store(false, std::memory_order_release);

            // finally retry inserting the current item 'cur' (the last evicted)
            return add(cur);
        } else {
            // someone else is resizing; wait and retry add
            while (is_resizing()) std::this_thread::yield();
            return add(cur); // try again after resize finished
        }
    }

    // remove using a short transaction
    bool remove(const T& x) {
        // wait if resize in progress
        while (is_resizing()) std::this_thread::yield();

        bool removed = false;
        __transaction_atomic {
            int sz = table_size;
            int h0 = hash0(x, sz);
            int h1 = hash1(x, sz);
            if (table0[h0].has_value() && table0[h0].value() == x) {
                table0[h0].reset();
                removed = true;
            } else if (table1[h1].has_value() && table1[h1].value() == x) {
                table1[h1].reset();
                removed = true;
            }
        }
        return removed;
    }

    // rough size (not transactional — may be a bit stale, matches project requirement)
    int size() const {
        int sz = table_size;
        int count = 0;
        // It's OK that this isn't atomic; it's a reported/stored value for debugging/comparison
        for (int i = 0; i < sz; ++i) {
            if (table0[i].has_value()) ++count;
        }
        for (int i = 0; i < sz; ++i) {
            if (table1[i].has_value()) ++count;
        }
        return count;
    }

    // populates with n random ints (calls add)
    void populate(int n) {
        std::uniform_int_distribution<int> dist(0, n * 8);
        for (int i = 0; i < n; ++i) {
            while (!add(dist(rng))) {
                // try again until inserted
                std::this_thread::yield();
            }
        }
    }

    // debug print (non-transactional)
    void print() const {
        int sz = table_size;
        std::cout << "Table size (buckets per table): " << sz << "\n";
        std::cout << "Table0:\n";
        for (int i = 0; i < sz; ++i) {
            if (table0[i].has_value()) std::cout << "[" << i << "]:" << *table0[i] << " ";
            else std::cout << "[" << i << "]:_ ";
        }
        std::cout << "\nTable1:\n";
        for (int i = 0; i < sz; ++i) {
            if (table1[i].has_value()) std::cout << "[" << i << "]:" << *table1[i] << " ";
            else std::cout << "[" << i << "]:_ ";
        }
        std::cout << "\n";
    }
};

// =========================
// Benchmark Driver (TM version) — matches your earlier harness
// =========================
int main() {
    // parameters
    int initial_size = 1000000;    // choose smaller for testing if you have limited memory
    int limit = 100;
    int num_threads = 1;
    int total_ops = 1000000;       // total operations across all threads
    double insert_ratio = 0.30;
    double remove_ratio = 0.30;
    // remainder is contains

    std::cout << "Note: this binary uses GCC transactional memory (-fgnu-tm). If your compiler doesn't support it, compilation will fail.\n";

    TxCuckooHashSet<int> set(initial_size, limit);
    set.populate(initial_size / 2);

    int ops_per_thread = total_ops / num_threads;
    int final_computed_size = set.size();
    std::vector<int> computed_size(num_threads, 0);

    std::cout << "Starting TM benchmark...\n";
    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(std::random_device{}());
            std::uniform_real_distribution<double> op_dist(0.0, 1.0);
            std::uniform_int_distribution<int> key_dist(0, initial_size * 4);

            for (int i = 0; i < ops_per_thread; ++i) {
                double op_choice = op_dist(rng);
                int key = key_dist(rng);

                if (op_choice < insert_ratio) {
                    if (set.add(key)) computed_size[t]++;
                } else if (op_choice < insert_ratio + remove_ratio) {
                    if (set.remove(key)) computed_size[t]--;
                } else {
                    set.contains(key);
                }
            }
        });
    }

    for (auto &th : threads) th.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;

    for (int c : computed_size) final_computed_size += c;

    std::cout << "Benchmark complete.\n";
    std::cout << "Expected final size: " << final_computed_size << "\n";
    std::cout << "Actual final size:   " << set.size() << "\n";
    std::cout << "Time taken:          " << duration.count() << " seconds\n";

    return 0;
}
// Compile with: g++ -std=c++17 -O2 -fgnu-tm -pthread tm_cuckoo.cpp -o tm_cuckoo