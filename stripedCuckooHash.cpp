#include <vector>
#include <optional>
#include <functional>
#include <random>
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>

// g++ -std=c++17 -O2 -pthread stripedCuckooHash.cpp -o striped_cuckoo_hash

template <typename T>
class StripedCuckooHashSet {
private:
    int LIMIT;            // Max displacements before resize
    int table_size;       // Number of buckets per table
    int PROBE_SIZE;       // Max elements per bucket
    std::vector<std::vector<T>> table0;
    std::vector<std::vector<T>> table1;

    std::vector<std::mutex> locks0;
    std::vector<std::mutex> locks1;
    std::mutex global_resize_lock;

    // Random seeds for hashing
    size_t seed, seed1;
    std::mt19937 rng;
    std::hash<T> hasher;

    int hash0(const T& x) const {
        return (hasher(x) ^ seed) % table_size;
    }

    int hash1(const T& x) const {
        return (hasher(x) ^ seed1) % table_size;
    }

    // Lock both buckets for an element (in order to avoid deadlock)
    void acquire(const T& x) {
        int h0 = hash0(x) % table_size;
        int h1 = hash1(x) % table_size;
        if (h0 < h1) {
            locks0[h0].lock();
            locks1[h1].lock();
        } else {
            locks1[h1].lock();
            locks0[h0].lock();
        }
    }

    void release(const T& x) {
        locks0[hash0(x) % table_size].unlock();
        locks1[hash1(x) % table_size].unlock();
    }

    // Resize (double capacity)
    void resize() {
        std::lock_guard<std::mutex> global_lock(global_resize_lock);

        int old_capacity = table_size;
        for (auto& l : locks0) l.lock();

        try {
            if (table_size != old_capacity) return; // already resized

            auto old0 = table0;
            auto old1 = table1;
            table_size *= 2;

            table0.assign(table_size, {});
            table1.assign(table_size, {});

            std::uniform_int_distribution<size_t> dist;
            seed = dist(rng);
            seed1 = dist(rng);

            for (auto& row : old0)
                for (auto& x : row)
                    add_internal(x);

            for (auto& row : old1)
                for (auto& x : row)
                    add_internal(x);
        } catch (...) {
            for (auto& l : locks0) l.unlock();
            throw;
        }

        for (auto& l : locks0) l.unlock();
    }

    // Internal add (used during resize without locks)
    bool add_internal(const T& x) {
        if (contains_internal(x)) return false;
        T loop_x = x;
        for (int i = 0; i < LIMIT; ++i) {
            int h0 = hash0(loop_x);
            if ((int)table0[h0].size() < PROBE_SIZE) {
                table0[h0].push_back(loop_x);
                return true;
            }

            int h1 = hash1(loop_x);
            if ((int)table1[h1].size() < PROBE_SIZE) {
                table1[h1].push_back(loop_x);
                return true;
            }

            // Evict random from table0[h0]
            int victim_index = i % PROBE_SIZE;
            T victim = table0[h0][victim_index];
            table0[h0][victim_index] = loop_x;
            loop_x = victim;
        }
        resize();
        return add_internal(loop_x);
    }

    bool contains_internal(const T& x) const {
        int h0 = hash0(x);
        int h1 = hash1(x);
        for (auto& y : table0[h0]) if (y == x) return true;
        for (auto& y : table1[h1]) if (y == x) return true;
        return false;
    }

public:
    StripedCuckooHashSet(int size, int limit, int probe_size = 4)
        : table_size(size),
          LIMIT(limit),
          PROBE_SIZE(probe_size),
          table0(size),
          table1(size),
          locks0(size),
          locks1(size),
          rng(std::mt19937(std::random_device{}())) {
        std::uniform_int_distribution<size_t> dist;
        seed = dist(rng);
        seed1 = dist(rng);
    }

    bool contains(const T& x) {
        acquire(x);
        bool res = contains_internal(x);
        release(x);
        return res;
    }

    bool add(const T& x) {
        acquire(x);
        if (contains_internal(x)) {
            release(x);
            return false;
        }

        bool result = add_internal(x);
        release(x);
        return result;
    }

    bool remove(const T& x) {
        acquire(x);
        int h0 = hash0(x);
        int h1 = hash1(x);
        bool removed = false;

        auto& bucket0 = table0[h0];
        auto& bucket1 = table1[h1];

        for (auto it = bucket0.begin(); it != bucket0.end(); ++it) {
            if (*it == x) {
                bucket0.erase(it);
                removed = true;
                break;
            }
        }
        if (!removed) {
            for (auto it = bucket1.begin(); it != bucket1.end(); ++it) {
                if (*it == x) {
                    bucket1.erase(it);
                    removed = true;
                    break;
                }
            }
        }
        release(x);
        return removed;
    }

    int size() {
        int count = 0;
        for (auto& bucket : table0) count += bucket.size();
        for (auto& bucket : table1) count += bucket.size();
        return count;
    }

    void populate(int n) {
        std::uniform_int_distribution<int> dist(0, n * 8);
        for (int i = 0; i < n; ++i)
            while (!add_internal(dist(rng))) {}
    }
};

// =========================
// Benchmark Driver (Same as Baseline)
// =========================
int main() {
    int initial_size = 1000000;  // try smaller first for safety
    int limit = 100;
    int num_threads = 8;       // try 1, 2, 4, 8, etc.
    int total_ops = 1000000;
    double insert_ratio = 0.30;
    double remove_ratio = 0.30;
    double contains_ratio = 0.40;

    StripedCuckooHashSet<int> set(initial_size, limit);
    set.populate(initial_size / 2);

    int ops_per_thread = total_ops / num_threads;
    int final_computed_size = set.size();
    std::vector<int> computed_size(num_threads, 0);

    std::cout << "Starting concurrent benchmark...\n";
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
                    if (set.add(key))
                        computed_size[t]++;
                } else if (op_choice < insert_ratio + remove_ratio) {
                    if (set.remove(key))
                        computed_size[t]--;
                } else {
                    set.contains(key);
                }
            }
        });
    }

    for (auto& th : threads) th.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;

    for (int c : computed_size)
        final_computed_size += c;

    std::cout << "Benchmark complete.\n";
    std::cout << "Expected final size: " << final_computed_size << "\n";
    std::cout << "Actual final size:   " << set.size() << "\n";
    std::cout << "Time taken:          " << duration.count() << " seconds\n";
}

// g++ -std=c++17 -O2 -pthread stripedCuckooHash.cpp -o striped_cuckoo_hash
//striped_cuckoo_hash