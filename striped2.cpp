#include <vector>
#include <optional>
#include <functional>
#include <random>
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>

// g++ -std=c++17 -O2 -pthread stripedCuckooHash.cpp -o striped_cuckoo

template <typename T>
class StripedCuckooHashSet {
private:
    int LIMIT; // Max displacements before resize
    int table_size;
    std::vector<std::optional<T>> table0;
    std::vector<std::optional<T>> table1;

    // Locks protecting each bucket (striped locking)
    std::vector<std::mutex> locks0;
    std::vector<std::mutex> locks1;
    std::mutex global_resize_lock; // protects global resize

    size_t seed, seed1;
    std::mt19937 rng;
    std::hash<T> hasher;

    int hash0(const T& x) const {
        return (hasher(x) ^ seed) % table_size;
    }

    int hash1(const T& x) const {
        return (hasher(x) ^ seed1) % table_size;
    }

    std::optional<T> swap(int table_index, int pos, const T& x) {
        std::optional<T> old;
        if (table_index == 0) {
            old = table0[pos];
            table0[pos] = x;
        } else {
            old = table1[pos];
            table1[pos] = x;
        }
        return old;
    }

    void acquire(const T& x) {
        int h0 = hash0(x);
        int h1 = hash1(x);
        if (h0 < h1) {
            locks0[h0].lock();
            locks1[h1].lock();
        } else if (h1 < h0) {
            locks1[h1].lock();
            locks0[h0].lock();
        } else {
            // same hash value; just lock one
            locks0[h0].lock();
        }
    }

    void release(const T& x) {
        int h0 = hash0(x);
        int h1 = hash1(x);
        if (h0 == h1) {
            locks0[h0].unlock();
        } else {
            locks1[h1].unlock();
            locks0[h0].unlock();
        }
    }

    void resize() {
        std::unique_lock<std::mutex> resize_guard(global_resize_lock);

        int old_size = table_size;
        table_size *= 2;

        std::vector<std::optional<T>> old0 = std::move(table0);
        std::vector<std::optional<T>> old1 = std::move(table1);

        table0.assign(table_size, std::nullopt);
        table1.assign(table_size, std::nullopt);

        locks0.resize(table_size);
        locks1.resize(table_size);

        std::uniform_int_distribution<size_t> dist;
        seed = dist(rng);
        seed1 = dist(rng);

        for (auto& i : old0)
            if (i.has_value()) add_internal(*i);

        for (auto& i : old1)
            if (i.has_value()) add_internal(*i);
    }

    // Internal add used during resize (no locking)
    bool add_internal(const T& x) {
        if (contains_internal(x)) return false;
        T loop_x = x;

        for (int i = 0; i < LIMIT; i++) {
            auto new_x = swap(0, hash0(loop_x), loop_x);
            if (!new_x.has_value()) return true;
            auto new_new_x = swap(1, hash1(*new_x), *new_x);
            if (!new_new_x.has_value()) return true;
            loop_x = *new_new_x;
        }
        resize();
        return add_internal(loop_x);
    }

    bool contains_internal(const T& x) const {
        return ((table0[hash0(x)] && *table0[hash0(x)] == x) ||
                (table1[hash1(x)] && *table1[hash1(x)] == x));
    }

public:
    StripedCuckooHashSet(int size, int limit)
        : LIMIT(limit),
          table_size(size),
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
        bool result = contains_internal(x);
        release(x);
        return result;
    }

    bool add(const T& x) {
        acquire(x);
        bool result;
        if (contains_internal(x)) {
            result = false;
        } else {
            T loop_x = x;
            bool placed = false;
            for (int i = 0; i < LIMIT; i++) {
                auto new_x = swap(0, hash0(loop_x), loop_x);
                if (!new_x.has_value()) {
                    placed = true;
                    break;
                }
                auto new_new_x = swap(1, hash1(*new_x), *new_x);
                if (!new_new_x.has_value()) {
                    placed = true;
                    break;
                }
                loop_x = *new_new_x;
            }
            if (!placed) {
                release(x);
                resize();
                return add(x);
            }
            result = true;
        }
        release(x);
        return result;
    }

    bool remove(const T& x) {
        acquire(x);
        bool result = false;
        int h0 = hash0(x);
        int h1 = hash1(x);

        if (table0[h0] && *table0[h0] == x) {
            table0[h0].reset();
            result = true;
        } else if (table1[h1] && *table1[h1] == x) {
            table1[h1].reset();
            result = true;
        }
        release(x);
        return result;
    }

    int size() const {
        int count = 0;
        for (auto& i : table0) if (i.has_value()) count++;
        for (auto& i : table1) if (i.has_value()) count++;
        return count;
    }

    void populate(int n) {
        std::uniform_int_distribution<int> dist(0, n * 8);
        for (int i = 0; i < n; ++i) {
            while (!add(dist(rng))) {}
        }
    }

    void print() const {
        std::cout << "\n=== Striped Cuckoo Hash Set State ===\n";
        std::cout << "Table size: " << table_size << "\n";
        for (int i = 0; i < table_size; ++i) {
            if (table0[i]) std::cout << "[0][" << i << "]: " << *table0[i] << "\n";
            if (table1[i]) std::cout << "[1][" << i << "]: " << *table1[i] << "\n";
        }
    }
};


// === Benchmark ===
int main() {
    int initial_size = 1000000;     // starting table size
    int limit = 100;
    int num_threads = 1;
    int total_ops = 1000000;
    double insert_ratio = 0.30;
    double remove_ratio = 0.30;
    double contains_ratio = 0.40;

    StripedCuckooHashSet<int> set(initial_size, limit);
    set.populate(initial_size / 2);

    int ops_per_thread = total_ops / num_threads;
    int final_computed_size = set.size();
    std::vector<int> computed_size(num_threads, 0);

    std::cout << "Starting benchmark test(s)...\n";
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

    for (auto& th : threads) th.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;

    for (int c : computed_size) final_computed_size += c;

    std::cout << "Benchmark complete.\n";
    std::cout << "Expected final size: " << final_computed_size << "\n";
    std::cout << "Actual final size:   " << set.size() << "\n";
    std::cout << "Time taken:          " << duration.count() << " seconds\n";
}
// g++ -std=c++17 -O2 -pthread striped2.cpp -o striped_2
//striped_2