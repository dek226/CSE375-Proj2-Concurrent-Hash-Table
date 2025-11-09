#include <vector>
#include <optional>
#include <functional> 
#include <random>
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>

// command line command:
// g++ -std=c++17 -O2 -fgnu-tm -pthread cuckooHash_TM.cpp -o cuckoo_hash_tm

template <typename T>
class CuckooHashSet {
private:
    int LIMIT; 
    int table_size;
    
    // Member variables for the tables
    std::vector<std::optional<T>> table0;
    std::vector<std::optional<T>> table1;

    size_t seed, seed1;
    std::mt19937 rng;
    std::hash<T> hasher;

    // --- Helper functions marked as transaction_safe ---
    int hash0(const T& x) const __attribute__((transaction_safe)) {
        return (hasher(x) ^ seed) % table_size;
    }

    int hash1(const T& x) const __attribute__((transaction_safe)) {
        return (hasher(x) ^ seed1) % table_size; 
    }

    std::optional<T> swap(int table_index, int pos, const T& x) __attribute__((transaction_safe)) {
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

    // Resize logic must be safe for use inside a transaction
    void resize() __attribute__((transaction_safe)) {
        // The resize process must be atomic, ensured by the outer transaction block.
        
        table_size = table_size * 2;

        // Save old elements
        std::vector<std::optional<T>> temp0 = std::move(table0);
        std::vector<std::optional<T>> temp1 = std::move(table1);

        // Create new empty tables
        table0.assign(table_size, std::nullopt);
        table1.assign(table_size, std::nullopt);

        // Reseed hash functions
        std::uniform_int_distribution<size_t> dist;
        seed = dist(rng);
        seed1 = dist(rng);

        // Reinsert all elements (recursive calls to add MUST be safe)
        for (auto& i : temp0) {
            if (i.has_value()) {
                add(*i);
            }
        }
        for (auto& i : temp1) {
            if (i.has_value()) {
                add(*i);
            }
        }
    }

public:
    CuckooHashSet(int size, int limit) 
    : table_size(size), LIMIT(limit), table0(size), table1(size), 
      rng(std::mt19937(std::random_device{}())) {
        std::uniform_int_distribution<size_t> dist;
        seed = dist(rng);
        seed1 = dist(rng);
    }

    // --- Core Operations wrapped in __transaction_atomic ---

    bool contains(const T& x) const {
        bool result;
        __transaction_atomic {
            // All reads happen here and are tracked by TM
            result = ((table0[hash0(x)] && *table0[hash0(x)] == x) ||
                     (table1[hash1(x)] && *table1[hash1(x)] == x));
        }
        return result;
    }

    // This function must be marked transaction_safe because it is called recursively in resize
    bool add(const T& x) __attribute__((transaction_safe)) {
        bool result = false; // Initialize result outside the loop/transaction
        
        __transaction_atomic {
            T loop_x; // Declare loop_x outside the potential break/goto points

            do { // Use do-while(false) structure to allow 'break' instead of 'goto'
                if (contains(x)) {
                    result = false;
                    break; 
                }
                
                loop_x = x; // Initialization moved inside the do-block

                for (int i = 0; i < LIMIT; i++) {
                    auto new_x = swap(0, hash0(loop_x), loop_x);
                    if (!(new_x.has_value())){
                        result = true;
                        goto exit_loop; // Safely jump out of the inner loop
                    }
                    auto new_new_x = swap(1, hash1(*new_x), *new_x);
                    if (!(new_new_x.has_value())) {
                        result = true;
                        goto exit_loop; // Safely jump out of the inner loop
                    }
                    loop_x = *new_new_x;
                }
                
                // Too many displacements — resize and try again
                resize();
                result = add(loop_x); // Recursive add call (safe)
                
            exit_loop:; // Label used for exiting the inner 'for' loop
            } while (false); // End of do-while block acting as a transaction body
        }
        return result;
    }

    bool remove(const T& x) {
        bool result;
        __transaction_atomic {
            int h0 = hash0(x);
            int h1 = hash1(x);

            do { // Use do-while(false) structure to allow 'break'
                if (table0[h0] && *table0[h0] == x) {
                    table0[h0].reset();
                    result = true;
                    break;
                }
                if (table1[h1] && *table1[h1] == x) {
                    table1[h1].reset();
                    result = true;
                    break;
                }
                result = false;
            } while (false);
        }
        return result;
    }

    // Other utility functions:
    
    int size() const __attribute__((transaction_safe)) {
        int count;
        __transaction_atomic {
            count = 0;
            for (auto& i : table0) if (i.has_value()) count++;
            for (auto& i : table1) if (i.has_value()) count++;
        }
        return count;
    }

    void populate(int n) {
        std::uniform_int_distribution<int> dist(0, n*8);
        for (int i = 0; i < n; ++i) {
            while(!(add(dist(rng)))){}
        }
    }

    void print() const {
        std::cout << "\n=== Cuckoo Hash Set State ===\n";
        std::cout << "Table size: " << table_size << "\n";

        std::cout << "\nTable 0:\n";
        for (int i = 0; i < table_size; ++i) {
            if (table0[i].has_value())
                std::cout << "[" << i << "]: " << *table0[i] << "\n";
            else
                std::cout << "[" << i << "]: (empty)\n";
        }

        std::cout << "\nTable 1:\n";
        for (int i = 0; i < table_size; ++i) {
            if (table1[i].has_value())
                std::cout << "[" << i << "]: " << *table1[i] << "\n";
            else
                std::cout << "[" << i << "]: (empty)\n";
        }

        std::cout << "==============================\n";
    }
};

// Example usage:
int main() {
    int initial_size = 1000000;
    int limit = 100;
    int num_threads = 8; 
    int total_ops = 1000000;
    double insert_ratio = 0.30;
    double remove_ratio = 0.30;
    double contains_ratio = 0.40;

    CuckooHashSet<int> set(initial_size, limit);
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

    for (int c : computed_size){
        final_computed_size += c;
    }

    std::cout << "Benchmark test(s) complete.\n";
    std::cout << "Expected final size: " << final_computed_size << "\n";
    std::cout << "Actual final size:   " << set.size() << "\n";
    std::cout << "Time taken:          " << duration.count() << " seconds\n";

    return 0;
}

// command line command:
// g++ -std=c++17 -O2 -fgnu-tm -pthread cuckooHash_TM.cpp -o cuckoo_hash_tm
// cuckoo_hash_tm