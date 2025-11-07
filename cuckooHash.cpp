#include <vector>
#include <optional>
#include <functional> //contains hasher
#include <random>
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>

//command line command:
//g++ -std=c++17 -O2 -pthread cuckooHash.cpp -o cuckoo_hash

template <typename T>
class CuckooHashSet {
private:
    int LIMIT; // Max displacements before resize
    int table_size;
    std::vector<std::optional<T>> table0;
    std::vector<std::optional<T>> table1;

    // Random seeds for the two hash functions
    size_t seed, seed1;

    // Random engine for resizing and populating
    std::mt19937 rng;

    std::hash<T> hasher;

    int hash0(const T& x) const {
        return (hasher(x)  ^ seed) % table_size; //return hash(x) % table_size
        // was return ((hasher(x) * table_size )^ seed) % table_size; //return hash(x) % table_size - problem was multiplying iyt by table_size!!
    }

    int hash1(const T& x) const {
        return (hasher(x) ^ seed1) % table_size; //used xor for better randomness / could bit shift too to mess iwth it << 5 -problem was multiplying iyt by table_size!!
        // was return ((hasher(x) * table_size )^ seed1) % table_size; //return hash(x) % table_size
    }

    std::optional<T> swap(int table_index, int pos, const T& x) { //used optional incase return null value
        std::optional<T> old;
        if (table_index == 0) {
            old = table0[pos];
            table0[pos] = x;
        } else {
            old = table1[pos];
            table1[pos] = x;
        }
        //std::cout << *old << std::endl;
        return old;
    }

    // Rehash all elements into a new table with larger size
    void resize() {
        //if (table_size > 1'000'000) { // safety limit warning since reached this
          //  std::cerr << "Resize limit reached (" << table_size << "). Stopping further resizes.\n";
            //return;
        //}
        //int temp_size = table_size;
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

        // Reinsert all elements
        for (auto& i : temp0) {
            if (i.has_value()) {
                add(*i);
                //print();
            }
        }
        for (auto& i : temp1) {
            if (i.has_value()) {
                add(*i);
                //print();
            }
        }
    }

public:
    CuckooHashSet(int size, int limit) //constructor
        : table_size(size),
        LIMIT(limit),
        table0(size),
        table1(size),
        rng(std::mt19937(std::random_device{}())) {

        std::uniform_int_distribution<size_t> dist;
        seed = dist(rng);
        seed1 = dist(rng);
    }


    bool contains(const T& x) const {
        return ((table0[hash0(x)] && *table0[hash0(x)] == x) ||
               (table1[hash1(x)] && *table1[hash1(x)] == x));
    }

    bool add(const T& x) {
        if (contains(x)) {
            return false;
        }
        //T ret_x = x;
        T loop_x = x;
        for (int i = 0; i < LIMIT; i++) {
            auto new_x = swap(0, hash0(loop_x), loop_x);
            if (!(new_x.has_value())){
                return true;
            }
            auto new_new_x = swap(1, hash1(*new_x), *new_x);
            if (!(new_new_x.has_value())) {
                return true;
            }
            loop_x = *new_new_x;
            //ret_x = *new_new_x;
        }
        // Too many displacements — resize and try again
        //std::cout << ret_x << std::endl;
        //print();
        resize();
        return add(loop_x);
    }

    bool remove(const T& x) {
        ///if (!contains(x)){
        //    return false;
        //}
        if (table0[hash0(x)] && *table0[hash0(x)] == x) {
            table0[hash0(x)].reset();
            return true;
        }
        if (table1[hash1(x)] && *table1[hash1(x)] == x) {
            table1[hash1(x)].reset();
            return true;
        }
        return false;
    }

    int size() const {
        int count = 0;
        for (auto& i : table0) if (i.has_value()) count++;
        for (auto& i : table1) if (i.has_value()) count++;
        return count;
    }

    void populate(int n) {
        std::uniform_int_distribution<int> dist(0, n*8); //4* size is the range just like in add or remove range in main
        for (int i = 0; i < n; ++i) {
            while(!(add(dist(rng)))){} //keep adding if already contained
        }
    }

        // Print the contents of both tables for testing purposes
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
    int initial_size = 1000000;     // starting table size 10k, 100k, 1M
    int limit = 100;             // displacement limit, //Common practical values for the maximum path length threshold are typically in the range of 100 to 200 displacements (kicks). 
    int num_threads = 1;         // can test 1, 2, 4, 8, etc.
    int total_ops = 1000000;   // total number of operations, shoudl do 1,000,000
    double insert_ratio = 0.10;  // 10% insert
    double remove_ratio = 0.10;  // 10% remove
    double contains_ratio = 0.80;// 80% contains

    CuckooHashSet<int> set(initial_size, limit);
    set.populate(initial_size / 2); // pre-populate 50% of table
    
     // Each thread performs total_ops / num_threads
    int ops_per_thread = total_ops / num_threads;
    int final_computed_size = set.size(); //current 50% full
    std::vector<int> computed_size(num_threads, 0);

    std::cout << "Starting benchmark test(s)...\n";

    auto start_time = std::chrono::high_resolution_clock::now(); //start clock

    std::vector<std::thread> threads;
    //start assigning each thread and its operations
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(std::random_device{}());
            std::uniform_real_distribution<double> op_dist(0.0, 1.0);
            std::uniform_int_distribution<int> key_dist(0, initial_size * 4);

            //operations for the current thread
            for (int i = 0; i < ops_per_thread; ++i) {
                double op_choice = op_dist(rng);
                int key = key_dist(rng);

                if (op_choice < insert_ratio) { // if in insert ratio
                    if (set.add(key))
                        computed_size[t]++;
                } else if (op_choice < insert_ratio + remove_ratio) { //if in remove ratio
                    if (set.remove(key))
                        computed_size[t]--;
                } else { // else is contains ratio
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
//command line command:
//g++ -std=c++17 -O2 -pthread cuckooHash.cpp -o cuckoo_hash




