#include <vector>
#include <optional>
#include <functional>
#include <random>
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <list>
#include <shared_mutex>

// g++ -std=c++17 -O2 -pthread stripedCuckooHash.cpp -o striped_cuckoo_hash

template <typename T>
class StripedCuckooHashSet {
private:
    int LIMIT;            // Max displacements before resize
    int table_size;       // Number of buckets per table
    int PROBE_SIZE;       // Max elements per bucket
    int THRESHOLD;        // Threshold of when to relocate
    //std::vector<std::vector<T>> table0;
    //std::vector<std::vector<T>> table1;
    // Use std::list<T> for probe sets (as 'oldest' element removal is needed)
    std::vector<std::list<T>> table0;
    std::vector<std::list<T>> table1;

    std::vector<std::mutex> locks0;
    std::vector<std::mutex> locks1;
    std::shared_mutex resize_mutex;

    //std::mutex global_resize_lock;

    // Random seeds for hashing
    size_t seed, seed1;
    std::mt19937 rng;
    std::hash<T> hasher;

    int hash0(const T& x) const { //good
        return (hasher(x) ^ seed) % table_size;
    }

    int hash1(const T& x) const { //good
        return (hasher(x) ^ seed1) % table_size;
    }

    // Lock both buckets for an element (in order to avoid deadlock)
    void acquire(const T& x) { //good
        locks0[hash0(x)].lock();
        locks1[hash1(x)].lock();
    }

    void release(const T& x) { //good
        locks0[hash0(x)].unlock();
        locks1[hash1(x)].unlock();
    }


    // Resize (double capacity)
    void resize() { // Good 
        int old_capacity = table_size;
        //std::lock_guard<std::mutex> global_lock(global_resize_lock);
        // Wait for all adds/deletes to finish and block new ones
        //std::cout << "\n=== ===\n" << "resize called" << "\n-----------\n";
        std::unique_lock<std::shared_mutex> resize_guard(resize_mutex);
        // std::cout << "\n=== ===\n" << "made it past lock" << "\n-----------\n";
        std::cerr << "Resize\n";
        //for (auto& l : locks0) l.lock();
        // dont need both for (auto& l : locks1) l.lock();

        //try {
            if (table_size != old_capacity) return; // already resized or locking issue

            table_size *= 2;
            std::vector<std::list<T>> temp0 = std::move(table0);;
            std::vector<std::list<T>> temp1 = std::move(table1);;
            //std::cout << "\n=== ===\n" << "made it past lock 1" << "\n-----------\n";
            table0.assign(table_size, std::list<T>{});
            table1.assign(table_size, std::list<T>{});
            //std::cout << "\n=== ===\n" << "made it past lock 2" << "\n-----------\n";
            //reassign locks
            //locks0.assign(table_size, std::mutex{});
            //locks1.assign(table_size, std::mutex{});
            locks0 = std::vector<std::mutex>(table_size);
            locks1 = std::vector<std::mutex>(table_size);
            //std::cout << "\n=== ===\n" << "made it past lock 3" << "\n-----------\n";
            std::uniform_int_distribution<size_t> dist;
            seed = dist(rng);
            seed1 = dist(rng);
            //std::cout << "\n=== ===\n" << "made it past lock 4" << "\n-----------\n";
            for (auto& row : temp0)
                for (auto& x : row)
                    add_internal(x);
            //std::cout << "\n=== ===\n" << "made it past lock 5" << "\n-----------\n";
            for (auto& row : temp1)
                for (auto& x : row)
                    add_internal(x);
        //} catch (...) {
        //for (auto& l : locks0) l.unlock();
        //std::cerr << "Resize done\n";
        //std::cerr << table_size << "\n";
          //  throw;
        //}

        //for (auto& l : locks0) l.unlock();
        // dont need both for (auto& l : locks1) l.unlock();
        //std::cout << "\n=== ===\n" << "made it to end of resize" << "\n-----------\n";
    }

    bool add_internal(const T& x) {
        // NOTE: Assume resize_mutex is held exclusively by the caller (resize())
        // and that bucket locks are free to use.
        //acquire(x);
        // NO resize_guard here
        
        int h0 = hash0(x);
        int h1 = hash1(x);
        //int i = -1, h = -1; 
        //bool mustResize = false; // This must be handled by the caller (resize)

        // The bucket logic remains the same 
        //if (present(x)) { return false; }
        std::list<T>& set0 = table0[h0]; 
        std::list<T>& set1 = table1[h1]; 
        
        if ((int)set0.size() < THRESHOLD) { // Threshold check is implicit when re-adding , maybe set to Probe_size
            set0.push_back(x); 
        } else if ((int)set1.size() < THRESHOLD) {  //, maybe set to Probe_size
            set1.push_back(x); 
        } else {
            // Re-adding elements during resize *must* succeed. 
            // If they fail, it's a structural error, but we treat it as an implicit resize fail.
            //release(x); 
            //  can't actually trigger a resize here, so  must rely on the 
            // re-insertion always succeeding, or handle failure. assume it fails only if all PROBE_SIZE is exceeded in both.
            return false; // Should not happen if new size is adequate
        }
        
        // No relocation is performed when re-adding during resize, only simple insertion.
        //release(x);
        return true;
    }

    bool present(const T& x) const{ //good
        int h0 = hash0(x);
        int h1 = hash1(x);
        //locks0[h0].lock();
        for (auto& y : table0[h0]) if (y == x) return true;
        //locks0[h0].unlock();
        //locks1[h1].lock();
        for (auto& y : table1[h1]) if (y == x) return true;
        //locks1[h1].unlock();
        return false;
    }

    bool relocate(int i, int hi) { //check this, make it so that you removestore locally, then check and make sure other table is less
        int hj = 0; // alternate table hash 
        int j = 1 - i; //alternate tables index
        { //resize scope
        std::shared_lock<std::shared_mutex> resize_guard(resize_mutex);
        for (int round = 0; round < LIMIT; round++) {
            std::list<T>& iSet = (i == 0 ? table0 : table1)[hi];
            


            // Check if iSet is below threshold (Fig. 13.27, line 91/94 check)
            if ((int)iSet.size() < THRESHOLD) {
                return true; // Set is now below threshold, successful relocation
            }
            
            // Get the oldest item (front) (Fig. 13.27, line 70)
            T y = iSet.front(); 
            //std::cout << "\n~~~~~~~~~~~~~~~~~`````````~~~~~~~~~~~~~\n" << y << "\n~~~~~~~~~~~~````````s~~~~~~~~~~~~~~~\n";
            //std::cout << "\n~~~~~~~~~~~~~~~~~````````~~~~MADE IT HERE~~~~~~~~````````s~~~~~~~~~~~~~~~\n";
            // Calculate the other hash (Fig. 13.27, line 71-74)
            if (i == 0) {
                hj = hash1(y); 
            } else { // i == 1
                hj = hash0(y);
            }
            
            // Acquire locks for the item y (Fig. 13.27, line 75)
            acquire(y);
            
            // Now safe to access the probe sets of y's location
            std::list<T>& jSet = (j == 0 ? table0 : table1)[hj];
            
        

            // Try block equivalent starts here
            // Check if y is still in iSet and remove it (Fig. 13.27, line 78)
            auto it = std::find(iSet.begin(), iSet.end(), y);
            if (it != iSet.end()) { 
                iSet.erase(it); // Successful removal (line 78), could replace with pop back but size is so small
                
                if ((int)jSet.size() < THRESHOLD) { // jSet is below threshold (line 79)
                    jSet.push_back(y); // Add to back of jSet
                    release(y);
                    return true; // Success (line 81)
                } else if ((int)jSet.size() < PROBE_SIZE) { // jSet is above threshold but not full (line 82)
                    jSet.push_back(y); // Add to back of jSet
                    // Swap i and j for next relocation round (lines 84-86)
                    i = 1 - i; hi = hj; j = 1 - j; 
                } else { // jSet is full (line 87)
                    iSet.push_back(y); // Put y back in iSet (line 88)
                    release(y);
                    return false; // Failed to relocate -> trigger resize (line 89)
                }
            } else { // Another thread removed y (line 91)
                if ((int)iSet.size() >= THRESHOLD) {
                    release(y);
                    continue; // Resume loop (line 92)
                } else {
                    release(y);
                    return true; // iSet is below threshold, success (line 94)
                }
            }
        release(y);
        }
        }
        return false; // Reached LIMIT rounds, trigger resize (line 100)
    }

public:
    StripedCuckooHashSet(int size, int limit, int probe_size, int threshold)
        : table_size(size),
          LIMIT(limit),
          PROBE_SIZE(probe_size),
          THRESHOLD(threshold),
          table0(size),
          table1(size),
          locks0(size),
          locks1(size),
          rng(std::mt19937(std::random_device{}())) {
        std::uniform_int_distribution<size_t> dist;
        seed = dist(rng);
        seed1 = dist(rng);
    }

    bool contains(const T& x) { //good
        // Block if a resize is in progress
        //std::cout << "\n=== in contains ===\n";
        std::shared_lock<std::shared_mutex> resize_guard(resize_mutex);
        //std::cout << "\n=== not stuck at lock ===\n";
        acquire(x);  
        //std::cout << "\n=== good we here ===\n";
        bool res = present(x);
        release(x);
        return res;
    }

    bool add(const T& x) { //do work
        // Block if a resize is in progress
        bool mustResize = false;
        int i = -1, h = -1; // row and column for relocation
        {// set scoped block so resize guard gives out
        std::shared_lock<std::shared_mutex> resize_guard(resize_mutex);
        acquire(x);
        //std::cout << "\n=== add lock set ===\n";
        int h0 = hash0(x);
        //std::cout << "\n=== hash0 ===\n" << h0 << "\n-----------\n";
        int h1 = hash1(x);
        //std::cout << "\n=== hash1 ===\n" << h1 << "\n-----------\n";
        
        if (present(x)) {release(x); return false;}
        std::list<T>& set0 = table0[h0]; 
        std::list<T>& set1 = table1[h1]; 
        if ((int)set0.size() < THRESHOLD) { 
            set0.push_back(x); release(x); return true; 
        } else if ((int)set1.size() < THRESHOLD) { 
            set1.push_back(x); release(x); return true; 
        } else if ((int)set0.size() < PROBE_SIZE) { 
            set0.push_back(x); i = 0; h = h0; 
        } else if ((int)set1.size() < PROBE_SIZE) { 
            set1.push_back(x); i = 1; h = h1; 
        } else {
            mustResize = true; 
        }
        
        release(x);
        } // <-- resize_guard (Shared Lock on resize_mutex) is RELEASED here automatically
       // std::cout << "\n=== add lock released ===\n";
        if (mustResize) { 
            //std::cout << "\n=== hash1 ===\n" << "attempting to resize" << "\n-----------\n";
            resize(); 
            return add(x); // Recursive add(x)
        } else if (i != -1) { // If a relocation was triggered (i, h were set)
            if (!relocate(i, h)) { 
                //std::cout << "\n=== hash1 ===\n" << "attempting to resize" << "\n-----------\n";
                resize(); 
                return add(x); // Recursive add(x) for the element that failed relocation 
            }
        }

        return true; // x must have been added to a set 

    }

    bool remove(const T& x) {
        // Block if a resize is in progress
        std::shared_lock<std::shared_mutex> resize_guard(resize_mutex);
        acquire(x); // line 16

    

        int h0 = hash0(x);
        int h1 = hash1(x);

        std::list<T>& set0 = table0[h0];
        auto it0 = std::find(set0.begin(), set0.end(), x);

        if (it0 != set0.end()) { // line 19
            set0.erase(it0); // line 20
            release(x);
            return true;
        } else {
            std::list<T>& set1 = table1[h1];
            auto it1 = std::find(set1.begin(), set1.end(), x);
            if (it1 != set1.end()) { // line 24
                set1.erase(it1); // line 25
                release(x);
                return true;
            }
        }
        release(x);
        return false; // line 29
    }

    int size() { //good
        int count = 0;
        for (auto& bucket : table0) count += bucket.size();
        for (auto& bucket : table1) count += bucket.size();
        return count;
    }

    void populate(int n) { //good
        std::uniform_int_distribution<int> dist(0, n * 8);
        for (int i = 0; i < n; ++i)
            while (!add_internal(dist(rng))) {}
    }

    void print() {
        // Acquire a shared lock to prevent a resize while printing
        std::shared_lock<std::shared_mutex> resize_guard(resize_mutex);
        
        std::cout << "\n=== Striped Cuckoo Hash Set State ===\n";
        std::cout << "Table Size: " << table_size << std::endl;
        std::cout << "Total Elements: " << size() << std::endl;
        std::cout << "-------------------------------------\n";

        // --- Print Table 0 ---
        std::cout << "Table 0 (Size: " << table0.size() << "):\n";
        for (size_t i = 0; i < table0.size(); ++i) {
            std::cout << "  Bucket [" << i << "]: ";
            
            // NOTE: Must lock the individual bucket mutex before accessing the bucket contents
            // Locking all locks would be impractical for printing, so we'll skip the bucket locks
            // for simple diagnostic printing, but note that concurrent access is UNSAFE here.
            
            if (table0[i].empty()) {
                std::cout << "[EMPTY]";
            } else {
                for (const auto& item : table0[i]) {
                    std::cout << item << " -> ";
                }
                std::cout << "[END]";
            }
            // Print the address of the associated lock
            // We cannot print the lock status (locked/unlocked)
            std::cout << " | Lock Address: " << &locks0[i] << "\n";
        }
        std::cout << "-------------------------------------\n";

        // --- Print Table 1 ---
        std::cout << "Table 1 (Size: " << table1.size() << "):\n";
        for (size_t i = 0; i < table1.size(); ++i) {
            std::cout << "  Bucket [" << i << "]: ";
            
            // NOTE: Skipping bucket lock for diagnostic purposes, concurrent read is UNSAFE.

            if (table1[i].empty()) {
                std::cout << "[EMPTY]";
            } else {
                for (const auto& item : table1[i]) {
                    std::cout << item << " -> ";
                }
                std::cout << "[END]";
            }
            // Print the address of the associated lock
            std::cout << " | Lock Address: " << &locks1[i] << "\n";
        }
        std::cout << "-------------------------------------\n";
    }
};

// =========================
// Benchmark Driver (Same as Baseline)
// =========================
int main() {
    /**
    int initial_size = 3;  // Use a small size for easy inspection
    int limit = 100;
    int num_threads = 1;    // Use 1 thread for initial testing
    //int total_ops = 10;
    //double insert_ratio = 1.0;
    //double remove_ratio = 0.0;
    //double contains_ratio = 0.0;
    int probe_size = 4;
    int threshold = 2;

    //set.print();
    StripedCuckooHashSet<int> set(initial_size, limit, probe_size, threshold);
    set.print();
    // Initial check
    std::cout << "Initial size: " << set.size() << "\n";
    std::cout << "-------------------------------\n";
    
    // Test Values
    const int A = 100;
    const int B = 200;
    const int C = 300;
    const int D = 400;
    const int E = 500;
    const int F = 600;
    const int G = 700;
    const int H = 800;
    const int I = 1;
    const int J = 2;
    const int K = 3;
    const int L = 4;
    const int M = 5;
    const int N = 6;
    const int O = 7;
    const int P = 8;

    // 2. Test Add Operations
    std::cout << "---  Testing Adds ---\n";
    
    // Add A (should succeed)
    set.add(A);
    set.print();
    set.add(B);
    set.print();
    set.add(C);
    set.print();
    set.add(D);
    set.print();
    set.add(E);
    set.print();
    set.add(F);
    set.print();
    set.add(G);
    set.print();
    set.add(H);
    set.print();
    set.add(I);
    set.print();
    set.add(J);
    set.print();
    set.add(K);
    set.print();
    set.add(L);
    set.print();
    set.add(M);
    set.print();
    set.add(N);
    set.print();
    set.add(O);
    set.print();
    set.add(P);
    set.print();
    // 5. Final Check
    std::cout << "-------------------------------\n";
    std::cout << "Final size: " << set.size() << " (Expected: 2)\n";
    set.print(); // Use your diagnostic print function
    */

    ///** average 10 trials, table size, iniital popilate, add remove contains ratio, talk about probe size and threshold, test resize, mention limit
    int initial_size = 1000000;  // try smaller first for safety
    int limit = 100;
    int num_threads = 16;       // try 1, 2, 4, 8, 16 etc.
    int total_ops = 1000000;
    double insert_ratio = 0.10;
    double remove_ratio = 0.10;
    double contains_ratio = 0.80;
    int probe_size = 4;
    int threshold = 2;

    StripedCuckooHashSet<int> set(initial_size, limit, probe_size, threshold);
    //set.print();
    set.populate(initial_size*0.5); //initial_size / 2

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
    //*/
    return 0;
}

// g++ -std=c++17 -O2 -pthread stripedCuckooHash.cpp -o striped_cuckoo_hash
//./striped_cuckoo_hash

