
#include <iostream>
#include <string>

#include "lru_cache.h"

using namespace std;

typedef cvmfs::LruCache<int, string> Cache;

bool insert(int i, string s, Cache &cache) {
	bool res = cache.insert(i, s);
	cout << "inserting (" << i << ", " << s << ") " << "size is now: " << cache.getNumberOfEntries() << endl;
	return res;
}

string lookup(int i, Cache &cache) {
	string s;
	if (cache.lookup(i, s)) {
		cout << "found entry for " << i << " --> " << s << endl;
		return s;
	} else {
		cout << "cache miss" << endl;
		return "";
	}
}

int main(int argc, char **argv) {
	cout << "--> create cache (size of 4)" << endl;
	Cache cache(4);
	
	cout << "--> testing cache inital state" << endl;
	if (cache.getNumberOfEntries() != 0) return 1;
	if (not cache.isEmpty()) return 2;
	if (cache.isFull()) return 3;
	
	cout << "--> testing cache insertion" << endl;
	if (not insert(1, "eins", cache)) return 4;
	if (cache.getNumberOfEntries() != 1) return 5;
	if (not insert(2, "zwei", cache)) return 6;
	if (cache.getNumberOfEntries() != 2) return 7;
	if (not insert(3, "drei", cache)) return 8;
	if (cache.getNumberOfEntries() != 3) return 9;
	if (not insert(4, "vier", cache)) return 10;
	if (cache.getNumberOfEntries() != 4) return 11;
	
	cout << "--> cache is now full... check this" << endl;
	if (not cache.isFull()) return 12;
	if (cache.isEmpty()) return 13;
	
	cout << "--> check lookup" << endl;
	string resp;
	resp = lookup(1, cache);
	if (resp != "eins") return 14;
	resp = lookup(2, cache);
	if (resp != "zwei") return 15;
	resp = lookup(3, cache);
	if (resp != "drei") return 16;
	resp = lookup(4, cache);
	if (resp != "vier") return 17;
	
	cout << "--> testing LRU (pushes out key=1)" << endl;
	if (not insert(5, "fünf", cache)) return 18;
	if (cache.getNumberOfEntries() != 4) return 19;
	if (not cache.isFull()) return 20;
	
	cout << "--> testing cache miss" << endl;
	resp = lookup(42, cache);
	if (resp != "") return 21;
	resp = lookup(1, cache); // should be pushed out by insert(5)
	if (resp != "") return 22;
	
	cout << "--> testing resize cache" << endl;
	cache.resize(10);
	if (cache.isFull()) return 23;
	if (cache.isEmpty()) return 24;
	if (cache.getNumberOfEntries() != 4) return 25;
	
	cout << "--> testing insert into now bigger cache" << endl;
	if (not insert(6, "sechs", cache)) return 26;
	if (cache.getNumberOfEntries() != 5) return 27;
	if (cache.isFull()) return 28;
	
	cout << "--> testing insert/update" << endl;
	if (not insert(3, "three", cache)) return 29;
	if (cache.getNumberOfEntries() != 5) return 30;
	resp = lookup(3, cache);
	if (resp != "three") return 31;
	
	cout << "--> testing resize cache with truncation" << endl;
	cache.resize(3);
	if (not cache.isFull()) return 32;
	if (cache.isEmpty()) return 33;
	if (cache.getNumberOfEntries() != 3) return 34;
	resp = lookup(5, cache);
	if (resp != "fünf") return 35;
	
	cout << "--> testing LRU again" << endl;
	if (not insert(7, "sieben", cache)) return 36;
	if (cache.getNumberOfEntries() != 3) return 37;
	resp = lookup(6, cache); // should be dropped by last insert
	if (resp != "") return 38;
	
	cout << "--> testing cache dropping" << endl;
	cache.drop();
	if (not cache.isEmpty()) return 39;
	if (cache.isFull()) return 40;
	
	cout << "--> testing reinsert" << endl;
	if (not insert(8, "acht", cache)) return 41;
	if (cache.getNumberOfEntries() != 1) return 42;
	
	cout << "!!! all done - LRU cache seems to work" << endl;
	return 0;
}
