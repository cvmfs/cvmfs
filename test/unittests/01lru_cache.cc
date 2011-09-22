
#include <iostream>
#include <string>
#include <sstream>

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
	cache.setSpecialHashTableKeys(99999999,99999998);
	
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
	
	cout << "--> testing insert/update" << endl;
	if (not insert(3, "three", cache)) return 29;
	if (cache.getNumberOfEntries() != 4) return 30;
	resp = lookup(3, cache);
	if (resp != "three") return 31;
	
	cout << "--> testing cache dropping" << endl;
	cache.drop();
	if (not cache.isEmpty()) return 39;
	if (cache.isFull()) return 40;
	
	cout << "--> testing reinsert" << endl;
	if (not insert(8, "acht", cache)) return 41;
	if (not insert(9, "neun", cache)) return 42;
	if (not insert(10, "zehn", cache)) return 43;
	if (not insert(11, "elf", cache)) return 44;
	if (cache.getNumberOfEntries() != 4) return 45;
	if (not cache.isFull()) return 46;

	cout << "--> testing forget of entries" << endl;
	if (cache.forget(12)) return 47; // not present... should return false
	if (not cache.forget(9)) return 48;
	if (cache.isFull()) return 49;
	resp = lookup(9, cache);
	if (resp != "") return 50;
	if (not insert(13, "dreizehn", cache)) return 51;
	if (not cache.isFull()) return 52;

	cout << "--> testing big cache" << endl;
	
	const unsigned int bigCacheSize = 1290;
	Cache bigCache(bigCacheSize);
	bigCache.setSpecialHashTableKeys(99999999,99999998);
	
	// filling big cache
	for (int i = 0; i < bigCacheSize; ++i) {
		stringstream ss;
		ss << (i+1);
		if (not insert(i, ss.str(), bigCache)) return 53;
	}

	// looking up every second element (sort even numbered keys up)
	for (int i = 0; i < bigCacheSize; i+=2) {
		stringstream ss;
		ss << (i+1);
		resp = lookup(i, bigCache);
		if (resp != ss.str()) return 54;
	}

	// refill cache, overwriting odd numbered keys
	for (int i = 0; i < bigCacheSize; i+=2) {
		stringstream ss;
		ss << (i+bigCacheSize+1);
		if (not insert(i+bigCacheSize, ss.str(), bigCache)) return 55;
	}

	// check an odd number
	resp = lookup(1, bigCache);
	if (resp != "") return 56;

	// check an even number
	resp = lookup(2, bigCache);
	if (resp != "3") return 57;

	cout << "!!! all done - LRU cache seems to work" << endl;
	return 0;
}
