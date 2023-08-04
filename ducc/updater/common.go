package updater

import "sync"

// TODO: Proper locking system for cvmfs. Need to look at existing code.
var cvmfsLock sync.Mutex
