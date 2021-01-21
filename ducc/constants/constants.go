package constants

import (
	"os"
)

var SubDirInsideRepo = ".layers"
var ChainSubDir = ".chains"
var DirtyChainSubDir = ".dirty-chains"

var DirPermision = os.FileMode(0755)
var FilePermision = os.FileMode(0644)
