package constants

import (
	"os"
)

var SubDirInsideRepo = ".layers"
var ChainSubDir = ".chains"
var DirtyChainSubDir = ".dirty-chains"

const LayersDirectory string = ".layers" // To replace SubDirInsideRepo
const LayersRootFSSubDir string = "layerfs"

const PodmanSubDir string = ".podmanStore"
const ImagesSubDir string = ".images"

var DirPermision = os.FileMode(0755)
var FilePermision = os.FileMode(0644)
