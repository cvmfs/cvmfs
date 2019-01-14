# Test variant symlinks
/SITECONF/*

# The following directory once caused I/O errors
/slc6_amd64_gcc700/cms/cmssw/CMSSW_10_3_0_pre6/src/DQMServices/Components/python/test/*

# Two chunked files: the de-duplicated store needs to find the right hashes
/slc6_amd64_gcc700/cms/cmssw/CMSSW_10_2_9/lib/slc6_amd64_gcc700/libFWCoreFramework.so
/slc6_amd64_gcc700/cms/cmssw/CMSSW_10_3_0/lib/slc6_amd64_gcc700/libFWCoreFramework.so

# Include files that are not writable by the owner
/COMP/slc6_amd64_gcc493/external/db4/*
