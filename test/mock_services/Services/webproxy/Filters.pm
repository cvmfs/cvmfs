package Filters;

##################################
# Ready to use filters for HTTP::Proxy
##################################

use strict;
use warnings;

# Response filter
use Filters::Filter403;
use Filters::FilterCrap;
use Filters::RecordTransfer;

# Request filter
use Filters::ForceBackend;

1;
