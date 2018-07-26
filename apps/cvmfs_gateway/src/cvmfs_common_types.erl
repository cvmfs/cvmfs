%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%
%%% @doc Definition of some common CVMFS types
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(cvmfs_common_types).

-type repository_tag() :: {binary(), binary(), binary()}.

-type commit_error() :: {error, merge_failure |
                                io_error |
                                misc_error |
                                missing_reflog |
                                worker_timeout}.