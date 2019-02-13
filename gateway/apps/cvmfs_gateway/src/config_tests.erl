%%%-------------------------------------------------------------------
%%% This file is part of the CernVM File System.
%%%-------------------------------------------------------------------

-module(config_tests).

-include_lib("eunit/include/eunit.hrl").


load_repos_adds_default_keys_test() ->
    RepoCfg = [
        #{domain => <<"test1.domain.org">>,
          keys => [<<"testkey1">>]},
        #{domain => <<"test2.domain.org">>,
          keys => [<<"testkey1">>, <<"testkey2">>]},
        <<"test3.domain.org">>
    ],

    Repos = config:load_repos(RepoCfg),
    #{domain := Name, keys := Keys} = lists:last(Repos),
    ?assert(Name =:= <<"test3.domain.org">>),
    ?assert(Keys =:= [<<"test3.domain.org">>]).


load_keys_adds_missing_key_definition_test() ->
    KeyCfg = [
        #{type => <<"plain_text">>,
          id => <<"testkey1">>,
          secret => <<"SECRET1">>,
          repo_subpath => <<"/">>},
        #{type => <<"plain_text">>,
          id => <<"testkey2">>,
          secret => <<"SECRET2">>,
          repo_subpath => <<"/path">>}
    ],
    Repos = [
        #{domain => <<"test1.domain.org">>,
          keys => [<<"testkey1">>]},
        #{domain => <<"test2.domain.org">>,
          keys => [<<"testkey1">>, <<"testkey2">>]},
        #{domain => <<"test3.domain.org">>,
          keys => [<<"test3.domain.org">>]}
    ],

    Keys = config:load_keys(KeyCfg, Repos, fun(K) -> mock_load_key(K) end),

    #{secret := Secret} = lists:last(Keys),

    ?assert(Secret =:= <<"mocksecret">>).


%% Private functions

mock_load_key(#{type := <<"plain_text">>, id := Id, secret := Secret, repo_subpath := Path}) ->
    #{id => Id, secret => Secret, path => Path};
mock_load_key(#{type := <<"file">>, file_name := FileName, repo_subpath := Path}) ->
    Id = lists:last(binary:split(FileName, <<"/">>, [global])),
    #{id => Id, secret => <<"mocksecret">>, path => Path}.
