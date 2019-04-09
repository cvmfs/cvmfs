package gateway

/*
valid_key_files_test() ->
    Data = [<<"plain_text id secret">>,
            <<"    plain_text   id   secret    ">>,
            <<"\tplain_text\tid\tsecret\t">>,
            <<" \t   plain_text \t  id \t  secret \t \n  ">>],
    [?assert({ok, <<"plain_text">>, <<"id">>, <<"secret">>} =:= cvmfs_keys:parse_binary(D)) || D <- Data].

invalid_key_files_test() ->
    Data = [<<"plane_text id secret">>,
            <<"plain_textid secret">>,
            <<"plain_text id secret garbage">>],
    [?assert({error, invalid_format} =:= cvmfs_keys:parse_binary(D)) || D <- Data].
*/
