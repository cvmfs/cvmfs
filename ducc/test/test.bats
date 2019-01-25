
# to run this tests you will need the utility bats.
# set the variable tool to the path of the actuall docker2cvmfs conversion
# utility
# the test automatically apply migration and remove database, be aware!

tool="../daemon --database $BATS_TMPDIR/db.sqlite"

function migrate_database {
        sudo tool migrate-database --database "$BATS_TMPDIR/db.sqlite"
}

function print_output() {
        lines=("$@")
        for line in "${lines[@]}"
        do
                echo "# $line" >&3
        done
}

function setup() {
        run $tool migrate-database
        run $tool add-user -u setup_user -r registry.hub.docker.com -p setup_pass
        run $tool add-user -u setup_user -r registry.cern.ch -p setup_pass
}

function teardown() {
        rm -f "$BATS_TMPDIR/db.sqlite"
}

@test "2 + 2 = 4" {
        [ $((2 + 2)) -eq 4 ]
}

@test "invoke the daemon" {
        run $tool
        [ "$status" -eq 0 ]
}

@test "migration works" {
        rm -f docker2cvmfs_archive.sqlite
        
        run $tool migrate-database
        [ "$status" -eq 0 ]
}

@test "add user" {
        run $tool add-user -u simone -r cern -p pass
        [ "$status" -eq 0 ]
        
        run $tool list-users -z
        [ "$status" -eq 0 ]
        [ "${lines[0]}" = "user,registry" ]
        [ "${lines[1]}" = "setup_user,registry.cern.ch" ]
        [ "${lines[2]}" = "setup_user,registry.hub.docker.com" ]
        [ "${lines[3]}" = "simone,cern" ]
}

@test "image syntax" {
        run $tool check-image-syntax -z https://gitlab.cern.ch/foo/test/bar:v4@sha256:digest

        [ "$status" -eq 0 ]
        [ "${lines[0]}" = "name,user,scheme,registry,repository,tag,digest,is_thin" ]
        [ "${lines[1]}" = "https://gitlab.cern.ch/foo/test/bar:v4@sha256:digest,,https,gitlab.cern.ch,foo/test/bar,v4,sha256:digest,false" ]
}

@test "add wish" {
        run $tool add-wish \
                -i https://registry.hub.docker.com/atlas/athanalysis:latest \
                -o https://registry.cern.ch/atlas/thin/athanalysis:latest \
                -b setup_user \
                -r setup_registry
        [ "$status" -eq 0 ]

        run $tool list-wishes -z
        [ "$status" -eq 0 ]
        [ "${#lines[@]}" -eq 2 ]
        [ "${lines[0]}" = "id,input_image_id,input_image_name,cvmfs_repo,output_image_id,output_image_name,converted" ]
        [ "${lines[1]}" = "1,1,https://registry.hub.docker.com/atlas/athanalysis:latest,setup_registry,2,https://registry.cern.ch/atlas/thin/athanalysis:latest,false" ]
}

@test "add twice same wish result in error" {
        run $tool add-wish \
                -i https://registry.hub.docker.com/atlas/athanalysis:latest \
                -o https://registry.cern.ch/atlas/thin/athanalysis:latest \
                -b setup_user \
                -r setup_registry
        [ "$status" -eq 0 ]
	run $tool add-wish \
                -i https://registry.hub.docker.com/atlas/athanalysis:latest \
                -o https://registry.cern.ch/atlas/thin/athanalysis:latest \
                -b setup_user \
                -r setup_registry
        [ "$status" -ne 0 ]
}

@test "remove wish" {
        run $tool add-wish \
                -i https://registry.hub.docker.com/atlas/athanalysis:latest \
                -o https://registry.cern.ch/atlas/thin/athanalysis:latest \
                -b setup_user \
                -r setup_registry
        [ "$status" -eq 0 ]
	
        run $tool remove-wish 1
        [ "$status" -eq 0 ]
        [ "${#lines[@]}" -eq 0 ]
        
        run $tool list-wishes -z
        [ "$status" -eq 0 ]
        [ "${#lines[@]}" -eq 1 ]
}
