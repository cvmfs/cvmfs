#
# This file is part of the CernVM File System
#
# Implementation of the "cvmfs_server generate-man" command

cvmfs_server_generate_man() {
  check_parameter_count 1 $#
  local CVMFS_SERVER_SUBCOMMAND_SHORT=""
  local CVMFS_SERVER_SUBCOMMAND_SYNOPSIS=""
  local CVMFS_SERVER_SUBCOMMAND_DESCRIPTION=""
  local CVMFS_SERVER_SUBCOMMAND_OPTIONS=""
  local TMP_SUBCOMMAND_OUTPUT=""
  local TMP_OUTPUT="$1"/cvmfs_server.adoc

  echo "\
cvmfs_server(8)
===============

NAME
----
cvmfs_server - Software distribution service


SYNOPSIS
--------
_cvmfs_server_ <command> [options] [parameters]


DESCRIPTION
-----------
TODO


COMMANDS
--------" > "$TMP_OUTPUT"


  for subcommand in $_CVMFS_SERVER_COMMANDS; do
    CVMFS_SERVER_SUBCOMMAND_SHORT="$(eval echo '"${'$(echo "_CVMFS_DOC_${subcommand}_SHORT" |tr '[a-z]-' '[A-Z]_')'}"')"

    echo "*cvmfs_server-${subcommand}*(8)" >> "$TMP_OUTPUT"
    if [ x"$CVMFS_SERVER_SUBCOMMAND_SHORT" = x"" ]; then
      echo "No short description provided for ${subcommand}" >&2
      return 1
    fi
    echo "    $CVMFS_SERVER_SUBCOMMAND_SHORT
    " >> "$TMP_OUTPUT"
  done


  echo "

EXAMPLES
--------
[verse]
____
Write a hello world message to a new CVMFS repository

    # cvmfs_server mkfs new.test.repo
    # cvmfs_server transaction new.test.repo
    # echo \"Hello World\" > /cvmfs/new.test.repo/hello_world
    # cvmfs_server publish new.test.repo
____

SEE ALSO
--------
*fuse*(8)" >> "$TMP_OUTPUT"


  ##############################################################################
  #                  Generate manual page for all subcommands
  ##############################################################################

  for subcommand in $_CVMFS_SERVER_COMMANDS; do
    TMP_SUBCOMMAND_OUTPUT="/$1/cvmfs_server-${subcommand}.adoc"
    CVMFS_SERVER_SUBCOMMAND_SHORT="$(eval echo '"${'$(echo "_CVMFS_DOC_${subcommand}_SHORT" |tr '[a-z]-' '[A-Z]_')'}"')"
    CVMFS_SERVER_SUBCOMMAND_SYNOPSIS="$(eval echo '"${'$(echo "_CVMFS_DOC_${subcommand}_SYNOPSIS" |tr '[a-z]-' '[A-Z]_')'}"')"
    CVMFS_SERVER_SUBCOMMAND_DESCRIPTION="$(eval echo '"${'$(echo "_CVMFS_DOC_${subcommand}_DESCRIPTION" |tr '[a-z]-' '[A-Z]_')'}"')"
    CVMFS_SERVER_SUBCOMMAND_OPTIONS="$(eval echo '"${'$(echo "_CVMFS_DOC_${subcommand}_OPTIONS" |tr '[a-z]-' '[A-Z]_')'}"')"
    CVMFS_SERVER_SUBCOMMAND_EXAMPLES="$(eval echo '"${'$(echo "_CVMFS_DOC_${subcommand}_EXAMPLES" |tr '[a-z]-' '[A-Z]_')'}"')"
    CVMFS_SERVER_SUBCOMMAND_SEE_ALSO="$(eval echo '"${'$(echo "_CVMFS_DOC_${subcommand}_SEE_ALSO" |tr '[a-z]-' '[A-Z]_')'}"')"

    echo "\
= cvmfs_server-${subcommand}(8)

NAME
----
cvmfs_server-${subcommand} - $CVMFS_SERVER_SUBCOMMAND_SHORT


SYNOPSIS
--------
_cvmfs_server ${subcommand}_ $CVMFS_SERVER_SUBCOMMAND_SYNOPSIS


DESCRIPTION
-----------
$CVMFS_SERVER_SUBCOMMAND_DESCRIPTION " > "$TMP_SUBCOMMAND_OUTPUT"

    if [ x"$CVMFS_SERVER_SUBCOMMAND_OPTIONS" != x"" ]; then
      echo "

OPTIONS
-------" >> "$TMP_SUBCOMMAND_OUTPUT"

      echo "$CVMFS_SERVER_SUBCOMMAND_OPTIONS" |
      while read -r option; do
        option_key="$(echo "$option" |cut -d : -f 1)"
        option_value="$(echo "$option" |cut -d : -f 2-)"
        echo "\
-$option_key
    $option_value
        " >> "$TMP_SUBCOMMAND_OUTPUT"
      done
    fi

    if [ x"$CVMFS_SERVER_SUBCOMMAND_EXAMPLES" != x"" ]; then
      echo "
EXAMPLES
--------
[verse]
____
$CVMFS_SERVER_SUBCOMMAND_EXAMPLES
____
      " >> "$TMP_SUBCOMMAND_OUTPUT"
    fi


    if [ x"$CVMFS_SERVER_SUBCOMMAND_SEE_ALSO" != x"" ]; then
      echo "
SEE ALSO
--------
$CVMFS_SERVER_SUBCOMMAND_SEE_ALSO
      " >> "$TMP_SUBCOMMAND_OUTPUT"
    fi

    echo "
CVMFS_SERVER
------------
Part of the *cvmfs_server*(8) suite" >> "$TMP_SUBCOMMAND_OUTPUT"

  done
}
