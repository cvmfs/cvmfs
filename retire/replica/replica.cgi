#!/bin/sh

echo Content-type: text/html
echo ""

echo '<head></head><html><body><pre>'

for file in /etc/cvmfs/replica.conf /etc/cvmfs/replica.local 
do 
  [ -f $file ] && . $file
done

###
dir=$CVMFS_REPLICA_CGI_DIR
cmd="/usr/bin/cvmfs_snapshot"
###

parse_query() #@ USAGE: parse_query var ...
{
    local var val
    local IFS='&'
    vars="&$*&"
    [ "$REQUEST_METHOD" = "POST" ] && read QUERY_STRING
    set -f
    for item in $QUERY_STRING; do
      var=${item%%=*}
      val=${item#*=}
      val=${val//+/ }
      case $vars in
          *"&$var&"* )
              case $val in
                  *%[0-9a-fA-F][0-9a-fA-F]*)
                       ## Next line requires bash 3.1 or later
                       printf -v val "%b" "${val//\%/\\x}."
                       ## Older bash, use: val=$( printf "%b" "${val//\%/\\x}." )
                       val=${val%.}
              esac
              eval "$var=\$val"
              ;;
      esac
    done
    set +f
}


cmdStart() {
  id=$1
  shift 1
  
  if [ -f $dir/lock ]
  then
    pid=`cat $dir/lock`
    if [ ! -d /proc/$pid ]
    then
      rm -rf $dir/lock 
    fi
  fi
  if [ ! -f $dir/lock ]
  then
    mkdir -p $dir
    touch $dir/lock
    echo "$cmd $*" > $dir/stdout.$id
    $cmd $* >> $dir/stdout.$id 2>&1  &
    echo $! > $dir/lock
    echo OK
    return
  fi
  echo ERROR 
}

cmdStop() {
  if [ -f $dir/lock ]
  then
    pid=`cat $dir/lock`
    if [ -d /proc/$pid ]
    then
      kill $pid && rm -rf $dir/lock
      if [ $? -eq 0 ]
      then
        rm -rf $dir/stdout
        echo OK
        return
      fi
    fi
  fi
  echo ERROR
}

cmdStdout() {
  id=$1
  shift 1
  
  if [ -f $dir/stdout.$id ]; then
    cat $dir/stdout.$id
  else
    echo "ERROR ID"
  fi
}

cmdStatus() {
  if [ -f $dir/lock ]
  then
    pid=`cat $dir/lock`
    if [ -d /proc/$pid ]
    then
      echo RUNNING
      return
    fi
  fi
  echo DONE
}

parse_query id args

case $PATH_INFO in
   /start)
       if [ -z $id ]; then
         echo "ERROR NO ID"
         return
       fi
       cmdStart $id $args
       ;;
   /stop)
       cmdStop
       ;;
   /stdout)
       if [ -z $id ]; then
         echo "ERROR NO ID"
         return
       fi
       cmdStdout $id $args
       ;;
   /status)
       cmdStatus
       ;;
   *)
       break
esac

echo '</pre></body></html>'
