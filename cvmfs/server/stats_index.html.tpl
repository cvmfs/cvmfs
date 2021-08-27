<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <title>CernVM-FS repository statistics - $REPO_NAME</title>
  <script src="https://root.cern/js/latest/scripts/JSRoot.core.min.js" type="text/javascript"></script>
  <script type='text/javascript'>
    var filename = "stats.root";

    var plots = ["utilization_daily", "utilization_weekly",
                "condemned_objects", "condemned_objects_daily", "condemned_objects_weekly",
                "condemned_bytes", "condemned_bytes_daily", "condemned_bytes_weekly",
                "condemned_catalogs", "condemned_catalogs_daily", "condemned_catalogs_weekly",
                "gc_ops_daily", "gc_ops_weekly",
                "files", "files_daily", "files_weekly",
                "directories", "directories_daily", "directories_weekly",
                "symlinks", "symlinks_daily", "symlinks_weekly",
                "chunks_uploaded", "chunks_uploaded_daily", "chunks_uploaded_weekly",
                "volume", "volume_daily", "volume_weekly",
                "uploaded_purged_daily", "uploaded_purged_weekly",
                "publish_ops_daily", "publish_ops_weekly",
                "uploaded_speed_daily", "uploaded_speed_weekly",
                "volume_speed_daily", "volume_speed_weekly"];

    async function readAndDrawAsync() {
      let file = await JSROOT.openFile(filename);

      for (let i = 0; i < plots.length; i++) {
        let obj = await file.readObject(plots[i]);
        await JSROOT.draw(plots[i], obj);
        console.log("drawing ".concat(plots[i]));
      }
    }
    readAndDrawAsync();
  </script>
</head>
<body>
  <h1>CernVM-FS repository statistics - $REPO_NAME</h1>
  [ <a href="stats.db">Raw data (SQLite database)</a> ]
  [ <a href="stats.root">ROOT file with plots</a> ]
  <div>
    <h2>Intro</h2>
    This is a statistics monitor of CernVM-FS Stratum0 repository.<br>
    <ul>
      <li>It contains useful plots related to publication and garbage collection on this machine.</li>
      <li>Data used to compose these plots come from CernVM-FS internal statistics database available <a href="stats.db">here</a>.</li>
      <li>Plots are redrawn every time the underlying database changes (i. e. after publication or garbage collection) with a few second delay.</li>
      <li>Plots are loaded from a <a href="stats.root">ROOT file</a> using JsROOT. <a href="https://root.cern.ch/js/">JsROOT official page</a> can provide more information on usage.</li>
    </ul>
    <h2>Tips & tricks</h2>
    <ul>
      <li>Use the little blue square on the bottom-left corner of a plot to enlarge to fullscreen, capture a PNG and a lot more.</li>
      <li>Scroll with mouse on the horizontal/vertical axis to change the axis range (zoom/unzoom).</li>
      <li>To zoom in on a certain area, click & drag over this area with mouse.</li>
      <li>You can move the legends using your mouse (click & drag).</li>
    </ul>
  </div>
  <div>
    <h2>Utilization</h2>
    <table id="table_utilization">
      <tr>
        <td><div id="utilization_daily" style="width:500px; height:300px"></div></td>
        <td><div id="utilization_weekly" style="width:500px; height:300px"></div></td>
      </tr>
    </table>
  </div>
  <div>
    <h2>Publication statistics</h2>
    <table id="table_publish">
      <tr>
        <td><div id="publish_ops_daily" style="width:500px; height:300px"></div></td>
        <td><div id="publish_ops_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td><div id="files" style="width:500px; height:300px"></div></td>
        <td><div id="files_daily" style="width:500px; height:300px"></div></td>
        <td><div id="files_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td><div id="volume" style="width:500px; height:300px"></div></td>
        <td><div id="volume_daily" style="width:500px; height:300px"></div></td>
        <td><div id="volume_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td></td>
        <td><div id="uploaded_speed_daily" style="width:500px; height:300px"></div></td>
        <td><div id="uploaded_speed_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td></td>
        <td><div id="volume_speed_daily" style="width:500px; height:300px"></div></td>
        <td><div id="volume_speed_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td><div id="uploaded" style="width:500px; height:300px"></div></td>
        <td><div id="uploaded_purged_daily" style="width:500px; height:300px"></div></td>
        <td><div id="uploaded_purged_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td><div id="chunks_uploaded" style="width:500px; height:300px"></div></td>
        <td><div id="chunks_uploaded_daily" style="width:500px; height:300px"></div></td>
        <td><div id="chunks_uploaded_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td><div id="directories" style="width:500px; height:300px"></div></td>
        <td><div id="directories_daily" style="width:500px; height:300px"></div></td>
        <td><div id="directories_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td><div id="symlinks" style="width:500px; height:300px"></div></td>
        <td><div id="symlinks_daily" style="width:500px; height:300px"></div></td>
        <td><div id="symlinks_weekly" style="width:500px; height:300px"></div></td>
      </tr>
    </table>
  </div>
  <div>
    <h2>Garbage collection statistics</h2>
    <table id="table_gc">
      <tr>
        <td><div id="gc_ops_daily" style="width:500px; height:300px"></div></td>
        <td><div id="gc_ops_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td><div id="condemned_objects" style="width:500px; height:300px"></div></td>
        <td><div id="condemned_objects_daily" style="width:500px; height:300px"></div></td>
        <td><div id="condemned_objects_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td><div id="condemned_catalogs" style="width:500px; height:300px"></div></td>
        <td><div id="condemned_catalogs_daily" style="width:500px; height:300px"></div></td>
        <td><div id="condemned_catalogs_weekly" style="width:500px; height:300px"></div></td>
      </tr>
      <tr>
        <td><div id="condemned_bytes" style="width:500px; height:300px"></div></td>
        <td><div id="condemned_bytes_daily" style="width:500px; height:300px"></div></td>
        <td><div id="condemned_bytes_weekly" style="width:500px; height:300px"></div></td>
      </tr>
    </table>
  </div>
</body>
</html>
