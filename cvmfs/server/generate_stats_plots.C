/**
 * This file is part of the CernVM File System.
 */

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RSqliteDS.hxx>
#include <TCanvas.h>
#include <TMultiGraph.h>
#include <THStack.h>
#include <TLegend.h>
#include <TPaveText.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

const int max_visible_values = 50;

const std::vector<std::string> gc_cols = {
  "n_preserved_catalogs", "n_condemned_catalogs",
  "n_condemned_objects", "sz_condemned_bytes"};
const std::vector<std::string> publish_cols = {
  "files_added", "files_removed", "files_changed",
  "directories_added", "directories_removed", "directories_changed",
  "symlinks_added", "symlinks_removed", "symlinks_changed",
  "chunks_duplicated", "chunks_added",
  "sz_bytes_added", "sz_bytes_removed", "sz_bytes_uploaded",
  "catalogs_added", "sz_catalog_bytes_uploaded"};

std::string getSumsSubStmt(const std::vector<std::string> &cols) {
  std::string result = "";
  for (const std::string &col : cols) {
    result += "IFNULL(sum(" + col + "),0) AS " + col + ",";
  }
  result = result.substr(0, result.size() - 1);  // remove last comma
  return result;
}

const std::string stmt_gc =
  "SELECT * FROM "
    "(SELECT * FROM gc_statistics ORDER BY start_time DESC LIMIT 2000) "
  "ORDER BY start_time ASC;";

const std::string stmt_gc_daily =
  "SELECT substr(start_time, 0, 11) AS day,"
  "count(gc_id) AS n_ops,"
  "sum((julianday(finish_time) - julianday(start_time))) AS duration,"
  + getSumsSubStmt(gc_cols) +
  " FROM gc_statistics GROUP BY substr(start_time, 0, 11);";

const std::string stmt_gc_weekly =
  "SELECT datetime(start_time, \"weekday 0\", \"start of day\") AS date,"
  "count(gc_id) AS n_ops,"
  "sum((julianday(finish_time) - julianday(start_time))) AS duration,"
  + getSumsSubStmt(gc_cols) +
  " FROM gc_statistics"
  " GROUP BY datetime(start_time, \"weekday 0\", \"start of day\")"
  " ORDER BY datetime(start_time, \"weekday 0\", \"start of day\") ASC;";

const std::string stmt_gc_monthly =
  "SELECT substr(start_time, 0, 8) AS month,"
  "count(gc_id) AS n_ops,"
  "sum((julianday(finish_time) - julianday(start_time))) AS duration,"
  + getSumsSubStmt(gc_cols) +
  " FROM gc_statistics GROUP BY substr(start_time, 0, 8);";

const std::string stmt_publish =
  "SELECT * FROM "
    "(SELECT * FROM publish_statistics ORDER BY start_time DESC LIMIT 2000) "
  "ORDER BY start_time ASC;";

const std::string stmt_publish_daily =
  "SELECT substr(start_time, 0, 11) AS day,"
  "count(publish_id) AS n_ops,"
  "sum((julianday(finish_time) - julianday(start_time))) AS duration,"
  + getSumsSubStmt(publish_cols) +
  " FROM publish_statistics GROUP BY substr(start_time, 0, 11)"
  " ORDER BY substr(start_time, 0, 11) ASC;";

const std::string stmt_publish_weekly =
  "SELECT datetime(start_time, \"weekday 0\", \"start of day\") AS date,"
  "count(publish_id) AS n_ops,"
  "sum((julianday(finish_time) - julianday(start_time))) AS duration,"
  + getSumsSubStmt(publish_cols) +
  " FROM publish_statistics"
  " GROUP BY datetime(start_time, \"weekday 0\", \"start of day\")"
  " ORDER BY datetime(start_time, \"weekday 0\", \"start of day\") ASC;";

const std::string stmt_publish_monthly =
  "SELECT substr(start_time, 0, 8) AS month,"
  "count(publish_id) AS n_ops,"
  "sum((julianday(finish_time) - julianday(start_time))) AS duration,"
  + getSumsSubStmt(publish_cols) +
  " FROM publish_statistics GROUP BY substr(start_time, 0, 8);";

unsigned offset = TDatime(1995, 1, 1, 0, 0, 0).Convert();

// unsigned getMonthCoord(const std::string &date) {
//   return TDatime(stoi(date.substr(0, 4)),
//                  stoi(date.substr(5, 2)),
//                  0, 0, 0, 0).Convert() - offset;
// }

unsigned getDayCoord(const std::string &date) {
  return TDatime(stoi(date.substr(0, 4)),
                 stoi(date.substr(5, 2)),
                 stoi(date.substr(8, 2)),
                 0, 0, 0).Convert() - offset;
}

unsigned getWeekCoord(const std::string &date) {
  return TDatime(date.c_str()).Convert() - offset - 24*3600*6;  // to Monday
}


typedef std::unordered_map<std::string, ROOT::RDF::RNode*> RDFMap;
typedef std::unordered_map<std::string, ROOT::RDF::RResultPtr<TGraph> >
        TGraphMap;
typedef std::vector<ROOT::RDF::RResultPtr<TGraph> > TGraphList;

struct PlotInstrs {
  TGraphList gr;
  std::vector<std::string> gr_titles;
  std::vector<int> colors;
  std::vector<int> marker_styles;
  std::string name;
  std::string title;
  std::string y_axis_title;
  bool time_axis = false;
  int period;
};

TH1D *GetHistogram(TGraph *gr, int period) {
  int nvals = gr->GetN();
  int n_visible_vals = std::min(nvals, max_visible_values);
  double xmin = gr->GetX()[0];
  // shift xmax to the RIGHT side of the bin
  double xmax = gr->GetX()[nvals-1] + period;
  int nbins = (xmax - xmin) / period;
  auto h = new TH1D((std::string(gr->GetName()) + "_hist").c_str(),
                    "",
                    nbins, xmin, xmax);
  for (int i = 0; i < nvals; ++i) {
    double x, y;
    gr->GetPoint(i, x, y);
    int bin = h->FindBin(x);
    h->SetBinContent(bin, y);
  }
  return h;
}

void WriteHistogram(PlotInstrs pi) {
  TCanvas *c = new TCanvas(pi.name.c_str());
  c->SetRightMargin(.02);

  auto hists = new TList();

  int nvals = pi.gr[0]->GetN();
  double xmin = pi.gr[0]->GetX()[0];
  // shift xmax to the RIGHT side of the bin
  double xmax = pi.gr[0]->GetX()[nvals-1] + pi.period;
  int nbins = (xmax - xmin) / pi.period;
  auto helper = new TH1D("helper", "", nbins, xmin, xmax);
  helper->SetBinContent(0, 0);
  int n_visible_vals = std::min(nbins, max_visible_values);

  auto legend = new TLegend();

  for (unsigned i = 0; i < pi.gr.size(); ++i) {
    auto gr = *pi.gr[i];
    auto h = GetHistogram(&gr, pi.period);
    int nbins = h->GetNbinsX();
    for (int i = 0; i < nbins; ++i) {
      if (helper->GetBinContent(i+1) < h->GetBinContent(i+1)) {
        helper->SetBinContent(i+1, h->GetBinContent(i+1));
      }
    }

    h->SetMarkerStyle(pi.marker_styles[i]);
    h->SetMarkerSize(.7);
    h->SetMarkerColor(pi.colors[i]);
    h->SetLineWidth(2);
    h->SetLineColor(pi.colors[i]);
    h->SetTitle(pi.gr_titles[i].c_str());
    hists->Add(h);
    legend->AddEntry(h, pi.gr_titles[i].c_str());
  }

  if (pi.time_axis) {
    helper->GetXaxis()->SetTimeDisplay(1);
    helper->GetXaxis()->SetTitle("date");
  } else {
    helper->GetXaxis()->SetTitle("revision");
    helper->GetXaxis()->SetNoExponent();
  }
  helper->GetXaxis()->CenterTitle();
  helper->GetXaxis()->SetTitleSize(.05);
  helper->GetXaxis()->SetRange(nbins - n_visible_vals, nbins);

  helper->GetYaxis()->SetTitle(pi.y_axis_title.c_str());
  helper->GetYaxis()->SetTitleSize(.05);
  helper->GetYaxis()->SetTitleOffset(.8);
  helper->GetYaxis()->CenterTitle();
  helper->SetMinimum(0);

  helper->SetLabelSize(.045, "xy");
  helper->SetStats(false);
  helper->SetLineWidth(0);
  helper->SetTitle(pi.title.c_str());
  helper->Draw("L");

  for (int i = 0; i < hists->GetEntries(); ++i) {
    auto h = hists->At(i);
    h->Draw("L P0 SAME");
  }

  if (pi.y_axis_title == "bytes" || pi.y_axis_title == "B/s") {
    auto *pt = new TPaveText(.8, .12, .96, .32, "N NDC");
    pt->AddText("10^3 = kB");
    pt->AddText("10^6 = MB");
    pt->AddText("10^9 = GB");
    pt->AddText("10^12 = TB");
    pt->Draw();
  }

  if (pi.gr_titles[0] != "") {
    legend->Draw();
  }
  c->Write();
  for (int i = 0; i < hists->GetEntries(); ++i) {
    delete hists->At(i);
  }
  delete helper;
}

void WriteTHStack(PlotInstrs pi)
{
  TCanvas *c = new TCanvas(pi.name.c_str());
  c->SetRightMargin(.02);

  double xmin = pi.gr[0]->GetX()[0];
  double xmax = pi.gr[0]->GetX()[pi.gr[0]->GetN()-1] + pi.period;
  int nbins = (xmax - xmin) / pi.period;
  int n_visible_vals = std::min(nbins, max_visible_values);
  THStack *hs = new THStack("hs", pi.title.c_str());

  auto hists = new TList();

  for (unsigned i = 0; i < pi.gr.size(); ++i) {
    auto gr = *pi.gr[i];
    auto name = std::string(gr.GetName()) + std::to_string(i);  // unique name
    auto h = new TH1D(name.c_str(), pi.gr_titles[i].c_str(), nbins, xmin, xmax);
    for (int i = 0; i < gr.GetN(); ++i) {
      double x, y;
      gr.GetPoint(i, x, y);
      int bin = h->FindBin(x);
      double value = y*100*86400/pi.period;  // percents of a day/week
      h->SetBinContent(bin, value);
    }

    h->SetFillColor(pi.colors[i]);
    hs->Add(h);
    hists->Add(h);
  }
  hs->Draw();
  hs->GetHistogram()->SetBarWidth();
  hs->GetHistogram()->SetLabelSize(.045, "xy");
  hs->GetXaxis()->SetTimeDisplay(1);
  hs->GetXaxis()->SetTitle("date");
  hs->GetXaxis()->CenterTitle();
  hs->GetXaxis()->SetTitleSize(.05);
  hs->GetXaxis()->SetRange(nbins - n_visible_vals, nbins);

  hs->GetYaxis()->SetTitleSize(.05);
  hs->GetYaxis()->SetTitleOffset(.8);
  hs->GetYaxis()->CenterTitle();
  hs->GetYaxis()->SetTitle(pi.y_axis_title.c_str());
  c->BuildLegend();
  c->Write();

  for (int i = 0; i < hists->GetEntries(); ++i) {
    delete hists->At(i);
  }
}


// rdfs["gc"|"publish"]["period"]
std::unordered_map<std::string, RDFMap> rdfs;
// graphs["gc"|"publish"]["period"]["col"]
std::unordered_map<std::string,
                   std::unordered_map<std::string, TGraphMap> > graphs;

// Do not generate graphs of these column values
std::set<std::string> excluded_cols =
  {"x", "date", "day", "month", "gc_id",
  "publish_id", "start_time", "finish_time"};

std::set<std::string> speed_cols =
  {"sz_bytes_added", "sz_bytes_uploaded"};

double getSpeed(int64_t count, double duration) {
  return count/duration/86400;
}

ROOT::RDF::RNode define_custom_cols(ROOT::RDF::RNode *rdf) {
  ROOT::RDF::RNode result = *rdf;

  if (result.HasColumn("chunks_duplicated")) {
    result =
      result.Define(
        "chunks_uploaded",
        [] (Long64_t added, Long64_t duplicated) { return added - duplicated; },
        {"chunks_added", "chunks_duplicated"});

    if (result.HasColumn("duration")) {
      result =
        result.Define("chunks_uploaded_speed",
                      getSpeed, {"chunks_uploaded", "duration"})
              .Define("files_added_speed",
                      getSpeed, {"files_added", "duration"})
              .Define("sz_bytes_uploaded_speed",
                      getSpeed, {"sz_bytes_uploaded", "duration"})
              .Define("sz_bytes_added_speed",
                      getSpeed, {"sz_bytes_added", "duration"});
    }
  }

  return result;
}

TGraphMap precompute_graphs(ROOT::RDF::RNode* rdf)
{
  auto rdf2 = define_custom_cols(rdf);
  auto result = TGraphMap();
  for (auto &&col : rdf2.GetColumnNames()) {
    if (excluded_cols.find(col) != excluded_cols.end()) {
      continue;
    }

    result[col] = rdf2.Graph("x", col);
  }
  return result;
}

void generate_stats_plots(std::string stats_db_path, std::string out_path) {
  TFile f(out_path.c_str(), "RECREATE");
  // TODO(jpriessn) change "x" to revision
  rdfs["publish"]["revision"] = new ROOT::RDF::RNode(
    ROOT::RDF::MakeSqliteDataFrame(stats_db_path, stmt_publish)
              .Alias("x", "publish_id"));
  rdfs["publish"]["daily"] = new ROOT::RDF::RNode(
    ROOT::RDF::MakeSqliteDataFrame(stats_db_path, stmt_publish_daily)
              .Define("x", getDayCoord, {"day"}));
  rdfs["publish"]["weekly"] = new ROOT::RDF::RNode(
    ROOT::RDF::MakeSqliteDataFrame(stats_db_path, stmt_publish_weekly)
              .Define("x", getWeekCoord, {"date"}));
  // rdfs["publish"]["monthly"] = new ROOT::RDF::RNode(
  //   ROOT::RDF::MakeSqliteDataFrame(stats_db_path, stmt_publish_monthly)
  //             .Define("x", getMonthCoord, {"month"}));

  // TODO(jpriessn) change "x" to revision
  auto rdf_gc_rev =
    new ROOT::RDF::RNode(
    ROOT::RDF::MakeSqliteDataFrame(stats_db_path, stmt_gc)
              .Alias("x", "gc_id"));
  bool garbage_collectible;
  garbage_collectible = *rdf_gc_rev->Count();
  if (garbage_collectible) {
    rdfs["gc"]["revision"] = rdf_gc_rev;
    rdfs["gc"]["daily"] = new ROOT::RDF::RNode(
      ROOT::RDF::MakeSqliteDataFrame(stats_db_path, stmt_gc_daily)
                .Define("x", getDayCoord, {"day"}));
    rdfs["gc"]["weekly"] = new ROOT::RDF::RNode(
      ROOT::RDF::MakeSqliteDataFrame(stats_db_path, stmt_gc_weekly)
                .Define("x", getWeekCoord, {"date"}));
    // rdfs["gc"]["monthly"] = new ROOT::RDF::RNode(
    //   ROOT::RDF::MakeSqliteDataFrame(stats_db_path, stmt_gc_monthly)
    //             .Define("x", getMonthCoord, {"month"}));
  }

  for (auto const& op : rdfs) {
    for (auto const& period : op.second) {
      graphs[op.first][period.first] = precompute_graphs(period.second);
    }
  }

  for (const std::string &period : {"revision", "daily", "weekly"}) {
    PlotInstrs pi;
    std::string name_suffix;
    std::string title_suffix;
    if (period != "revision") {
      name_suffix = "_" + period;
      title_suffix = " (" + period + ")";
      pi.time_axis = true;
      if (period == "daily") {
        pi.period = 3600*24;
      } else {
        pi.period = 3600*24*7;
      }
    } else {
      name_suffix = "";
      title_suffix = "";
      pi.time_axis = false;
      pi.period = 1;
    }
    pi.marker_styles = {20, 21, 22};

    auto p_grs = graphs["publish"][period];
    auto gc_grs = graphs["gc"][period];

    pi.gr = {p_grs["files_added"], p_grs["files_removed"],
             p_grs["files_changed"]};
    pi.gr_titles = {"added", "removed", "changed"};
    pi.colors = {4, 2, 799};
    pi.name = "files" + name_suffix;
    pi.title = "Files" + title_suffix;
    pi.y_axis_title = "files";
    WriteHistogram(pi);

    pi.gr = {p_grs["directories_added"], p_grs["directories_removed"],
             p_grs["directories_changed"]};
    pi.gr_titles = {"added", "removed", "changed"};
    pi.colors = {4, 2, 799};
    pi.name = "directories" + name_suffix;
    pi.title = "Directories" + title_suffix;
    pi.y_axis_title = "directories";
    WriteHistogram(pi);

    pi.gr = {p_grs["symlinks_added"], p_grs["symlinks_removed"],
             p_grs["symlinks_changed"]};
    pi.gr_titles = {"added", "removed", "changed"};
    pi.colors = {4, 2, 799};
    pi.name = "symlinks" + name_suffix;
    pi.title = "Symlinks" + title_suffix;
    pi.y_axis_title = "symlinks";
    WriteHistogram(pi);

    pi.gr = {p_grs["sz_bytes_added"], p_grs["sz_bytes_removed"],
             p_grs["sz_bytes_uploaded"]};
    pi.gr_titles = {"added", "removed", "uploaded"};
    pi.colors = {4, 2, 799};
    pi.name = "volume" + name_suffix;
    pi.title = "Bytes added/removed/uploaded" + title_suffix;
    pi.y_axis_title = "bytes";
    WriteHistogram(pi);

    if (garbage_collectible) {
      pi.gr = {gc_grs["n_condemned_objects"]};
      pi.gr_titles = {""};
      pi.colors = {2};
      pi.name = "condemned_objects" + name_suffix;
      pi.title = "Purged objects" + title_suffix;
      pi.y_axis_title = "";
      WriteHistogram(pi);

      pi.gr = {gc_grs["sz_condemned_bytes"]};
      pi.gr_titles = {""};
      pi.colors = {2};
      pi.name = "condemned_bytes" + name_suffix;
      pi.title = "Bytes purged" + title_suffix;
      pi.y_axis_title = "bytes";
      WriteHistogram(pi);

      pi.gr = {gc_grs["n_condemned_catalogs"]};
      pi.gr_titles = {""};
      pi.colors = {2};
      pi.name = "condemned_catalogs" + name_suffix;
      pi.title = "Purged catalogs" + title_suffix;
      pi.y_axis_title = "";
      WriteHistogram(pi);

      pi.gr = {gc_grs["n_preserved_catalogs"]};
      pi.gr_titles = {""};
      pi.colors = {2};
      pi.name = "preserved_catalogs" + name_suffix;
      pi.title = "Preserved catalogs" + title_suffix;
      pi.y_axis_title = "";
      WriteHistogram(pi);
    }

    // Aggregate graphs only (exclude graph by revision) follow
    if (period == "revision") {
      // TODO(jpriessn) review methodology for this plot
      pi.gr = {p_grs["chunks_added"], p_grs["chunks_uploaded"]};
      pi.gr_titles = {"added", "uploaded"};
      pi.colors = {4, 799};
      pi.name = "chunks_uploaded";
      pi.title = "Objects/chunks";
      pi.y_axis_title = "objects";
      WriteHistogram(pi);
      continue;
    }

    pi.gr = {p_grs["n_ops"]};
    pi.gr_titles = {""};
    pi.colors = {4};
    pi.name = "publish_ops" + name_suffix;
    pi.title = "Number of publishes" + title_suffix;
    pi.y_axis_title = "";
    WriteHistogram(pi);

    pi.gr = {p_grs["sz_bytes_added_speed"], p_grs["sz_bytes_uploaded_speed"]};
    pi.gr_titles = {"processed", "uploaded"};
    pi.colors = {2, 799};
    pi.name = "volume_speed" + name_suffix;
    pi.title = "Publication throughput" + title_suffix;
    pi.y_axis_title = "B/s";
    WriteHistogram(pi);

    pi.gr = {p_grs["files_added_speed"], p_grs["chunks_uploaded_speed"]};
    pi.gr_titles = {"processed", "uploaded"};
    pi.colors = {2, 799};
    pi.name = "uploaded_speed" + name_suffix;
    pi.title = "Publication speed" + title_suffix;
    pi.y_axis_title = "files/s";
    WriteHistogram(pi);

    if (garbage_collectible) {
      pi.gr = {gc_grs["n_ops"]};
      pi.gr_titles = {""};
      pi.colors = {4};
      pi.name = "gc_ops" + name_suffix;
      pi.title = "Number of garbage collections" + title_suffix;
      pi.y_axis_title = "";
      WriteHistogram(pi);

      pi.gr = {p_grs["chunks_added"], p_grs["chunks_uploaded"],
               gc_grs["n_condemned_objects"]};
      pi.gr_titles = {"added", "uploaded", "purged"};
      pi.colors = {4, 799, 2};
      pi.name = "chunks_uploaded" + name_suffix;
      pi.title = "Objects/chunks" + title_suffix;
      pi.y_axis_title = "objects";
      WriteHistogram(pi);

      pi.gr = {p_grs["duration"], gc_grs["duration"]};
      pi.gr_titles = {"publish", "garbage collection"};
      pi.colors = {4, 2};
      pi.name = "utilization" + name_suffix;
      pi.title = "Utilization" + title_suffix;
      pi.y_axis_title = "% of the time";
      WriteTHStack(pi);

      pi.gr = {p_grs["sz_bytes_uploaded"], gc_grs["sz_condemned_bytes"]};
      pi.gr_titles = {"uploaded", "purged"};
      pi.colors = {4, 2};
      pi.name = "uploaded_purged" + name_suffix;
      pi.title = "Uploaded/purged bytes" + title_suffix;
      pi.y_axis_title = "bytes";
      WriteHistogram(pi);
    } else {
      pi.gr = {p_grs["chunks_added"], p_grs["chunks_uploaded"]};
      pi.gr_titles = {"added", "uploaded"};
      pi.colors = {4, 799};
      pi.name = "chunks_uploaded" + name_suffix;
      pi.title = "Objects/chunks" + title_suffix;
      pi.y_axis_title = "objects";
      WriteHistogram(pi);

      pi.gr = {p_grs["duration"]};
      pi.gr_titles = {"publish"};
      pi.colors = {4};
      pi.name = "utilization" + name_suffix;
      pi.title = "Utilization" + title_suffix;
      pi.y_axis_title = "% of the time";
      WriteTHStack(pi);

      pi.gr = {p_grs["sz_bytes_uploaded"]};
      pi.gr_titles = {"uploaded"};
      pi.colors = {4, 2};
      pi.name = "uploaded_purged" + name_suffix;
      pi.title = "Uploaded bytes" + title_suffix;
      pi.y_axis_title = "bytes";
      WriteHistogram(pi);
    }
  }
}
