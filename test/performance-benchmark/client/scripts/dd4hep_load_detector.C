#ifdef __CLING__
R__LOAD_LIBRARY(libDDCore)
#endif



void dd4hep_load_detector(std::string compact, std::string outPath = ""){
  gSystem->Load("libDDCore");
  using namespace dd4hep;
  Detector& description = Detector::getInstance();
  description.fromCompact(compact);

  if (!outPath.empty()) {
    gGeoManager->Export(outPath.c_str());
  }
}
