void sim(Int_t nev=1) {

  gSystem->Load("liblhapdf");
  gSystem->Load("libEGPythia6");
  gSystem->Load("libpythia6");
  gSystem->Load("libAliPythia6");
  gSystem->Load("libhijing");
  gSystem->Load("libTHijing");
  gSystem->Load("libgeant321");

  if (gSystem->Getenv("EVENT"))
   nev = atoi(gSystem->Getenv("EVENT")) ;   
  
  AliSimulation simulator;
  simulator.SetMakeSDigits("TRD TOF PHOS HMPID EMCAL FMD ZDC PMD T0 VZERO");
  simulator.SetMakeDigitsFromHits("ITS TPC");
  simulator.SetWriteRawData("ALL","raw.root",kTRUE);

  //simulator.SetDefaultStorage("local:///cvmfs/alice-ocdb.cern.ch/calibration/MC/Ideal");
  simulator.SetDefaultStorage(Form("local://%s/OCDB", gSystem->pwd()));
  simulator.SetSpecificStorage("GRP/GRP/Data",
			       Form("local://%s",gSystem->pwd()));
  
  simulator.SetRunQA("ALL:ALL") ; 
  
  simulator.SetQARefDefaultStorage("local://$ALICE_ROOT/QAref") ;

  for (Int_t det = 0 ; det < AliQA::kNDET ; det++) {
    simulator.SetQACycles((AliQAv1::DETECTORINDEX_t)det, nev+1) ;
  }
  
  TStopwatch timer;
  timer.Start();
  simulator.Run(nev);
  timer.Stop();
  timer.Print();
}
