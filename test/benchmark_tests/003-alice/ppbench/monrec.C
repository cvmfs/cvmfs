void monrec() {
  // MonaLisa monitoring
  gSystem->Load("libNet.so");
  gSystem->Load("libMonaLisa.so");

  new TMonaLisaWriter("aliendb3.cern.ch", "Reconstruction pp", gSystem->Getenv("TEST_PLATFORMID"), gSystem->Getenv("TEST_PLATFORMID"), "global");


  gROOT->LoadMacro("rec.C");
  rec();
  if (gMonitoringWriter) gMonitoringWriter->SendProcessingProgress(1,1,kTRUE);  

  // Send the size of the AliESDs.root file

  FileStat_t buf;
  gSystem->GetPathInfo("./AliESDs.root",buf);

  TList *valuelist = new TList();
  valuelist->SetOwner(kTRUE);

  TMonaLisaValue* valdouble = new TMonaLisaValue("AliESDs.root size",buf.fSize);
  valuelist->Add(valdouble);

  if (gMonitoringWriter) gMonitoringWriter->SendParameters(valuelist);
  delete valuelist;

  printf("#Test finished successfully#\n");
}
