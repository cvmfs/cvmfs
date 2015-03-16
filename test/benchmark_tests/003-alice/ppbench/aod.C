void aod(){

    gSystem->Load("libANALYSIS");
    gSystem->Load("libANALYSISalice");
    gSystem->Load("libCORRFW");
    gSystem->Load("libPWGHFbase");
    gSystem->Load("libPWGmuon");
    gSystem->Load("libESDfilter");
    gSystem->Load("libTENDER");
    gSystem->Load("libPWGPP");

    gROOT->Macro("${ALICE_ROOT}/STEER/CreateAODfromESD.C(\"AliESDs.root\",\"AliAOD.root\",\"local://OCDB\",\"local://.\")");
}
