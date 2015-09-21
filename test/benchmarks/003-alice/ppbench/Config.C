//Configuration of simulation

enum PprRun_t 
{
    test50,
    kParam_8000,   kParam_4000,  kParam_2000, 
    kHijing_cent1, kHijing_cent2, 
    kHijing_per1,  kHijing_per2, kHijing_per3, kHijing_per4,  kHijing_per5,
    kHijing_jj25,  kHijing_jj50, kHijing_jj75, kHijing_jj100, kHijing_jj200, 
    kHijing_gj25,  kHijing_gj50, kHijing_gj75, kHijing_gj100, kHijing_gj200,
    kHijing_pA, kPythia6, 
    kPythia6Jets20_24,   kPythia6Jets24_29,   kPythia6Jets29_35,
    kPythia6Jets35_42,   kPythia6Jets42_50,   kPythia6Jets50_60,
    kPythia6Jets60_72,   kPythia6Jets72_86,   kPythia6Jets86_104,
    kPythia6Jets104_125, kPythia6Jets125_150, kPythia6Jets150_180,
    kD0PbPb5500, kCharmSemiElPbPb5500, kBeautySemiElPbPb5500,
    kCocktailTRD, kPyJJ, kPyGJ, 
    kMuonCocktailCent1, kMuonCocktailPer1, kMuonCocktailPer4, 
    kMuonCocktailCent1HighPt, kMuonCocktailPer1HighPt, kMuonCocktailPer4HighPt,
    kMuonCocktailCent1Single, kMuonCocktailPer1Single, kMuonCocktailPer4Single,
    kFlow_2_2000, kFlow_10_2000, kFlow_6_2000, kFlow_6_5000,
    kHIJINGplus, kRunMax
};

const char* pprRunName[] = {
    "test50",
    "kParam_8000",   "kParam_4000",  "kParam_2000", 
    "kHijing_cent1", "kHijing_cent2", 
    "kHijing_per1",  "kHijing_per2", "kHijing_per3", "kHijing_per4",  
    "kHijing_per5",
    "kHijing_jj25",  "kHijing_jj50", "kHijing_jj75", "kHijing_jj100", 
    "kHijing_jj200", 
    "kHijing_gj25",  "kHijing_gj50", "kHijing_gj75", "kHijing_gj100", 
    "kHijing_gj200", "kHijing_pA", "kPythia6", 
    "kPythia6Jets20_24",   "kPythia6Jets24_29",   "kPythia6Jets29_35",
    "kPythia6Jets35_42",   "kPythia6Jets42_50",   "kPythia6Jets50_60",
    "kPythia6Jets60_72",   "kPythia6Jets72_86",   "kPythia6Jets86_104",
    "kPythia6Jets104_125", "kPythia6Jets125_150", "kPythia6Jets150_180",
    "kD0PbPb5500", "kCharmSemiElPbPb5500", "kBeautySemiElPbPb5500",
    "kCocktailTRD", "kPyJJ", "kPyGJ", 
    "kMuonCocktailCent1", "kMuonCocktailPer1", "kMuonCocktailPer4",  
    "kMuonCocktailCent1HighPt", "kMuonCocktailPer1HighPt", "kMuonCocktailPer4HighPt",
    "kMuonCocktailCent1Single", "kMuonCocktailPer1Single", "kMuonCocktailPer4Single",
    "kFlow_2_2000", "kFlow_10_2000", "kFlow_6_2000", "kFlow_6_5000", "kHIJINGplus"
};

enum PprRad_t
{
    kGluonRadiation, kNoGluonRadiation
};

enum PprTrigConf_t
{
    kDefaultPPTrig, kDefaultPbPbTrig
};

const char * pprTrigConfName[] = {
    "p-p","Pb-Pb"
};

// This part for configuration    

static PprRun_t srun = kPythia6;
static PprRad_t srad = kGluonRadiation;
static AliMagF::BMap_t smag = AliMagF::k5kG;
static Int_t    sseed = 12345; //Set 0 to use the current time
static PprTrigConf_t strig = kDefaultPPTrig; // default pp trigger configuration

// Comment line 
static TString  comment;

// Functions
Float_t EtaToTheta(Float_t arg);
AliGenerator* GeneratorFactory(PprRun_t srun);
AliGenHijing* HijingStandard();
AliGenGeVSim* GeVSimStandard(Float_t, Float_t);
void ProcessEnvironmentVars();

void Config()
{
    // ThetaRange is (0., 180.). It was (0.28,179.72) 7/12/00 09:00
    // Theta range given through pseudorapidity limits 22/6/2001

    // Get settings from environment variables
    ProcessEnvironmentVars();

    // Set Random Number seed
    gRandom->SetSeed(sseed);
    cout<<"Seed for random number generation= "<<gRandom->GetSeed()<<endl; 


   // libraries required by geant321 and Pythia: loaded in sim.C

    new     TGeant3TGeo("C++ Interface to Geant3");

  // Output every 100 tracks

    TVirtualMC * vmc = TVirtualMC::GetMC();

  ((TGeant3*)vmc)->SetSWIT(4,100);

    AliRunLoader* rl=0x0;

    AliLog::Message(AliLog::kInfo, "Creating Run Loader", "", "", "Config()"," ConfigPPR.C", __LINE__);

    rl = AliRunLoader::Open("galice.root",
			    AliConfig::GetDefaultEventFolderName(),
			    "recreate");
    if (rl == 0x0)
      {
	gAlice->Fatal("Config.C","Can not instatiate the Run Loader");
	return;
      }
    rl->SetCompressionLevel(2);
    rl->SetNumberOfEventsPerFile(100);
    gAlice->SetRunLoader(rl);

    // Set the trigger configuration
    AliSimulation::Instance()->SetTriggerConfig(pprTrigConfName[strig]);
    cout<<"Trigger configuration is set to  "<<pprTrigConfName[strig]<<endl;

    //
    // Set External decayer
    AliDecayer *decayer = new AliDecayerPythia();


    switch (srun) {
    case kD0PbPb5500:
      decayer->SetForceDecay(kHadronicD);
      break;
    case kCharmSemiElPbPb5500:
      decayer->SetForceDecay(kSemiElectronic);
      break;
    case kBeautySemiElPbPb5500:
      decayer->SetForceDecay(kSemiElectronic);
      break;
    default:
      decayer->SetForceDecay(kAll);
      break;
    }
    decayer->Init();
    vmc->SetExternalDecayer(decayer);
    //
    //
    //=======================================================================
    //
    //=======================================================================
    // ************* STEERING parameters FOR ALICE SIMULATION **************
    // --- Specify event type to be tracked through the ALICE setup
    // --- All positions are in cm, angles in degrees, and P and E in GeV

    vmc->SetProcess("DCAY",1);
    vmc->SetProcess("PAIR",1);
    vmc->SetProcess("COMP",1);
    vmc->SetProcess("PHOT",1);
    vmc->SetProcess("PFIS",0);
    vmc->SetProcess("DRAY",0);
    vmc->SetProcess("ANNI",1);
    vmc->SetProcess("BREM",1);
    vmc->SetProcess("MUNU",1);
    vmc->SetProcess("CKOV",1);
    vmc->SetProcess("HADR",1);
    vmc->SetProcess("LOSS",2);
    vmc->SetProcess("MULS",1);
    vmc->SetProcess("RAYL",1);

    Float_t cut = 1.e-3;        // 1MeV cut by default
    Float_t tofmax = 1.e10;

    vmc->SetCut("CUTGAM", cut);
    vmc->SetCut("CUTELE", cut);
    vmc->SetCut("CUTNEU", cut);
    vmc->SetCut("CUTHAD", cut);
    vmc->SetCut("CUTMUO", cut);
    vmc->SetCut("BCUTE",  cut); 
    vmc->SetCut("BCUTM",  cut); 
    vmc->SetCut("DCUTE",  cut); 
    vmc->SetCut("DCUTM",  cut); 
    vmc->SetCut("PPCUTM", cut);
    vmc->SetCut("TOFMAX", tofmax); 

    // Generator Configuration
    AliGenerator* gener = GeneratorFactory(srun);
    gener->SetOrigin(0, 0, 0);    // vertex position
    gener->SetSigma(0, 0, 5.3);   // Sigma in (X,Y,Z) (cm) on IP position
    gener->SetCutVertexZ(1.);     // Truncate at 1 sigma
    gener->SetVertexSmear(kPerEvent); 
    gener->SetTrackingFlag(1);
    gener->Init();
    
    if (smag == AliMagF::k2kG) {
	comment = comment.Append(" | L3 field 0.2 T");
    } else if (smag == AliMagF::k5kG) {
	comment = comment.Append(" | L3 field 0.5 T");
    }
    
    
    if (srad == kGluonRadiation)
    {
	comment = comment.Append(" | Gluon Radiation On");
	
    } else {
	comment = comment.Append(" | Gluon Radiation Off");
    }

    printf("\n \n Comment: %s \n \n", comment.Data());
    
    
// Field
    TGeoGlobalMagField::Instance()->SetField(new AliMagF("Maps","Maps", -1., -1., smag));

    rl->CdGAFile();
//
    Int_t   iABSO   = 1;
    Int_t   iDIPO   = 1;
    Int_t   iFMD    = 1;
    Int_t   iFRAME  = 1;
    Int_t   iHALL   = 1;
    Int_t   iITS    = 1;
    Int_t   iMAG    = 1;
    Int_t   iMUON   = 0;
    Int_t   iPHOS   = 1;
    Int_t   iPIPE   = 1;
    Int_t   iPMD    = 1;
    Int_t   iHMPID  = 1;
    Int_t   iSHIL   = 1;
    Int_t   iT0     = 1;
    Int_t   iTOF    = 1;
    Int_t   iTPC    = 1;
    Int_t   iTRD    = 0;
    Int_t   iZDC    = 0;
    Int_t   iEMCAL  = 1;
    Int_t   iVZERO  = 0;
    Int_t   iACORDE = 1;
    Int_t   iAD = 0;

    //=================== Alice BODY parameters =============================
    AliBODY *BODY = new AliBODY("BODY", "Alice envelop");


    if (iMAG)
    {
        //=================== MAG parameters ============================
        // --- Start with Magnet since detector layouts may be depending ---
        // --- on the selected Magnet dimensions ---
        AliMAG *MAG = new AliMAG("MAG", "Magnet");
    }


    if (iABSO)
    {
        //=================== ABSO parameters ============================
        AliABSO *ABSO = new AliABSOv3("ABSO", "Muon Absorber");
    }

    if (iDIPO)
    {
        //=================== DIPO parameters ============================

        AliDIPO *DIPO = new AliDIPOv3("DIPO", "Dipole version 3");
    }

    if (iHALL)
    {
        //=================== HALL parameters ============================

        AliHALL *HALL = new AliHALLv3("HALL", "Alice Hall");
    }


    if (iFRAME)
    {
        //=================== FRAME parameters ============================

        AliFRAMEv2 *FRAME = new AliFRAMEv2("FRAME", "Space Frame");
	FRAME->SetHoles(1);
    }

    if (iSHIL)
    {
        //=================== SHIL parameters ============================

        AliSHIL *SHIL = new AliSHILv3("SHIL", "Shielding Version 3");
    }


    if (iPIPE)
    {
        //=================== PIPE parameters ============================

        AliPIPE *PIPE = new AliPIPEv3("PIPE", "Beam Pipe");
    }
 
    if (iITS)
    {
        //=================== ITS parameters ============================

	AliITS *ITS  = new AliITSv11("ITS","ITS v11");
    }

    if (iTPC)
    {
      //============================ TPC parameters =====================
        AliTPC *TPC = new AliTPCv2("TPC", "Default");
    }


    if (iTOF) {
        //=================== TOF parameters ============================
	AliTOF *TOF = new AliTOFv6T0("TOF", "normal TOF");
    }


    if (iHMPID)
    {
        //=================== HMPID parameters ===========================
        AliHMPID *HMPID = new AliHMPIDv3("HMPID", "normal HMPID");

    }


    if (iZDC)
    {
        //=================== ZDC parameters ============================

        AliZDC *ZDC = new AliZDCv4("ZDC", "normal ZDC");
    }

    if (iTRD)
    {
        //=================== TRD parameters ============================

        AliTRD *TRD = new AliTRDv1("TRD", "TRD slow simulator");
    }

    if (iFMD)
    {
        //=================== FMD parameters ============================
	AliFMD *FMD = new AliFMDv1("FMD", "normal FMD");
   }

    if (iMUON)
    {
        //=================== MUON parameters ===========================
        // New MUONv1 version (geometry defined via builders)
        AliMUON *MUON = new AliMUONv1("MUON", "default");
    }
    //=================== PHOS parameters ===========================

    if (iPHOS)
    {
        AliPHOS *PHOS = new AliPHOSv1("PHOS", "IHEP");
    }


    if (iPMD)
    {
        //=================== PMD parameters ============================
        AliPMD *PMD = new AliPMDv1("PMD", "normal PMD");
    }

    if (iT0)
    {
        //=================== T0 parameters ============================
        AliT0 *T0 = new AliT0v1("T0", "T0 Detector");
    }

    if (iEMCAL)
    {
        //=================== EMCAL parameters ============================
        AliEMCAL *EMCAL = new AliEMCALv2("EMCAL", "EMCAL_COMPLETEV1");
    }

     if (iACORDE)
    {
        //=================== ACORDE parameters ============================
        AliACORDE *ACORDE = new AliACORDEv1("ACORDE", "normal ACORDE");
    }

     if (iVZERO)
    {
        //=================== VZERO parameters ============================
        AliVZERO *VZERO = new AliVZEROv7("VZERO", "normal VZERO");
    }
 
      if (iAD)
    {
        //=================== AD parameters ============================
        AliAD *AD = new AliADv1("AD", "normal AD test");
	AD->SetADAToInstalled(kTRUE);
	AD->SetADCToInstalled(kTRUE);
    }
 
        
}

Float_t EtaToTheta(Float_t arg){
  return (180./TMath::Pi())*2.*atan(exp(-arg));
}



AliGenerator* GeneratorFactory(PprRun_t srun) {
    Int_t isw = 3;
    if (srad == kNoGluonRadiation) isw = 0;
    

    AliGenerator * gGener = 0x0;
    switch (srun) {
    case test50:
      {
	comment = comment.Append(":HIJINGparam test 50 particles");
	AliGenHIJINGpara *gener = new AliGenHIJINGpara(50);
	gener->SetMomentumRange(0, 999999.);
	gener->SetPhiRange(0., 360.);
	// Set pseudorapidity range from -8 to 8.
	Float_t thmin = EtaToTheta(8);   // theta min. <---> eta max
	Float_t thmax = EtaToTheta(-8);  // theta max. <---> eta min 
	gener->SetThetaRange(thmin,thmax);
	gGener=gener;
      }
      break;
    case kParam_8000:
      {
	comment = comment.Append(":HIJINGparam N=8000");
	AliGenHIJINGpara *gener = new AliGenHIJINGpara(86030);
	gener->SetMomentumRange(0, 999999.);
	gener->SetPhiRange(0., 360.);
	// Set pseudorapidity range from -8 to 8.
	Float_t thmin = EtaToTheta(8);   // theta min. <---> eta max
	Float_t thmax = EtaToTheta(-8);  // theta max. <---> eta min 
	gener->SetThetaRange(thmin,thmax);
	gGener=gener;
      }
      break;
    case kParam_4000:
      {
	comment = comment.Append("HIJINGparam N=4000");
	AliGenHIJINGpara *gener = new AliGenHIJINGpara(43015);
	gener->SetMomentumRange(0, 999999.);
	gener->SetPhiRange(0., 360.);
	// Set pseudorapidity range from -8 to 8.
	Float_t thmin = EtaToTheta(8);   // theta min. <---> eta max
	Float_t thmax = EtaToTheta(-8);  // theta max. <---> eta min 
	gener->SetThetaRange(thmin,thmax);
	gGener=gener;
      }
	break;
    case kParam_2000:
      {
	comment = comment.Append("HIJINGparam N=2000");
	AliGenHIJINGpara *gener = new AliGenHIJINGpara(21507);
	gener->SetMomentumRange(0, 999999.);
	gener->SetPhiRange(0., 360.);
	// Set pseudorapidity range from -8 to 8.
	Float_t thmin = EtaToTheta(8);   // theta min. <---> eta max
	Float_t thmax = EtaToTheta(-8);  // theta max. <---> eta min 
	gener->SetThetaRange(thmin,thmax);
	gGener=gener;
      }
      break;
//
//  Hijing Central
//
    case kHijing_cent1:
      {
	comment = comment.Append("HIJING cent1");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	gGener=gener;
      }
      break;
    case kHijing_cent2:
      {
	comment = comment.Append("HIJING cent2");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 2.);
	gGener=gener;
      }
      break;
//
// Hijing Peripheral 
//
    case kHijing_per1:
      {
	comment = comment.Append("HIJING per1");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(5., 8.6);
	gGener=gener;
      }
      break;
    case kHijing_per2:
      {
	comment = comment.Append("HIJING per2");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(8.6, 11.2);
	gGener=gener;
      }
      break;
    case kHijing_per3:
      {
	comment = comment.Append("HIJING per3");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(11.2, 13.2);
	gGener=gener;
      }
      break;
    case kHijing_per4:
      {
	comment = comment.Append("HIJING per4");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(13.2, 15.);
	gGener=gener;
      }
      break;
    case kHijing_per5:
      {
	comment = comment.Append("HIJING per5");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(15., 100.);
	gGener=gener;
      }
      break;
//
//  Jet-Jet
//
    case kHijing_jj25:
      {
	comment = comment.Append("HIJING Jet 25 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(1);
	gener->SetPtJet(25.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.3,0.3);
	gener->SetJetPhiRange(75., 165.);   
	gGener=gener;
      }
      break;

    case kHijing_jj50:
      {
	comment = comment.Append("HIJING Jet 50 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(1);
	gener->SetPtJet(50.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.3,0.3);
	gener->SetJetPhiRange(75., 165.);   
	gGener=gener;
      }
	break;

    case kHijing_jj75:
      {
	comment = comment.Append("HIJING Jet 75 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(1);
	gener->SetPtJet(75.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.3,0.3);
	gener->SetJetPhiRange(75., 165.);   
	gGener=gener;
      }
      break;

    case kHijing_jj100:
      {
	comment = comment.Append("HIJING Jet 100 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(1);
	gener->SetPtJet(100.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.3,0.3);
	gener->SetJetPhiRange(75., 165.);   
	gGener=gener;
      }
      break;

    case kHijing_jj200:
      {
	comment = comment.Append("HIJING Jet 200 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(1);
	gener->SetPtJet(200.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.3,0.3);
	gener->SetJetPhiRange(75., 165.);   
	gGener=gener;
      }
      break;
//
// Gamma-Jet
//
    case kHijing_gj25:
      {
	comment = comment.Append("HIJING Gamma 25 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(2);
	gener->SetPtJet(25.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.12, 0.12);
        gener->SetJetPhiRange(220., 320.);
	gGener=gener;
      }
      break;

    case kHijing_gj50:
      {
	comment = comment.Append("HIJING Gamma 50 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(2);
	gener->SetPtJet(50.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.12, 0.12);
        gener->SetJetPhiRange(220., 320.);
	gGener=gener;
      }
      break;

    case kHijing_gj75:
      {
	comment = comment.Append("HIJING Gamma 75 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(2);
	gener->SetPtJet(75.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.12, 0.12);
        gener->SetJetPhiRange(220., 320.);
	gGener=gener;
      }
      break;

    case kHijing_gj100:
      {
	comment = comment.Append("HIJING Gamma 100 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(2);
	gener->SetPtJet(100.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.12, 0.12);
        gener->SetJetPhiRange(220., 320.);
	gGener=gener;
      }
      break;

    case kHijing_gj200:
      {
	comment = comment.Append("HIJING Gamma 200 GeV");
	AliGenHijing *gener = HijingStandard();
// impact parameter range
	gener->SetImpactParameterRange(0., 5.);
	// trigger
	gener->SetTrigger(2);
	gener->SetPtJet(200.);
	gener->SetRadiation(isw);
	gener->SetSimpleJets(!isw);
	gener->SetJetEtaRange(-0.12, 0.12);
        gener->SetJetPhiRange(220., 320.);
	gGener=gener;
      }
      break;
    case kHijing_pA:
      {
	comment = comment.Append("HIJING pA");

	AliGenCocktail *gener  = new AliGenCocktail();

	AliGenHijing   *hijing = new AliGenHijing(-1);
// centre of mass energy 
	hijing->SetEnergyCMS(TMath::Sqrt(82./208.) * 14000.);
// impact parameter range
	hijing->SetImpactParameterRange(0., 15.);
// reference frame
	hijing->SetReferenceFrame("CMS");
	hijing->SetBoostLHC(1);
// projectile
	hijing->SetProjectile("P", 1, 1);
	hijing->SetTarget    ("A", 208, 82);
// tell hijing to keep the full parent child chain
	hijing->KeepFullEvent();
// enable jet quenching
	hijing->SetJetQuenching(0);
// enable shadowing
	hijing->SetShadowing(1);
// Don't track spectators
	hijing->SetSpectators(0);
// kinematic selection
	hijing->SetSelectAll(0);
//
	AliGenSlowNucleons*  gray    = new AliGenSlowNucleons(1);
	AliSlowNucleonModel* model   = new AliSlowNucleonModelExp();
	gray->SetSlowNucleonModel(model);
	gray->SetDebug(1);
	gener->AddGenerator(hijing,"Hijing pPb", 1);
	gener->AddGenerator(gray,  "Gray Particles",1);
	gGener=gener;
      }
      break;
      case kPythia6:
      {
        comment = comment.Append(":Pythia p-p @ 14 TeV");
        AliGenPythia *gener = new AliGenPythia(-1); 
        gener->SetMomentumRange(0,999999);
        gener->SetThetaRange(0., 180.);
        gener->SetYRange(-12,12);
        gener->SetPtRange(0,1000);
        gener->SetProcess(kPyMb);
        gener->SetEnergyCMS(14000.);
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
        break;
      case kPythia6Jets20_24:
      {
        comment = comment.Append(":Pythia jets 20-24 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(20., 24.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
        break;
      case kPythia6Jets24_29:
      {
        comment = comment.Append(":Pythia jets 24-29 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(24., 29.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
        break;
      case kPythia6Jets29_35:
      {
        comment = comment.Append(":Pythia jets 29-35 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(29., 35.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
        break;
      case kPythia6Jets35_42:
      {
        comment = comment.Append(":Pythia jets 35-42 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(35., 42.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
        break;
      case kPythia6Jets42_50:
      {
        comment = comment.Append(":Pythia jets 42-50 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(42., 50.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
      break;
      case kPythia6Jets50_60:
      {
        comment = comment.Append(":Pythia jets 50-60 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(50., 60.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gGener=gener;
      }
        break;
      case kPythia6Jets60_72:
      {
        comment = comment.Append(":Pythia jets 60-72 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(60., 72.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
        break;
      case kPythia6Jets72_86:
      {
        comment = comment.Append(":Pythia jets 72-86 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(72., 86.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
      break;
      case kPythia6Jets86_104:
      {
        comment = comment.Append(":Pythia jets 86-104 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(86., 104.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
      break;
    case kPythia6Jets104_125:
      {
        comment = comment.Append(":Pythia jets 105-125 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(104., 125.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
        break;
      case kPythia6Jets125_150:
      {
        comment = comment.Append(":Pythia jets 125-150 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(125., 150.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gGener=gener;
      }
        break;
      case kPythia6Jets150_180:
      {
        comment = comment.Append(":Pythia jets 150-180 GeV @ 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);//        Centre of mass energy
        gener->SetProcess(kPyJets);//        Process type
        gener->SetJetEtaRange(-0.5, 0.5);//  Final state kinematic cuts
        gener->SetJetPhiRange(0., 360.);
        gener->SetJetEtRange(10., 1000.);
        gener->SetGluonRadiation(1,1);
        //    gener->SetPtKick(0.);
        //   Structure function
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(150., 180.);// Pt transfer of the hard scattering
        gener->SetPycellParameters(2., 274, 432, 0., 4., 5., 1.0);
        gener->SetForceDecay(kAll);//  Decay type (semielectronic, etc.)
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
      break;
      case kD0PbPb5500:
      {
        comment = comment.Append(" D0 in Pb-Pb at 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(10);
        gener->SetProcess(kPyD0PbPbMNR);
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(2.1,-1.0);
        gener->SetEnergyCMS(5500.);
        gener->SetNuclei(208,208);
        gener->SetForceDecay(kHadronicD);
        gener->SetYRange(-2,2);
        gener->SetFeedDownHigherFamily(kFALSE);
        gener->SetStackFillOpt(AliGenPythia::kParentSelection);
        gener->SetCountMode(AliGenPythia::kCountParents);
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
      break;
    case kCharmSemiElPbPb5500:
      {
        comment = comment.Append(" Charm in Pb-Pb at 5.5 TeV");
        AliGenPythia * gener = new AliGenPythia(10);
        gener->SetProcess(kPyCharmPbPbMNR);
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(2.1,-1.0);
        gener->SetEnergyCMS(5500.);
        gener->SetNuclei(208,208);
        gener->SetForceDecay(kSemiElectronic);
        gener->SetYRange(-2,2);
        gener->SetFeedDownHigherFamily(kFALSE);
        gener->SetCountMode(AliGenPythia::kCountParents);
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
      break;
      case kBeautySemiElPbPb5500:
      {
        comment = comment.Append(" Beauty in Pb-Pb at 5.5 TeV");
        AliGenPythia *gener = new AliGenPythia(10);
        gener->SetProcess(kPyBeautyPbPbMNR);
        gener->SetStrucFunc(kCTEQ4L);
        gener->SetPtHard(2.75,-1.0);
        gener->SetEnergyCMS(5500.);
        gener->SetNuclei(208,208);
        gener->SetForceDecay(kSemiElectronic);
        gener->SetYRange(-2,2);
        gener->SetFeedDownHigherFamily(kFALSE);
        gener->SetCountMode(AliGenPythia::kCountParents);
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
        break;
      case kCocktailTRD:
      {
        comment = comment.Append(" Cocktail for TRD at 5.5 TeV");
        AliGenCocktail *gener  = new AliGenCocktail();
        
        AliGenParam *phi = new AliGenParam(10,
                                           new AliGenMUONlib(),
                                           AliGenMUONlib::kPhi,
                                           "Vogt PbPb");
        
        phi->SetPtRange(0, 100);
        phi->SetYRange(-1., +1.);
        phi->SetForceDecay(kDiElectron);
        
        AliGenParam *omega = new AliGenParam(10,
                                             new AliGenMUONlib(),
                                             AliGenMUONlib::kOmega,
                                             "Vogt PbPb");
        
        omega->SetPtRange(0, 100);
        omega->SetYRange(-1., +1.);
        omega->SetForceDecay(kDiElectron);
        
        AliGenParam *jpsi = new AliGenParam(10,
                                            new AliGenMUONlib(),
                                            AliGenMUONlib::kJpsiFamily,
                                            "Vogt PbPb");
        
        jpsi->SetPtRange(0, 100);
        jpsi->SetYRange(-1., +1.);
        jpsi->SetForceDecay(kDiElectron);
        
        AliGenParam *ups = new AliGenParam(10,
                                           new AliGenMUONlib(),
                                           AliGenMUONlib::kUpsilonFamily,
                                           "Vogt PbPb");
        ups->SetPtRange(0, 100);
        ups->SetYRange(-1., +1.);
        ups->SetForceDecay(kDiElectron);
        
        AliGenParam *charm = new AliGenParam(10,
                                             new AliGenMUONlib(), 
                                             AliGenMUONlib::kCharm,
                                             "central");
        charm->SetPtRange(0, 100);
        charm->SetYRange(-1.5, +1.5);
        charm->SetForceDecay(kSemiElectronic);
        
        
        AliGenParam *beauty = new AliGenParam(10,
                                              new AliGenMUONlib(), 
                                              AliGenMUONlib::kBeauty,
                                              "central");
        beauty->SetPtRange(0, 100);
        beauty->SetYRange(-1.5, +1.5);
        beauty->SetForceDecay(kSemiElectronic);
        
        AliGenParam *beautyJ = new AliGenParam(10,
                                               new AliGenMUONlib(), 
                                               AliGenMUONlib::kBeauty,
                                               "central");
        beautyJ->SetPtRange(0, 100);
        beautyJ->SetYRange(-1.5, +1.5);
        beautyJ->SetForceDecay(kBJpsiDiElectron);
        
        gener->AddGenerator(phi,"Phi",1);
        gener->AddGenerator(omega,"Omega",1);
        gener->AddGenerator(jpsi,"J/psi",1);
        gener->AddGenerator(ups,"Upsilon",1);
        gener->AddGenerator(charm,"Charm",1);
        gener->AddGenerator(beauty,"Beauty",1);
        gener->AddGenerator(beautyJ,"J/Psi from Beauty",1);
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
      break;
    case kPyJJ:
      {
        comment = comment.Append(" Jet-jet at 5.5 TeV");
        AliGenPythia *gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);
        gener->SetProcess(kPyJets);
        Double_t ptHardMin=10.0, ptHardMax=-1.0;
        gener->SetPtHard(ptHardMin,ptHardMax);
        gener->SetYHard(-0.7,0.7);
        gener->SetJetEtaRange(-0.2,0.2);
        gener->SetEventListRange(0,1);
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
        break;
      case kPyGJ:
      {
        comment = comment.Append(" Gamma-jet at 5.5 TeV");
        AliGenPythia *gener = new AliGenPythia(-1);
        gener->SetEnergyCMS(5500.);
        gener->SetProcess(kPyDirectGamma);
        Double_t ptHardMin=10.0, ptHardMax=-1.0;
        gener->SetPtHard(ptHardMin,ptHardMax);
        gener->SetYHard(-1.0,1.0);
        gener->SetGammaEtaRange(-0.13,0.13);
        gener->SetGammaPhiRange(210.,330.);
        gener->SetEventListRange(0,1);
        gener->SetProjectile("p", 1, 1) ; 
        gener->SetTarget("p", 1, 1) ; 
        gGener=gener;
      }
        break;
      case kMuonCocktailCent1:
      {
        comment = comment.Append(" Muon Cocktail Cent1");
        AliGenMUONCocktail * gener = new AliGenMUONCocktail();
        gener->SetPtRange(0.4,100.);       // Transverse momentum range   
        gener->SetPhiRange(0.,360.);    // Azimuthal angle range  
        gener->SetYRange(-4.0,-2.4);
        gener->SetMuonPtCut(0.8);
        gener->SetMuonThetaCut(171.,178.);
        gener->SetMuonMultiplicity(2);
        gener->SetImpactParameterRange(0.,5.);  //Centrality class Cent1 for PDC04
        gGener=gener;
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
      }
        break;
      case kMuonCocktailPer1:
      {
        comment = comment.Append(" Muon Cocktail Per1");
        AliGenMUONCocktail * gener = new AliGenMUONCocktail();
        gener->SetPtRange(0.0,100.);       // Transverse momentum range   
        gener->SetPhiRange(0.,360.);    // Azimuthal angle range  
        gener->SetYRange(-4.0,-2.4);
        gener->SetMuonPtCut(0.8);
        gener->SetMuonThetaCut(171.,178.);
        gener->SetMuonMultiplicity(2);
        gener->SetImpactParameterRange(5.,8.6);//Centrality class Per1 for PDC04
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
      break;
    case kMuonCocktailPer4:
      {
        comment = comment.Append(" Muon Cocktail Per4");
        AliGenMUONCocktail * gener = new AliGenMUONCocktail();
        gener->SetPtRange(0.0,100.);       // Transverse momentum range   
        gener->SetPhiRange(0.,360.);    // Azimuthal angle range  
        gener->SetYRange(-4.0,-2.4);
        gener->SetMuonPtCut(0.8);
        gener->SetMuonThetaCut(171.,178.);
        gener->SetMuonMultiplicity(2);
        gener->SetImpactParameterRange(13.2,15.0);//Centrality class Per4 for PDC04
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
        break;
      case kMuonCocktailCent1HighPt:
      {
        comment = comment.Append(" Muon Cocktail HighPt Cent1");
        AliGenMUONCocktail * gener = new AliGenMUONCocktail();
        gener->SetPtRange(0.0,100.);       // Transverse momentum range   
        gener->SetPhiRange(0.,360.);    // Azimuthal angle range  
        gener->SetYRange(-4.0,-2.4);
        gener->SetMuonPtCut(2.5);
        gener->SetMuonThetaCut(171.,178.);
        gener->SetMuonMultiplicity(2);
        gener->SetImpactParameterRange(0.,5.);  //Centrality class Cent1 for PDC04
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
       gGener=gener;
      }
        break;
      case kMuonCocktailPer1HighPt :
      {
        comment = comment.Append(" Muon Cocktail HighPt Per1");
        AliGenMUONCocktail * gener = new AliGenMUONCocktail();
        gener->SetPtRange(0.0,100.);       // Transverse momentum range   
        gener->SetPhiRange(0.,360.);    // Azimuthal angle range  
        gener->SetYRange(-4.0,-2.4);
        gener->SetMuonPtCut(2.5);
        gener->SetMuonThetaCut(171.,178.);
        gener->SetMuonMultiplicity(2);
        gener->SetImpactParameterRange(5.,8.6);//Centrality class Per1 for PDC04
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
        break;
      case kMuonCocktailPer4HighPt:
      {
        comment = comment.Append(" Muon Cocktail HighPt Per4");
        AliGenMUONCocktail * gener = new AliGenMUONCocktail();
        gener->SetPtRange(0.0,100.);       // Transverse momentum range   
        gener->SetPhiRange(0.,360.);    // Azimuthal angle range  
        gener->SetYRange(-4.0,-2.4);
        gener->SetMuonPtCut(2.5);
        gener->SetMuonThetaCut(171.,178.);
        gener->SetMuonMultiplicity(2);
        gener->SetImpactParameterRange(13.2,15.0);//Centrality class Per4 for PDC04
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
        break;
      case kMuonCocktailCent1Single:
      {
        comment = comment.Append(" Muon Cocktail Single Cent1");
        AliGenMUONCocktail * gener = new AliGenMUONCocktail();
        gener->SetPtRange(0.0,100.);       // Transverse momentum range   
        gener->SetPhiRange(0.,360.);    // Azimuthal angle range  
        gener->SetYRange(-4.0,-2.4);
        gener->SetMuonPtCut(0.8);
        gener->SetMuonThetaCut(171.,178.);
        gener->SetMuonMultiplicity(1);
        gener->SetImpactParameterRange(0.,5.);  //Centrality class Cent1 for PDC04
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
        break;
      case kMuonCocktailPer1Single :
      {
        comment = comment.Append(" Muon Cocktail Single Per1");
        AliGenMUONCocktail * gener = new AliGenMUONCocktail();
        gener->SetPtRange(0.0,100.);       // Transverse momentum range   
        gener->SetPhiRange(0.,360.);    // Azimuthal angle range  
        gener->SetYRange(-4.0,-2.4);
        gener->SetMuonPtCut(0.8);
        gener->SetMuonThetaCut(171.,178.);
        gener->SetMuonMultiplicity(1);
        gener->SetImpactParameterRange(5.,8.6);//Centrality class Per1 for PDC04
        gener->SetNumberOfParticipants(229.3);//Centrality class Per1 for PDC04
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
        break;
      case kMuonCocktailPer4Single:
      {
        comment = comment.Append(" Muon Cocktail Single Per4");
        AliGenMUONCocktail * gener = new AliGenMUONCocktail();
        gener->SetPtRange(0.0,100.);       // Transverse momentum range   
        gener->SetPhiRange(0.,360.);    // Azimuthal angle range  
        gener->SetYRange(-4.0,-2.4);
        gener->SetMuonPtCut(0.8);
        gener->SetMuonThetaCut(171.,178.);
        gener->SetMuonMultiplicity(1);
        gener->SetImpactParameterRange(13.2,15.0);//Centrality class Per4 for PDC04
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener=gener;
      }
        break;
      case kFlow_2_2000:
      {
        comment = comment.Append(" Flow with dN/deta  = 2000, vn = 2%");
        gGener = GeVSimStandard(2000., 2.);
      }
        break;
        
      case kFlow_10_2000:
      {
        comment = comment.Append(" Flow with dN/deta  = 2000, vn = 10%");
        gGener = GeVSimStandard(2000., 10.);
      }
        break;
        
      case kFlow_6_2000:
      {
        comment = comment.Append(" Flow with dN/deta  = 2000, vn = 6%");
        gGener = GeVSimStandard(2000., 6.);
      }
        break;
        
      case kFlow_6_5000:
      {
        comment = comment.Append(" Flow with dN/deta  = 5000, vn = 6%");
        gGener = GeVSimStandard(5000., 6.);
      }
        break;
      case kHIJINGplus:
      {
        //
        // The cocktail
        AliGenCocktail *gener  = new AliGenCocktail();
        
        //
        // Charm production by Pythia
        AliGenPythia * genpyc = new AliGenPythia(230);
        genpyc->SetProcess(kPyCharmPbPbMNR);
        genpyc->SetStrucFunc(kCTEQ4L);
        genpyc->SetPtHard(2.1,-1.0);
        genpyc->SetEnergyCMS(5500.);
        genpyc->SetNuclei(208,208);
        genpyc->SetYRange(-999,999);
        genpyc->SetForceDecay(kAll);
        genpyc->SetFeedDownHigherFamily(kFALSE);
        genpyc->SetCountMode(AliGenPythia::kCountParents);
        //
        // Beauty production by Pythia
        AliGenPythia * genpyb = new AliGenPythia(9);
        genpyb->SetProcess(kPyBeautyPbPbMNR);
        genpyb->SetStrucFunc(kCTEQ4L);
        genpyb->SetPtHard(2.75,-1.0);
        genpyb->SetEnergyCMS(5500.);
        genpyb->SetNuclei(208,208);
        genpyb->SetYRange(-999,999);
        genpyb->SetForceDecay(kAll);
        genpyb->SetFeedDownHigherFamily(kFALSE);
        genpyb->SetCountMode(AliGenPythia::kCountParents);
        //
        // Hyperons
        //
        AliGenSTRANGElib *lib = new AliGenSTRANGElib();
        Int_t particle;
        // Xi
        particle = kXiMinus;
        AliGenParam *genXi = new AliGenParam(16,lib,particle);
        genXi->SetPtRange(0., 12.);
        genXi->SetYRange(-1.1, 1.1);
        genXi->SetForceDecay(kNoDecay);	
        
        //
        // Omega
        particle = kOmegaMinus;
        AliGenParam *genOmega = new AliGenParam(10,lib,particle);
        genOmega->SetPtRange(0, 12.);
        genOmega->SetYRange(-1.1, 1.1);
        genOmega->SetForceDecay(kNoDecay);
        
        //
        // Central Hijing 
        AliGenHijing *genHi = HijingStandard();
        genHi->SwitchOffHeavyQuarks(kTRUE);
        genHi->SetImpactParameterRange(0.,5.);
        //
        // Add everything to the cocktail and shake ...
        gener->AddGenerator(genHi,    "Hijing cent1", 1);
        gener->AddGenerator(genpyc,   "Extra charm",  1);
        gener->AddGenerator(genpyb,   "Extra beauty", 1);
        gener->AddGenerator(genXi,    "Xi"          , 1);
        gener->AddGenerator(genOmega, "Omega",        1);
        gener->SetProjectile("A", 208, 82) ; 
        gener->SetTarget("A", 208, 82) ; 
        gGener = gener;
      }
        break;
      default: break;
    }
  
  return gGener;
}

AliGenHijing* HijingStandard()
{
  AliGenHijing *gener = new AliGenHijing(-1);
  // centre of mass energy 
  gener->SetEnergyCMS(5500.);
  // reference frame
  gener->SetReferenceFrame("CMS");
  // projectile
  gener->SetProjectile("A", 208, 82);
  gener->SetTarget    ("A", 208, 82);
  // tell hijing to keep the full parent child chain
  gener->KeepFullEvent();
  // enable jet quenching
  gener->SetJetQuenching(1);
  // enable shadowing
  gener->SetShadowing(1);
  // neutral pion and heavy particle decays switched off
  gener->SetDecaysOff(1);
  // Don't track spectators
  gener->SetSpectators(0);
  // kinematic selection
  gener->SetSelectAll(0);
  return gener;
}

AliGenGeVSim* GeVSimStandard(Float_t mult, Float_t vn)
{
  AliGenGeVSim* gener = new AliGenGeVSim(0);
  //
  // Mult is the number of charged particles in |eta| < 0.5
  // Vn is in (%)
  //
  // Sigma of the Gaussian dN/deta
  Float_t sigma_eta  = 2.75;
  //
  // Maximum eta
  Float_t etamax     = 7.00;
  //
  //
  // Scale from multiplicity in |eta| < 0.5 to |eta| < |etamax|	
  Float_t mm = mult * (TMath::Erf(etamax/sigma_eta/sqrt(2.)) / TMath::Erf(0.5/sigma_eta/sqrt(2.))); 
  //
  // Scale from charged to total multiplicity
  // 
  mm *= 1.587;
  //
  // Vn 
  vn /= 100.;    	 
  //
  // Define particles
  //
  //
  // 78% Pions (26% pi+, 26% pi-, 26% p0)              T = 250 MeV
  AliGeVSimParticle *pp =  new AliGeVSimParticle(kPiPlus,  1, 0.26 * mm, 0.25, sigma_eta) ;
  AliGeVSimParticle *pm =  new AliGeVSimParticle(kPiMinus, 1, 0.26 * mm, 0.25, sigma_eta) ;
  AliGeVSimParticle *p0 =  new AliGeVSimParticle(kPi0,     1, 0.26 * mm, 0.25, sigma_eta) ;
  //
  // 12% Kaons (3% K0short, 3% K0long, 3% K+, 3% K-)   T = 300 MeV
  AliGeVSimParticle *ks =  new AliGeVSimParticle(kK0Short, 1, 0.03 * mm, 0.30, sigma_eta) ;
  AliGeVSimParticle *kl =  new AliGeVSimParticle(kK0Long,  1, 0.03 * mm, 0.30, sigma_eta) ;
  AliGeVSimParticle *kp =  new AliGeVSimParticle(kKPlus,   1, 0.03 * mm, 0.30, sigma_eta) ;
  AliGeVSimParticle *km =  new AliGeVSimParticle(kKMinus,  1, 0.03 * mm, 0.30, sigma_eta) ;
  //
  // 10% Protons / Neutrons (5% Protons, 5% Neutrons)  T = 250 MeV
  AliGeVSimParticle *pr =  new AliGeVSimParticle(kProton,  1, 0.05 * mm, 0.25, sigma_eta) ;
  AliGeVSimParticle *ne =  new AliGeVSimParticle(kNeutron, 1, 0.05 * mm, 0.25, sigma_eta) ;
  //
  // Set Elliptic Flow properties 	
  
  Float_t pTsaturation = 2. ;
  
  pp->SetEllipticParam(vn,pTsaturation,0.) ;
  pm->SetEllipticParam(vn,pTsaturation,0.) ;
  p0->SetEllipticParam(vn,pTsaturation,0.) ;
  pr->SetEllipticParam(vn,pTsaturation,0.) ;
  ne->SetEllipticParam(vn,pTsaturation,0.) ;
  ks->SetEllipticParam(vn,pTsaturation,0.) ;
  kl->SetEllipticParam(vn,pTsaturation,0.) ;
  kp->SetEllipticParam(vn,pTsaturation,0.) ;
  km->SetEllipticParam(vn,pTsaturation,0.) ;
  //
  // Set Direct Flow properties	
  pp->SetDirectedParam(vn,1.0,0.) ;
  pm->SetDirectedParam(vn,1.0,0.) ;
  p0->SetDirectedParam(vn,1.0,0.) ;
  pr->SetDirectedParam(vn,1.0,0.) ;
  ne->SetDirectedParam(vn,1.0,0.) ;
  ks->SetDirectedParam(vn,1.0,0.) ;
  kl->SetDirectedParam(vn,1.0,0.) ;
  kp->SetDirectedParam(vn,1.0,0.) ;
  km->SetDirectedParam(vn,1.0,0.) ;
  //
  // Add particles to the list
  gener->AddParticleType(pp) ;
  gener->AddParticleType(pm) ;
  gener->AddParticleType(p0) ;
  gener->AddParticleType(pr) ;
  gener->AddParticleType(ne) ;
  gener->AddParticleType(ks) ;
  gener->AddParticleType(kl) ;
  gener->AddParticleType(kp) ;
  gener->AddParticleType(km) ;
  //	
  // Random Ev.Plane ----------------------------------
  TF1 *rpa = new TF1("gevsimPsiRndm","1", 0, 360);
  // --------------------------------------------------
  gener->SetPtRange(0., 9.) ; // Use a resonable range! (used for bin size in numerical integration)
  gener->SetPhiRange(0, 360);
  //
  // Set pseudorapidity range 
  Float_t thmin = EtaToTheta(+etamax);   
  Float_t thmax = EtaToTheta(-etamax);   
  gener->SetThetaRange(thmin,thmax);     
  return gener;
}



void ProcessEnvironmentVars()
{
    // Run type
    if (gSystem->Getenv("CONFIG_RUN_TYPE")) {
      for (Int_t iRun = 0; iRun < kRunMax; iRun++) {
	if (strcmp(gSystem->Getenv("CONFIG_RUN_TYPE"), pprRunName[iRun])==0) {
	  srun = (PprRun_t)iRun;
	  cout<<"Run type set to "<<pprRunName[iRun]<<endl;
	}
      }
    }

    // Random Number seed
    if (gSystem->Getenv("CONFIG_SEED")) {
      sseed = atoi(gSystem->Getenv("CONFIG_SEED"));
    }
}
