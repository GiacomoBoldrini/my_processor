import json
from coffea.dataset_tools import rucio_utils
import uproot
import dask
import random
from dbs.apis.dbsClient import DbsApi
import sys


def build_file_nevents(files):

    samples_nevents = {}
    url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    api=DbsApi(url=url)

    for dataset in files.keys():
        query = files[dataset]["query"]
        
        filelist = api.listFiles(dataset=query, detail=1)
        retain_branches = ["logical_file_name", "is_file_valid", "event_count", "file_size"]
        samples_nevents[dataset] = [{key:item for key,item in f__.items() if key in retain_branches} for f__ in filelist]

    return samples_nevents


def get_files():
    Samples = {}

    
    # data
#    Samples["DoubleMuon_Run2018A-UL2018-v1"] = {
#        "nanoAOD": "/DoubleMuon/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
#    }
    Samples["DoubleMuon_Run2018B-UL2018-v2"] = {
        "nanoAOD": "/DoubleMuon/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
        "isData": True
    }
#     Samples["DoubleMuon_Run2018C-UL2018-v1"] = {
#         "nanoAOD": "/DoubleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
#     }
#     Samples["DoubleMuon_Run2018D-UL2018-v2"] = {
#         "nanoAOD": "/DoubleMuon/Run2018D-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
#     }

#    Samples["EGamma_Run2018A-UL2018-v1"] = {
#        "nanoAOD": "/EGamma/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
#    }
    Samples["EGamma_Run2018B-UL2018-v1"] = {
        "nanoAOD": "/EGamma/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
        "isData": True
    }
#    Samples["EGamma_Run2018C-UL2018-v1"] = {
#        "nanoAOD": "/EGamma/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
#    }
#    Samples["EGamma_Run2018D-UL2018-v1"] = {
#        "nanoAOD": "/EGamma/Run2018D-UL2018_MiniAODv2_NanoAODv9-v3/NANOAOD"
#    }
#
#    Samples["MuonEG_Run2018A-UL2018-v1"] = {
#        "nanoAOD": "/MuonEG/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
#    }
    Samples["MuonEG_Run2018B-UL2018-v1"] = {
        "nanoAOD": "/MuonEG/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
        "isData": True
    }
#    Samples["MuonEG_Run2018C-UL2018-v1"] = {
#        "nanoAOD": "/MuonEG/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
#    }
#    Samples["MuonEG_Run2018D-UL2018-v1"] = {
#        "nanoAOD": "/MuonEG/Run2018D-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
#    }
#
#    Samples["SingleMuon_Run2018A-UL2018-v2"] = {
#        "nanoAOD": "/SingleMuon/Run2018A-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
#    }
    Samples["SingleMuon_Run2018B-UL2018-v2"] = {
        "nanoAOD": "/SingleMuon/Run2018B-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD",
        "isData": True
    }
#    Samples["SingleMuon_Run2018C-UL2018-v2"] = {
#        "nanoAOD": "/SingleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
#    }
#    Samples["SingleMuon_Run2018D-UL2018-v1"] = {
#        "nanoAOD": "/SingleMuon/Run2018D-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
#    }

    # mc
    
    # DY
    Samples['DYJetsToMuMu_M-50'] = {
        'nanoAOD' : '/DYJetsToMuMu_M-50_massWgtFix_TuneCP5_13TeV-powhegMiNNLO-pythia8-photos/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM',
        "isData": False
        
    }

    Samples['DYJetsToEE_M-50'] = {
        'nanoAOD' : '/DYJetsToEE_M-50_massWgtFix_TuneCP5_13TeV-powhegMiNNLO-pythia8-photos/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM',
        "isData": False
    }
    Samples['DYJetsToTauTau_M-50_AtLeastOneEorMuDecay'] = {
        'nanoAOD' : '/DYJetsToTauTau_M-50_AtLeastOneEorMuDecay_massWgtFix_TuneCP5_13TeV-powhegMiNNLO-pythia8-photos/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM',
        "isData": False
    }

    # TTbar
    Samples['TTJets'] = {
        'nanoAOD' : '/TTJets_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        "isData": False
    }

    # s channel single top 4f 
    Samples['ST_s-channel'] = {
        'nanoAOD' :'/ST_s-channel_4f_leptonDecays_TuneCP5_13TeV-amcatnlo-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        "isData": False
    }

    # t channel single top 5f
    Samples['ST_t-channel_top_5f'] = {
        'nanoAOD' :'/ST_t-channel_top_5f_InclusiveDecays_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        "isData": False
    }
    Samples['ST_t-channel_antitop_5f'] = {
        'nanoAOD' :'/ST_t-channel_antitop_5f_InclusiveDecays_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        "isData": False
    }

    # tW no had
    Samples['ST_tW_antitop_noHad'] = {
        'nanoAOD' :'/ST_tW_antitop_5f_NoFullyHadronicDecays_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        "isData": False
    }
    Samples['ST_tW_top_noHad'] = {
        'nanoAOD' : '/ST_tW_top_5f_NoFullyHadronicDecays_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        "isData": False
    }

    # WW Tune CP5
    Samples['WW'] = {
        'nanoAOD' :'/WW_TuneCP5_13TeV-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        "isData": False
    }

    # ZZ 
    Samples['ZZ'] = {
        'nanoAOD' :'/ZZ_TuneCP5_13TeV-pythia8/RunIISummer20UL18NanoAODv9-20UL18JMENano_106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        "isData": False
    }

    # WZ
    Samples['WZ'] = {
        'nanoAOD' :'/WZ_TuneCP5_13TeV-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
         "isData": False
    }

    # Wjets
    Samples['WJetsToLNu-LO'] = {
        'nanoAOD' :'/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
         "isData": False
    }

    Samples['GGToLL_M-5To50_TuneCP5_13TeV-pythia8'] = {
        'nanoAOD': '/GGToLL_M-5To50_TuneCP5_13TeV-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM',
        'isData': False
    }

    # Samples['GGToLL_TuneCP5_13TeV-pythia8'] = {
    #     'nanoAOD': '/GGToLL_TuneCP5_13TeV-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM',
    #     'isData': False
    # }
    
    Samples['WJetsToLNu_1J-LO'] = {
        'nanoAOD' :'/W1JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        'isData': False
    }

    Samples['WJetsToLNu_2J-LO'] = {
        'nanoAOD' :'/W2JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
        'isData': False
    }

    Samples['WJetsToLNu_3J-LO'] = {
        'nanoAOD' :'/W3JetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANO',
        'isData': False
    }
    
    # Samples = {k:j for k,j in Samples.items() if k in ["WZ"]}
    files = {}
    for sampleName in Samples:
        # if "DoubleMuon" not in sampleName:
        #     continue
        files[sampleName] = {"isData": Samples[sampleName]["isData"], "query": Samples[sampleName]["nanoAOD"], "files": {}}

    return files

"""
@dask.delayed
def get_filename_nevents(files, **kwargs):
    # file is actually a list of all replicas
    result = {
        "path": files + [],
        "nevents": 200_000_000,
    }

    # max_events = 20_000_000
    for file in files:
        # skip = False
        # for bad_site in kwargs["bad_sites"]:
        #     if bad_site in _file:
        #         skip = True
        #         break
        # if skip:
        #     continue
        try:
            f = uproot.open(file, handler=uproot.source.xrootd.XRootDSource)
            num_entries = f["Events"].num_entries
            f.close()
            nevents = min(num_entries, result["nevents"])
            if nevents < result["nevents"]:
                # the file is good, just move it on the top of the list
                result["path"].pop(files.index(file))
                result["path"].insert(0, file)
                result["nevents"] = nevents
                return result
        except Exception:
            continue
    # final_file = file[0]
    # for bad_site in kwargs["bad_sites"]:
    #     if bad_site in _file:
    #         print("had to use a bad site!", file=sys.stderr)
    #         break
    result["bad_file"] = True
    return result
"""

@dask.delayed
def get_filename_nevents(files, isData=False, **kwargs):
    # file is actually a list of all replicas
    result = {
        "path": files + [],
        "nevents": 200_000_000,
    }

    url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    api=DbsApi(url=url)

    # max_events = 20_000_000

    # we do not really need to cycle on all file replicas 
    # Information will be stored centrally in the database for the non-wildcarded file name
    # However we may fail to build the logical filename so, if that's the case we can try 
    # with the next file replica to add a little bit of redundancy
    for file in files:
        # skip = False
        # for bad_site in kwargs["bad_sites"]:
        #     if bad_site in _file:
        #         skip = True
        #         break
        # if skip:
        #     continue

        ###########################################
        # Need the non-wildcarded logical_file_name
        ###########################################
        # sanity check 
        logical_cutexpr = "/store/data" if isData else "/store/mc"

        if logical_cutexpr not in file:
            print(f"[ERROR] I cannot create the non-wildcarded file name for file {file}")
            continue

        # create non wildcarded file name for query
        nw_filename = logical_cutexpr + file.split(logical_cutexpr)[1]
        fileinfo = api.listFiles(logical_file_name=nw_filename, detail=1)[0]

        if not fileinfo["is_file_valid"]:
            print(f"[ERROR] file {nw_filename} has non valid status, will skip")
            continue

        nevents = min(fileinfo["event_count"], result["nevents"])

        if nevents < result["nevents"]:
            result["path"].pop(files.index(file))
            result["path"].insert(0, file)
            result["nevents"] = nevents
            return result

    result["bad_file"] = True
    return result


def get_cluster(local=True):
    if not local:
        from dask_jobqueue import HTCondorCluster
        import socket

        machines = [
            # "empire.hcms.it",
            # "pccms01.hcms.it",
            "pccms02.hcms.it",
            "pccms04.hcms.it",
            # "pccms08.hcms.it",
            "pccms11.hcms.it",
            "pccms12.hcms.it",
            # "pccms13.hcms.it",
            # "pccms14.hcms.it",
        ]

        cluster = HTCondorCluster(
            cores=1,
            memory="2 GB",  # hardcoded
            disk="100 MB",
            death_timeout="60",
            nanny=False,
            scheduler_options={
                # 'port': n_port,
                "dashboard_address": 8887,
                "host": socket.gethostname(),
            },
            job_extra_directives={
                "log": "dask_out/dask_job_output.log",
                "output": "dask_out/dask_job_output.out",
                "error": "dask_out/dask_job_output.err",
                "should_transfer_files": "Yes",
                "when_to_transfer_output": "ON_EXIT",
                "Requirements": " || ".join(
                    [f'(machine == "{machine}")' for machine in machines]
                ),
            },
            job_script_prologue=[
                "export PATH=/gwpool/users/gpizzati/mambaforge/bin:$PATH",
                "mamba activate test_uproot",
                "export X509_USER_PROXY=/gwpool/users/gpizzati/.proxy",
                "export XRD_RUNFORKHANDLER=1",
            ],
        )
        cluster.scale(150)

        return cluster
    else:
        from distributed import LocalCluster

        cluster = LocalCluster(
            n_workers=10,
            threads_per_worker=1,
            memory_limit="2GB",  # hardcoded
            dashboard_address=":8887",
        )
        return cluster


if __name__ == "__main__":
    kwargs = {
        "bad_sites": [
            # "ts.infn",
        ],
    }
    cluster = get_cluster(local=True)

    client = cluster.get_client()
    client.wait_for_workers(10)

    files = get_files()

    rucio_client = rucio_utils.get_rucio_client()
    # DE|FR|IT|BE|CH|ES|UK
    good_sites = ['IT', 'BE', 'CH', 'UK', 'ES', 'DE', 'US']
    for dname in files:

        dataset = files[dname]["query"]

        print("Checking", dname, "files with query", dataset)
        try:
            (
                outfiles,
                _,
                _,
            ) = rucio_utils.get_dataset_files_replicas(
                dataset,
                allowlist_sites=[],
                blocklist_sites=[
                    # "T2_FR_IPHC",
                    # "T2_ES_IFCA",
                    # "T2_CH_CERN",
                    "T3_IT_Trieste",
                    "T1_FR_CCIN2P3",
                    "T1_DE_KIT",
                    "T1_ES_PIC",
                    "T1_DE_KIT",
                    "T1_ES_PIC",
                    "T1_FR_CCIN2P3",
                    "T1_IT_CNAF",
                    "T1_RU_JINR",
                    "T1_UK_RAL",
                    "T1_US_FNAL",
                    "T1_US_FNAL_Disk",
                    "T2_FR_IPHC",
                    "T3_FR_IPNL"
                    
                ],
                #regex_sites=[],
                regex_sites=r"T[123]_(" + "|".join(good_sites) + ")_\w+",
                # regex_sites = r"T[123]_(DE|IT|BE|CH|ES|UK|US)_\w+",
                mode="full",  # full or first. "full"==all the available replicas
                client=rucio_client,
            )
        except Exception as e:
            print(f"\n[red bold] Exception: {e}[/]")
            sys.exit(1)

        """

        # files[dname]["files"] = list(map(lambda k: k[0], outfiles))
        for file, site in zip(outfiles, outsites):
            #print(file, site)
            # actually file and site are lists here
            file = sorted([(_file, _site.split('_')[1]) for _file , _site in zip(file, site)], key=lambda i: good_sites.index(i[1]))
            
            # print(list(map(lambda k: k[1], file)))
            file = list(map(lambda k: k[0], file))

            files[dname]["files"] = [
                get_filename_nevents(file, **kwargs) for file in outfiles
            ]
        # break
        """
        for f__ in outfiles:  random.shuffle(f__)

        files[dname]["files"] = [
            get_filename_nevents(file, isData=files[dname]["isData"], **kwargs) for file in outfiles
        ]

    files = dask.compute(files)
    files = files[0]    

    MAX_FILES_PER_MC = 50
    MAX_EVENTS_PER_MC = None 

    for dataset in files.keys():

        files[dataset]["files"] = sorted(files[dataset]["files"], key=lambda d: d["nevents"], reverse=True)

        if files[dataset]["isData"]: continue
        if MAX_FILES_PER_MC is not None:
            files[dataset]["files"] = files[dataset]["files"][:MAX_FILES_PER_MC]
        if MAX_EVENTS_PER_MC is not None:
            # sort files by number of events
            
            import numpy as np
            partial_sums = np.array([sum([files[dataset]["files"][i]["nevents"] for i in range(k+1)]) for k in range(len(files[dataset]["files"]))])
            print(dataset, partial_sums)
            # first index for which partial sums are greater than max events per MC
            idx_max = (partial_sums > MAX_EVENTS_PER_MC).argmax() - 1 # -1 so that we are ffectively < MAX_EVENTS_PER_MC
            # if MAX_EVENTS_PER_MC is greater than the total sum ov events then we keep all files
            # but .argmax() will be 0 (idx_max = -1) therefore we set it to None to get every file
            if partial_sums[-1] < MAX_EVENTS_PER_MC: idx_max = None
            files[dataset]["files"] = files[dataset]["files"][:idx_max]


    # print(files)

    with open("data/files_all2.json", "w") as file:
        json.dump(files, file, indent=2)
