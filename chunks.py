import json
from math import ceil
from framework import write_chunks
import random
from copy import deepcopy

def create_datasets():

    import json

    with open("data/files_all2.json", "r") as file:
        files = json.load(file)

    datasets = {
        "DYmm": {
            "files": files["DYJetsToMuMu_M-50"]["files"][:],
            "xs": 1976.0,
        },
        "DYee": {
            "files": files["DYJetsToEE_M-50"]["files"][:],
            "xs": 1976.0,
        },
        "DYtt": {
            "files": files["DYJetsToTauTau_M-50_AtLeastOneEorMuDecay"]["files"][:],
            "xs": 1976.0,
        },
        "TTJets": {
            "files": files["TTJets"]["files"][:],
            "xs": 1976.0,
        },
        "ST_s-channel": {
            "files": files["ST_s-channel"]["files"][:],
            "xs": 1976.0,
        },
        "ST_t-channel_top_5f": {
            "files": files["ST_t-channel_top_5f"]["files"][:],
            "xs": 1976.0,
        },
        "ST_t-channel_antitop_5f": {
            "files": files["ST_t-channel_antitop_5f"]["files"][:],
            "xs": 1976.0,
        },
        "ST_tW_antitop_noHad": {
            "files": files["ST_tW_antitop_noHad"]["files"][:],
            "xs": 1976.0,
        },
        "ST_tW_top_noHad": {
            "files": files["ST_tW_top_noHad"]["files"][:],
            "xs": 1976.0,
        },
        "WW": {
            "files": files["WW"]["files"][:],
            "xs": 1976.0,
        },
        "ZZ": {
            "files": files["ZZ"]["files"][:],
            "xs": 1976.0,
        },
        "WZ": {
            "files": files["WZ"]["files"][:],
            "xs": 1976.0,
        },
        "WJetsToLNu-LO": {
            "files": files["WJetsToLNu-LO"]["files"][:],
            "xs": 1976.0,
        },
        "WJetsToLNu_1J-LO": {
            "files": files["WJetsToLNu_1J-LO"]["files"][:],
            "xs": 1976.0,
        },
        "WJetsToLNu_2J-LO": {
            "files": files["WJetsToLNu_2J-LO"]["files"][:],
            "xs": 1976.0,
        },
        "WJetsToLNu_3J-LO": {
            "files": files["WJetsToLNu_3J-LO"]["files"][:],
            "xs": 1976.0,
        },
        "GGToLL_M-5To50": {
            "files": files["GGToLL_M-5To50_TuneCP5_13TeV-pythia8"]["files"][:],
            "xs": 1976.0,
        }
    }

    for dataset in datasets:
        datasets[dataset]["read_form"] = "mc"

    # DataTrig = {
    #     "MuonEG": "events.EleMu",
    #     "DoubleMuon": "(~events.EleMu) & events.DoubleMu",
    #     "SingleMuon": "(~events.EleMu) & (~events.DoubleMu) & events.SingleMu",
    #     "EGamma": "(~events.EleMu) & (~events.DoubleMu) & (~events.SingleMu) & (events.SingleEle | events.DoubleEle)",
    # }
    DataTrig = {
        "DoubleMuon": "events.DoubleMu",
        "SingleMuon": "(~events.DoubleMu) & events.SingleMu",
        "EGamma":     "(~events.DoubleMu) & (~events.SingleMu) & (events.SingleEle | events.DoubleEle)",
    }

    # for dataset in ["DoubleMuon", "EGamma", "MuonEG", "SingleMuon"]:
    for dataset in ["DoubleMuon", "EGamma", "SingleMuon"]:
        _files = []
        for filesName in [k for k in list(files.keys()) if dataset in k]:
            _files += files[filesName]["files"]
        datasets[dataset] = {
            "files": _files,
            "trigger_sel": DataTrig[dataset],
            "read_form": "data",
            "is_data": True,
        }

    return datasets


def split_chunks(num_entries):
    max_events = 20_000_000
    chunksize = 200_000
    max_events = min(num_entries, max_events)
    nIterations = ceil(max_events / chunksize)
    file_results = []
    for i in range(nIterations):
        start = min(num_entries, chunksize * i)
        stop = min(num_entries, chunksize * (i + 1))
        if start >= stop:
            break
        file_results.append([start, stop])
    return file_results


def create_chunks(datasets):
    #print(datasets)
    chunks = []
    for dataset in datasets:
        dataset_dict = {k: v for k, v in datasets[dataset].items() if k != "files"}
        for file in datasets[dataset]["files"]:
            steps = split_chunks(file["nevents"])
            for start, stop in steps:
                random.shuffle(file['path'])
                shuffledfiles = deepcopy(file['path'])
                d = {
                    "dataset": dataset,
                    "filenames": shuffledfiles,
                    "start": start,
                    "stop": stop,
                    **dataset_dict,
                }
                chunks.append(d)
    return chunks


if __name__ == "__main__":

    datasets = create_datasets()
    print("---> Requested datasets: {}".format(datasets.keys()))
    with open("data/dataset.json", "w") as file:
        json.dump(datasets, file, indent=2)

    chunks = create_chunks(datasets)
    print("Split into {} chunks".format(len(chunks)))
    with open("data/chunks.json", "w") as file:
        json.dump(chunks, file, indent=2)
    write_chunks(chunks, "data/chunks.gz")

