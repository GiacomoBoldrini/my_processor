import zlib
import cloudpickle
import uproot
import json
import awkward as ak
from copy import deepcopy
import sys
import time
import gc
import traceback as tb


def read_array(tree, branch_name, start, stop):
    interp = tree[branch_name].interpretation
    interp._forth = True
    return tree[branch_name].array(
        interp,
        entry_start=start,
        entry_stop=stop,
        decompression_executor=uproot.source.futures.TrivialExecutor(),
        interpretation_executor=uproot.source.futures.TrivialExecutor(),
    )


def read_events(filename, start=0, stop=100, read_form={}):
    print("start reading")
    # f = uproot.open(filename, handler=uproot.source.xrootd.XRootDSource, num_workers=2)
    uproot_options = dict(
        timeout=30,
        handler=uproot.source.xrootd.XRootDSource,
        num_workers=1,
        use_threads=False,
    )
    f = uproot.open(filename, **uproot_options)
    tree = f["Events"]
    start = min(start, tree.num_entries)
    stop = min(stop, tree.num_entries)
    if start >= stop:
        return ak.Array([])

    branches = [k.name for k in tree.branches]

    events = {}
    form = deepcopy(read_form)

    all_branches = []
    for coll in form:
        coll_branches = form[coll]["branches"]
        if len(coll_branches) == 0:
            if coll in branches:
                all_branches.append(coll)
        else:
            for branch in coll_branches:
                branch_name = coll + "_" + branch
                if branch_name in branches:
                    all_branches.append(branch_name)

    events_bad_form = tree.arrays(
        all_branches,
        entry_start=start,
        entry_stop=stop,
        decompression_executor=uproot.source.futures.TrivialExecutor(),
        interpretation_executor=uproot.source.futures.TrivialExecutor(),
    )
    f.close()

    for coll in form:
        d = {}
        coll_branches = form[coll].pop("branches")

        if len(coll_branches) == 0:
            if coll in branches:
                events[coll] = events_bad_form[coll]
            continue

        for branch in coll_branches:
            branch_name = coll + "_" + branch
            if branch_name in branches:
                d[branch] = events_bad_form[branch_name]

        if len(d.keys()) == 0:
            print("did not find anything for", coll, file=sys.stderr)
            continue

        events[coll] = ak.zip(d, **form[coll])
        del d

    print("created events")
    _events = ak.zip(events, depth_limit=1)
    del events
    gc.collect()
    return _events


def add_dict(d1, d2):
    if isinstance(d1, dict):
        d = {}
        common_keys = set(list(d1.keys())).intersection(list(d2.keys()))
        for key in common_keys:
            d[key] = add_dict(d1[key], d2[key])
        for key in d1:
            if key in common_keys:
                continue
            d[key] = d1[key]
        for key in d2:
            if key in common_keys:
                continue
            d[key] = d2[key]

        return d
    else:
        return d1 + d2


def add_dict_iterable(iterable):
    tmp = -99999
    for it in iterable:
        if tmp == -99999:
            tmp = it
        else:
            tmp = add_dict(tmp, it)
    return tmp


# import dask
# @dask.delayed
def big_process(process, filenames, start, stop, read_form, **kwargs):
    t_start = time.time()

    events = 0
    error = ""
    for filename in filenames:
        try:
            events = read_events(filename, start=start, stop=stop, read_form=read_form)
            break
        except Exception as e:
            error += "".join(tb.format_exception(None, e, e.__traceback__))
            # time.sleep(1)
            continue

    if isinstance(events, int):
        print(error, file=sys.stderr)
        raise Exception(
            "Error, could not read any of the filenames\n" + error, filenames
        )

    t_reading = time.time() - t_start
    if len(events) == 0:
        return {}
    results = {"real_results": 0, "performance": {}}
    results["real_results"] = process(events, **kwargs)
    t_total = time.time() - t_start
    results["performance"][f"{filename}_{start}"] = {
        "total": t_total,
        "read": t_reading,
    }
    del events
    gc.collect()
    return results


def read_chunks(filename, readable=False):
    if not readable:
        with open(filename, "rb") as file:
            chunks = cloudpickle.loads(zlib.decompress(file.read()))
        return chunks
    else:
        with open(filename, "r") as file:
            chunks = json.load(file)
        return chunks


def write_chunks(d, filename, readable=False):
    if not readable:
        with open(filename, "wb") as file:
            file.write(zlib.compress(cloudpickle.dumps(d)))
    else:
        with open(filename, "w") as file:
            json.dump(d, file)
