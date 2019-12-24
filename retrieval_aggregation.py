import os
import sys
import argparse
from glob import glob, iglob
import re

data_dir = "/data10/hcronk/geocarb/ditl_1/data/L2Ret/process"
part_file_regex = re.compile(".*.prt$")
sel_file_regex = re.compile("geocarb_(?P<product>[L2SEL](.*))_(?P<yyyymmdd>[0-9]{8})_(?P<resolution>(.*))_(?P<box>[boxncsa_0-9]{7,8})-(.*)_(?P<chunk>[chunk0-9]{8}).txt$")
ret_file_regex = re.compile("geocarb_(?P<product>[L2FPRet](.*?))_(?P<sid>[0-9]{19})_(?P<yyyymmdd>[0-9]{8})_(?P<box>[boxncsa_0-9]{7,8})_(?P<chunk>[chunk0-9]{8}).h5$")

verbose=True

for gran_dir in iglob(os.path.join(data_dir, "*")):
    if verbose:
        print("Checking " + gran_dir)
    ret_dir = os.path.join(gran_dir, "l2fp_retrievals")
    if not glob(ret_dir):
        if verbose:
            print(ret_dir + " DNE yet. Moving on.")
        continue
    listdir = glob(os.path.join(ret_dir, "*"))
    if not listdir:
        if verbose:
            print(ret_dir + " is empty. Moving on.")
        continue 
    if any(part_file_regex.match(f) for f in listdir):
        if verbose:
            print(ret_dir + " still has part files. Moving on.")
        continue
    else:
        if verbose:
            print(ret_dir + " is ready to check against the sounding selection file.")
        ret_file_sids = [ret_file_regex.search(f).groupdict()["sid"] for f in listdir]
        sel_filename = [m.group() for f in os.listdir(gran_dir) for m in [sel_file_regex.match(f)] if m][0]
        with open(os.path.join(gran_dir, sel_filename)) as sf:
            sel_file_sids = sf.read().splitlines()
        if not all(sid in ret_file_sids for sid in sel_file_sids):
            if verbose:
                print(ret_dir + " does not have all the SIDs the sounding selection file.")
        else:
            if verbose:
                print(ret_dir + " has all the SIDs the sounding selection file. Time to aggregate.")
        
