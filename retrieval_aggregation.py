import os
import sys
import argparse
from glob import glob, iglob
import re
import h5py

data_dir = "/data10/hcronk/geocarb/ditl_1/data/L2Ret/process"
part_file_regex = re.compile(".*.prt$")
sel_file_regex = re.compile("geocarb_(?P<product>[L2SEL]{5})_(?P<yyyymmdd>[0-9]{8})_(?P<resolution>(.*))_(?P<box>[boxncsa_0-9]{7,8})-(.*)_(?P<chunk>[chunk0-9]{8}).txt$")
ret_file_regex = re.compile("geocarb_(?P<product>[L2FPRet]{7})_(?P<sid>[0-9]{19})_(?P<yyyymmdd>[0-9]{8})_(?P<box>[boxncsa_0-9]{7,8})_(?P<chunk>[chunk0-9]{8}).h5$")
l1b_file_regex = re.compile("geocarb_(?P<product>[l1b]{3})_rx_intensity_(?P<yyyymmdd>[0-9]{8})_(?P<resolution>(.*))_(?P<box>[boxncsa_0-9]{7,8})-(.*)_(?P<chunk>[chunk0-9]{8}).h5$")

verbose=True

def read_hdf5_datafield_and_attrs(field, filename):
    """
    Extract data from an HDF5 file for a named field"
    """
    
    open_file = h5py.File(filename, 'r')
    data_obj = open_file[field]
    data = data_obj[:]
    data_dtype = data.dtype
    #attr_list = data_obj.attrs.items()
    attr_dict = {}
    for k, v in data_obj.attrs.items():
        attr_dict[str(k)] = v[0]
    
    return data, attr_dict


def build_ds_list(name):
    global DS_NAMES
    DS_NAMES.append(name)

def aggregate(l1b_file):
    
    global DS_NAMES
    
    agg_file = re.sub("l1b_rx_intensity", "L2Ret", os.path.basename(l1b_file))
    print(agg_file)
    l1b_sid = get_hdf5_data("/SoundingGeometry/sounding_id", l1b_file)
    ret_files = sorted(iglob(os.path.join(RET_DIR, "*L2FPRet*.h5")))
    #Set up agg file
    open_file = h5py.File(ret_files[0], "r")
    DS_NAMES = []
    open_file.visit(build_ds_list)
    open_file.close()
    get_rid_of_groups = [DS_NAMES.remove(ds) for ds in DS_NAMES[:] if "/" not in ds]
    
    open_file = h5py.File(agg_file, "w") 
    for ds in DS_NAMES:
        print(ds)
        if "Dimensions/" in ds or "Shapes/" in ds:
            #Figure out at end?
            print("Skipping for now")
            continue
        elif "Metadata" in ds: 
            #go ahead and write this info in, it should be the same for all retrievals?
            data, attr_dict = read_hdf5_datafield_and_attrs(ds, ret_files[0])
            write_dataset = open_file.create_dataset(ds, data = data, dtype = data.dtype, compression="gzip")
            for a in attr_dict.keys():
                #print a
                attr_value = attr_dict.get(a)
                write_dataset.attrs.create(a, data=attr_value)
        else:
            data, attr_dict = read_hdf5_datafield_and_attrs(ds, ret_files[0])
            create_dataset = open_file.create_dataset(ds, l1b_sid.shape)
            for a in attr_dict.keys():
                #print a
                attr_value = attr_dict.get(a)
                create_dataset.attrs.create(a, data=attr_value)
    open_file.close()
    sys.exit()
    

    
    for rf in sorted(iglob(os.path.join(RET_DIR, "*L2FPRet*.h5"))):
        rf_sid = ret_file_regex.search(rf).groupdict()["sid"]
        
    
    
    open_file = h5py.File(agg_file, "w")    
    write_dataset = open_file.create_dataset("/SoundingGeometry/sounding_id", data = l1b_sid, dtype = l1b_sid.dtype, compression="gzip")
    open_file.close()

def get_hdf5_data(var, hdf5_file):
    """
    Extract given variable data from an HDF-5 file
    """
    f = h5py.File(hdf5_file, "r")
    try: 
        data = f[var][:]
    except:
        print("Problem retrieving " + var + " from " + hdf5_file)
        f.close()
        return
    f.close()
    return data


if __name__ == "__main__":
    
    global SEL_FILE_SIDS
    global RET_DIR
    global RET_FILE_SIDS

    for gran_dir in iglob(os.path.join(data_dir, "*")):
        if verbose:
            print("Checking " + gran_dir)
        RET_DIR = os.path.join(gran_dir, "l2fp_retrievals")
        if not glob(RET_DIR):
            if verbose:
                print(RET_DIR + " DNE yet. Moving on.")
            continue
        listdir = glob(os.path.join(RET_DIR, "*"))
        if not listdir:
            if verbose:
                print(RET_DIR + " is empty. Moving on.")
            continue 
        if any(part_file_regex.match(f) for f in listdir):
            if verbose:
                print(RET_DIR + " still has part files. Moving on.")
            continue
        else:
            if verbose:
                print(RET_DIR + " is ready to check against the sounding selection file.")
            RET_FILE_SIDS = sorted([ret_file_regex.search(f).groupdict()["sid"] for f in listdir])
            sel_filename = [m.group() for f in os.listdir(gran_dir) for m in [sel_file_regex.match(f)] if m][0]
            with open(os.path.join(gran_dir, sel_filename)) as sf:
                SEL_FILE_SIDS = sf.read().splitlines()
            if not all(sid in RET_FILE_SIDS for sid in SEL_FILE_SIDS):
                if verbose:
                    print(RET_DIR + " does not have all the SIDs the sounding selection file.")
            else:
                if verbose:
                    print(RET_DIR + " has all the SIDs the sounding selection file. Time to aggregate.")
                l1b_filename = [m.group() for f in os.listdir(gran_dir) for m in [l1b_file_regex.match(f)] if m][0]
                agg = aggregate(os.path.join(gran_dir, l1b_filename))
        
