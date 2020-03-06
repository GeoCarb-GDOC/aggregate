import warnings
warnings.filterwarnings("ignore")

import os
import sys
import argparse
from glob import glob, iglob
import re
import h5py
import numpy as np
import shutil

data_dir = "/nobackup/hcronk/data"
#data_dir = "/data10/hcronk/geocarb/ditl_1/data/L2Ret/testing"
OUTPUT_DIR = "/nobackup/hcronk/data/L2Ret_grans"
#OUTPUT_DIR = "/home/hcronk/geocarb/ditl_1/aggregation"
# the l2_fp code automatically adds a .generating tag to files as they are being written
part_file_regex = re.compile(".*.generating$")
error_file_regex = re.compile(".*.error$")
sel_file_regex = re.compile("geocarb_(?P<product>[L2SEL]{5})_(?P<yyyymmdd>[0-9]{8})_(?P<resolution>(.*))_(?P<box>[boxncsa_0-9]{7,8})-(.*)_(?P<chunk>[chunk0-9]{8}).txt$")
ret_file_regex = re.compile("geocarb_(?P<product>[L2FPRet]{7})_(?P<sid>[0-9]{19})_(?P<yyyymmdd>[0-9]{8})_(?P<box>[boxncsa_0-9]{7,8})_(?P<chunk>[chunk0-9]{8}).h5")
l1b_file_regex = re.compile("geocarb_(?P<product>[l1b]{3})_rx_intensity_(?P<yyyymmdd>[0-9]{8})_(?P<resolution>(.*))_(?P<box>[boxncsa_0-9]{7,8})-(.*)_(?P<chunk>[chunk0-9]{8}).h5$")

verbose=True

def read_hdf5_datafield_and_attrs(field, filename):
    """
    Extract data from an HDF5 file for a named field"
    """
    try:
        open_file = h5py.File(filename, 'r')
    except Exception as e:
            print("Problem opening " + filename)
            print("Error:", e)
            sys.exit()
    try:
        data_obj = open_file[field]
    except Exception as e:
            print("Problem attaching to " + field)
            print("Error:", e)
            sys.exit()
    try:
        data = data_obj[:]
    except Exception as e:
            print("Problem extracting data from " + field)
            print("Error:", e)
            sys.exit()
    try:
        attr_dict = {}
        for k, v in data_obj.attrs.items():
            attr_dict[str(k)] = v[0]
    except Exception as e:
            print("Problem extracting attributes from " + field)
            print("Error:", e)
            sys.exit()
            
    open_file.close()
    return data, attr_dict

def build_ds_list(name):
    global DS_NAMES
    DS_NAMES.append(name)

def aggregate(l1b_file):
    
    global DS_NAMES
    global AGG_FILE
    all_dat_dict = {}
    all_attrs_dict = {}
    
    AGG_FILE = os.path.join(OUTPUT_DIR, re.sub("l1b_rx_intensity", "L2Ret", os.path.basename(l1b_file)))
    l1b_sid, attr_dict = read_hdf5_datafield_and_attrs("/SoundingGeometry/sounding_id", l1b_file)
    sid_bool = np.isin(l1b_sid, np.array(SEL_FILE_SIDS).astype("int64"))
    relevant_sids = np.ma.masked_array(l1b_sid, ~sid_bool)
    #.error files are ok to aggregate
    ret_files = sorted(iglob(os.path.join(RET_DIR, "*L2FPRet*.h5*")))
    
    #Set up agg file
    open_file = h5py.File(ret_files[0], "r")
    DS_NAMES = []
    open_file.visit(build_ds_list)
    open_file.close()
    get_rid_of_groups = [DS_NAMES.remove(ds) for ds in DS_NAMES[:] if "/" not in ds]
    #Skip Dimensions/, Shapes/, and /RetrievalResults/aerosol_model for now; handle Metadata once separately
    metadata_ds_names = [ds for ds in DS_NAMES[:] if "Metadata" in ds]
    get_rid_of_extras = [DS_NAMES.remove(ds) for ds in DS_NAMES[:] if "Dimensions/" in ds or 
                                                               "Shapes/" in ds or 
                                                               "Metadata/" in ds or 
                                                               "RetrievalResults/aerosol_model" in ds or
                                                               "RetrievalResults/surface_type" in ds or
                                                               "RetrievedStateVector/state_vector_names" in ds]
    
    #Write metadata fields into agg file
    open_out_file = h5py.File(AGG_FILE + ".generating", "w")
    try:
        open_first_file = h5py.File(ret_files[0], 'r')
    except Exception as e:
        print("Problem opening " + ret_files[0])
        print("Error:", e)
        sys.exit()
    for ds in metadata_ds_names:
        #print(ds)
        try:
            data_obj = open_first_file[ds]
        except Exception as e:
                print("Problem attaching to " + ds)
                print("Error:", e)
                sys.exit()
        try:
            data = data_obj[:]
        except Exception as e:
                print("Problem extracting data from " + ds)
                print("Error:", e)
                sys.exit()
        try:
            attr_dict = {}
            for k, v in data_obj.attrs.items():
                attr_dict[str(k)] = v[0]
        except Exception as e:
                print("Problem extracting attributes from " + ds)
                print("Error:", e)
                sys.exit()
    
    for ds in DS_NAMES:
        #print(ds)
        try:
            data_obj = open_first_file[ds]
        except Exception as e:
                print("Problem attaching to " + ds)
                print("Error:", e)
                sys.exit()
        try:
            data = data_obj[:]
        except Exception as e:
                print("Problem extracting data from " + ds)
                print("Error:", e)
                sys.exit()
        try:
            attr_dict = {}
            for k, v in data_obj.attrs.items():
                attr_dict[str(k)] = v[0]
        except Exception as e:
                print("Problem extracting attributes from " + ds)
                print("Error:", e)
                sys.exit()
        
        data_dtype = data.dtype
#        if data.dtype == object:
#            #byte/string issue with /RetrievalResults/aerosol_model...figure out if we actually need it
#            continue
        if data.ndim == 1:
            add_xdim = np.expand_dims(data, axis=0)
            new_data_shape = list(add_xdim.shape)
            new_data_shape[0] = len(l1b_sid)
        else:
            new_data_shape = list(data.shape)
            new_data_shape[0] = len(l1b_sid)
        #print(new_data_shape)
        all_dat_dict[ds] = np.full(tuple(new_data_shape), np.nan, dtype = data_dtype)
        all_attrs_dict[ds] = attr_dict
    open_first_file.close() 
       
    for sid in relevant_sids.compressed():
        #print(sid)
        ret_file = glob(os.path.join(RET_DIR, "*L2FPRet_" + str(sid) + "*.h5*"))[0]
        xidx, yidx = np.where(l1b_sid == sid)
        open_ret_file = h5py.File(ret_file, 'r')
        for ret_ds in DS_NAMES:
            #print(ret_ds)
            #print(all_dat_dict[ret_ds].shape)
            all_dat_dict[ret_ds][xidx] = open_ret_file[ret_ds][:]
        open_ret_file.close()
    
    for ds_name, dat in all_dat_dict.items():
        print("Writing " + ds_name)
        write_dataset = open_out_file.create_dataset(ds_name, data=dat, dtype = data_dtype, compression="gzip")
        for attr_name, attr_value in all_attrs_dict[ds_name].items():
            print("Writing " + attr_name)
            write_dataset.attrs.create(attr_name, data=attr_value)
    #open_first_file.close()
    open_out_file.close()
    #sys.exit()
    
#    #write other datasets into agg file
#    for ds in DS_NAMES:
#        print(ds)
#        if "Dimensions/" in ds or "Shapes/" in ds:
#            #Figure out at end?
#            #Eventually we may just want to put these fields in ourselves when we package the L2Dia/L2Std
#            print("Skipping for now")
#            continue
#        elif "Metadata" in ds: 
#            #go ahead and write this info in, it should be the same for all retrievals?
#            #Eventually we may just want to put these fields in ourselves when we package the L2Dia/L2Std
#            data, attr_dict = read_hdf5_datafield_and_attrs(ds, ret_files[0])
#            write_dataset = open_file.create_dataset(ds, data = data, dtype = data.dtype, compression="gzip")
#            for a in attr_dict.keys():
#                #print a
#                attr_value = attr_dict.get(a)
#                write_dataset.attrs.create(a, data=attr_value)
#        else:
#        data, attr_dict = read_hdf5_datafield_and_attrs(ds, ret_files[0])
#        data_dtype = data.dtype
#        if data.dtype == object:
#            #byte/string issue with /RetrievalResults/aerosol_model...figure out if we actually need it
#            continue
#        if data.ndim == 1:
#            add_xdim = np.expand_dims(data, axis=0)
#            new_data_shape = list(add_xdim.shape)
#            new_data_shape[0] = len(l1b_sid)
#        else:
#            new_data_shape = list(data.shape)
#            new_data_shape[0] = len(l1b_sid)
#        all_data = np.full(tuple(new_data_shape), np.nan, dtype = data_dtype)
#
#        for sid in relevant_sids.compressed():
#            #print(sid)
#            ret_file = glob(os.path.join(RET_DIR, "*L2FPRet_" + str(sid) + "*.h5*"))[0]
#            xidx, yidx = np.where(l1b_sid == sid)
#            data, attr_dict = read_hdf5_datafield_and_attrs(ds, ret_file)
#            all_data[xidx] = data
#
#        write_dataset = open_file.create_dataset(ds, data = all_data, dtype = data_dtype, compression="gzip")
#        for a in attr_dict.keys():
#            #print a
#            attr_value = attr_dict.get(a)
#            write_dataset.attrs.create(a, data=attr_value)
#    open_file.close()
    return True

if __name__ == "__main__":
    
    global SEL_FILE_SIDS
    global RET_DIR
    
    parser = argparse.ArgumentParser(description="GeoCarb L2FP retrieval aggregation", prefix_chars="-")
    parser.add_argument(dest="gran_to_process", help="Full path to granule directory", default="")
    args = parser.parse_args()
    
    #gran_to_process = args.gran
    if args.gran_to_process:
        all_gran_dirs = [args.gran_to_process]
    else:
        all_gran_dirs = iglob(os.path.join(data_dir, "process", "*"))
        
    for gran_dir in all_gran_dirs:
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
            #.error files are ok to aggregate
            if verbose:
                print(RET_DIR + " is ready to check against the sounding selection file.")
            try:
                ret_file_sids = sorted([ret_file_regex.search(f).groupdict()["sid"] for f in listdir])
            except AttributeError as ae:
                print("Exception with retrieval file SID extraction from filenames")
                print("RET_DIR", RET_DIR)
                print("Regex:",ret_file_regex)
                for f in listdir:
                    print(f)
                    try:
                       sid = ret_file_regex.search(f).groupdict()["sid"]
                    except:
                       print("Didn't work")
                       continue
                continue
            sel_filename = [m.group() for f in os.listdir(gran_dir) for m in [sel_file_regex.match(f)] if m][0]
            with open(os.path.join(gran_dir, sel_filename)) as sf:
                SEL_FILE_SIDS = sf.read().splitlines()
            if not all(sid in ret_file_sids for sid in SEL_FILE_SIDS):
                if verbose:
                    print(RET_DIR + " does not have all the SIDs the sounding selection file.")
            else:
                if verbose:
                    print(RET_DIR + " has all the SIDs the sounding selection file. Time to aggregate.")
                l1b_filename = [m.group() for f in os.listdir(gran_dir) for m in [l1b_file_regex.match(f)] if m][0]
                agg = aggregate(os.path.join(gran_dir, l1b_filename))
                if agg:
                    get_rid_of_partfile = shutil.move(AGG_FILE + ".generating", AGG_FILE)
                    if verbose:
                        print("Aggregation successful for " + os.path.basename(gran_dir))
                        print("Copying " +  gran_dir + " to  " + re.sub("process", "complete", gran_dir))
                    #cmd = "shiftc -r " + gran_dir + " " + re.sub("process", "complete", gran_dir)
                    #print(cmd)
                    #os.system(cmd)
                    move_to_complete = shutil.move(gran_dir, re.sub("process", "complete", gran_dir))
                    #if os.path.isdir(re.sub("process", "complete", gran_dir)):
                    #    shutil.rmtree(gran_dir)
 
