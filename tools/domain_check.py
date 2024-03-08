##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################

#
# do a spot check to verify data and metadata from a domain are accessible
#
import sys
import h5pyd as h5py

if len(sys.argv) < 3 or sys.argv[1] in ("-h", "--help"):
    print(f"usage: {sys.argv[0]} <domain> <h5path>")
    sys.exit(1)

domain_path = sys.argv[1]
h5path = sys.argv[2]

f = h5py.File(domain_path, use_cache=False, retries=0)

if h5path not in f:
    print(f"{h5path} not found in {domain_path}")
    sys.exit(1)

dset = f[h5path]
print(f"{h5path}: {dset}")

if not isinstance(dset, h5py.Dataset):
    print(f"{h5path} is not a dataset, exiting")
    sys.exit(0)

rank = len(dset.shape)
if rank > 0:
    # choose something in the middle of the dataset
    tgt_row = dset.shape[rank - 1] // 2
else:
    tgt_row = None

print(f"dset chunks: {dset.chunks}")
slices = [slice(0, 1, 1) for _ in range(rank)]

dcpl_json = dset.id.dcpl_json
if dcpl_json and "layout" in dcpl_json:
    layout_json = dcpl_json["layout"]
    layout_class = layout_json.get("class")
    print(f"layout_class: {layout_class}")
    if layout_class == "H5D_CHUNKED_REF_INDIRECT":
        s3_uri = layout_json["file_uri"]
        print(f"s3_uri: {s3_uri}")

        chunk_shape = layout_json["dims"]
        print(f"layout chunk_shape: {tuple(chunk_shape)}")

        chunk_table_id = layout_json["chunk_table"]
        chunk_table = f["datasets/" + chunk_table_id]
        print(f"chunk_table shape: {chunk_table.shape}")
        # print a chunk_table entry
        chunk_table_row = tgt_row // chunk_shape[-1]

        slices[-1] = slice(chunk_table_row, chunk_table_row + 1, 1)
        chunk_table_data = chunk_table[tuple(slices)]
        print(f"chunk_table data for row {chunk_table_row}: {chunk_table_data}")

print(f"fetching dataset data for {tgt_row}...")
slices[-1] = slice(tgt_row, tgt_row + 1, 1)
# read an element for the dataset
data = dset[tuple(slices)]
print(f"dset data for row: {tgt_row}: {data}")
print("done!")
