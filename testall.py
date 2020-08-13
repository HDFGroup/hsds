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

import os
import sys


unit_tests = ('arrayUtilTest', 'chunkUtilTest', 'domainUtilTest',
    'dsetUtilTest', 'hdf5dtypeTest', 'idUtilTest', 'lruCacheTest', 'shuffleTest')

integ_tests = ('uptest', 'setup_test', 'domain_test', 'group_test', 'link_test',
 'attr_test', 'datatype_test', 'dataset_test', 'acl_test', 'value_test', 'pointsel_test', 'query_test', 'vlen_test' )

skip_unit = False
if len(sys.argv) > 1:
    arg = sys.argv[1]
    if arg == "--skip_unit":
        skip_unit = True
    else:
        print("Usage: python testall.py [--skip_unit]")
        sys.exit(0)

cwd = os.getcwd()
no_server = False
if len(sys.argv) > 1:
    if sys.argv[1] == '--unit':
        integ_tests = () # skip integ tests
    elif sys.argv[1] == '--integ':
        unit_tests = () # skip unit tests


this_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
test_dir = os.path.join(this_dir, "tests")

os.chdir(test_dir)

#
# Run all hsds tests
#
if not skip_unit:
    os.chdir('unit')
    for file_name in unit_tests:
        print(file_name)
        rc = os.system('python ' + file_name + '.py')
        if rc != 0:
            os.chdir(cwd)
            sys.exit("Failed")
    os.chdir("..")

print("cwd", os.getcwd())
os.chdir('integ')

for file_name in integ_tests:
    print(file_name)
    rc = os.system('python ' + file_name + '.py')
    if rc != 0:
        os.chdir(cwd)
        sys.exit("Failed")

os.chdir(cwd)
print("Done!")
