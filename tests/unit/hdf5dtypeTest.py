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
import unittest
import logging
import numpy as np
import sys

sys.path.append('../../hsds')
sys.path.append('../../hsds/util')

import hdf5dtype
from hdf5dtype import special_dtype
from hdf5dtype import check_dtype
from hdf5dtype import Reference
from hdf5dtype import RegionReference


class Hdf5dtypeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(Hdf5dtypeTest, self).__init__(*args, **kwargs)
        # main
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def testGetBaseTypeJson(self):
        type_json = hdf5dtype.getBaseTypeJson("H5T_IEEE_F64LE")
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_FLOAT")
        self.assertTrue("base" in type_json)
        self.assertEqual(type_json["base"], "H5T_IEEE_F64LE")

        type_json = hdf5dtype.getBaseTypeJson("H5T_STD_I32LE")
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_INTEGER")
        self.assertTrue("base" in type_json)
        self.assertEqual(type_json["base"], "H5T_STD_I32LE")

        try:
            hdf5dtype.getBaseTypeJson("foobar")
            self.assertTrue(False)
        except TypeError:
            pass # expected



    def testBaseIntegerTypeItem(self):
        dt = np.dtype('<i1')
        typeItem = hdf5dtype.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_INTEGER')
        self.assertEqual(typeItem['base'], 'H5T_STD_I8LE')
        typeItem = hdf5dtype.getTypeResponse(typeItem) # non-verbose format
        self.assertEqual(typeItem['class'], 'H5T_INTEGER')
        self.assertEqual(typeItem['base'], 'H5T_STD_I8LE')


    def testBaseFloatTypeItem(self):
        dt = np.dtype('<f8')
        typeItem = hdf5dtype.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_FLOAT')
        self.assertEqual(typeItem['base'], 'H5T_IEEE_F64LE')
        typeItem = hdf5dtype.getTypeResponse(typeItem) # non-verbose format
        self.assertEqual(typeItem['class'], 'H5T_FLOAT')
        self.assertEqual(typeItem['base'], 'H5T_IEEE_F64LE')

    def testBaseStringTypeItem(self):
        dt = np.dtype('S3')
        typeItem = hdf5dtype.getTypeItem(dt)
        self.assertEqual(typeItem['class'], 'H5T_STRING')
        self.assertEqual(typeItem['length'], 3)
        self.assertEqual(typeItem['strPad'], 'H5T_STR_NULLPAD')
        self.assertEqual(typeItem['charSet'], 'H5T_CSET_ASCII')

    def testBaseStringUTFTypeItem(self):
        dt = np.dtype('U3')
        try:
            typeItem = hdf5dtype.getTypeItem(dt)
            self.assertTrue(typeItem is not None)  # avoid pyflakes error
            self.assertTrue(False)  # expected exception
        except TypeError:
            pass # expected

    def testBaseVLenAsciiTypeItem(self):
        dt = special_dtype(vlen=bytes)
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_STRING')
        self.assertEqual(typeItem['length'], 'H5T_VARIABLE')
        self.assertEqual(typeItem['strPad'], 'H5T_STR_NULLTERM')
        self.assertEqual(typeItem['charSet'], 'H5T_CSET_ASCII')
        self.assertEqual(typeSize, 'H5T_VARIABLE')
        

    def testBaseVLenUnicodeTypeItem(self):
        dt = special_dtype(vlen=str)
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_STRING')
        self.assertEqual(typeItem['length'], 'H5T_VARIABLE')
        self.assertEqual(typeItem['strPad'], 'H5T_STR_NULLTERM')
        self.assertEqual(typeItem['charSet'], 'H5T_CSET_UTF8')
        self.assertEqual(typeSize, 'H5T_VARIABLE')

    def testBaseEnumTypeItem(self):
        mapping = {'RED': 0, 'GREEN': 1, 'BLUE': 2}
        dt = special_dtype(enum=(np.int8, mapping))
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_ENUM')
        baseItem = typeItem['base']
        self.assertEqual(baseItem['class'], 'H5T_INTEGER')
        self.assertEqual(baseItem['base'], 'H5T_STD_I8LE')
        self.assertTrue('mapping' in typeItem)
        self.assertEqual(typeItem['mapping']['GREEN'], 1)
        self.assertEqual(typeSize, 1)

    def testBaseBoolTypeItem(self):
        typeItem = hdf5dtype.getTypeItem(np.dtype('bool'))
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_ENUM')
        baseItem = typeItem['base']
        self.assertEqual(baseItem['class'], 'H5T_INTEGER')
        self.assertEqual(baseItem['base'], 'H5T_STD_I8LE')
        self.assertTrue('mapping' in typeItem)
        mapping = typeItem['mapping']
        self.assertEqual(len(mapping), 2)
        self.assertEqual(mapping['FALSE'], 0)
        self.assertEqual(mapping['TRUE'], 1)
        self.assertEqual(typeSize, 1)

    def testBaseArrayTypeItem(self):
        dt = np.dtype('(2,2)<int32')
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_ARRAY')
        baseItem = typeItem['base']
        self.assertEqual(baseItem['class'], 'H5T_INTEGER')
        self.assertEqual(baseItem['base'], 'H5T_STD_I32LE')
        self.assertEqual(typeSize, 16)

    def testObjReferenceTypeItem(self):
        dt = special_dtype(ref=Reference)
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_REFERENCE')  
        self.assertEqual(typeItem['base'], 'H5T_STD_REF_OBJ')
        # length of obj id, e.g.:
        # g-b2c9a750-a557-11e7-ab09-0242ac110009
        self.assertEqual(typeSize, 48)

    def testRegionReferenceTypeItem(self):
        dt = special_dtype(ref=RegionReference)
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_REFERENCE')
        #self.assertEqual(typeItem['base'], 'H5T_STD_REF_DSETREG')
        #self.assertEqual(typeSize, 'H5T_VARIABLE')

    def testCompoundArrayTypeItem(self):
        dt = np.dtype([('a', '<i1'), ('b', 'S1', (10,))])
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_COMPOUND')
        fields = typeItem['fields']
        field_a = fields[0]
        self.assertEqual(field_a['name'], 'a')
        field_a_type = field_a['type']
        self.assertEqual(field_a_type['class'], 'H5T_INTEGER')
        self.assertEqual(field_a_type['base'], 'H5T_STD_I8LE')
        field_b = fields[1]
        self.assertEqual(field_b['name'], 'b')
        field_b_type = field_b['type']
        self.assertEqual(field_b_type['class'], 'H5T_ARRAY')
        self.assertEqual(field_b_type['dims'], (10,))
        field_b_basetype = field_b_type['base']
        self.assertEqual(field_b_basetype['class'], 'H5T_STRING')
        self.assertEqual(typeSize, 11)
    
        
    def testCompoundArrayVlenIntTypeItem(self):
        dt_vlen = special_dtype(vlen=np.int32)
        dt_arr = np.dtype((dt_vlen, (4,)))
        dt_compound = np.dtype([('VALUE1', np.float64), ('VALUE2', np.int64), ('VALUE3', dt_arr) ])
        typeItem = hdf5dtype.getTypeItem(dt_compound)
        
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeSize, 'H5T_VARIABLE')
        self.assertEqual(typeItem['class'], 'H5T_COMPOUND')
        fields = typeItem['fields']
        field_a = fields[0]
        self.assertEqual(field_a['name'], 'VALUE1')
        field_a_type = field_a['type']
        self.assertEqual(field_a_type['class'], 'H5T_FLOAT')
        self.assertEqual(field_a_type['base'], 'H5T_IEEE_F64LE')
        field_b = fields[1]
        self.assertEqual(field_b['name'], 'VALUE2')
        field_b_type = field_b['type']
        self.assertEqual(field_b_type['class'], 'H5T_INTEGER')
        self.assertEqual(field_b_type['base'], 'H5T_STD_I64LE')
        field_c = fields[2]
        field_c_type = field_c['type']
        self.assertEqual(field_c_type['class'], 'H5T_ARRAY')
        self.assertEqual(field_c_type['dims'], (4,))
        field_c_base_type = field_c_type['base']
        self.assertEqual(field_c_base_type['class'], 'H5T_VLEN')
        self.assertEqual(field_c_base_type['size'], 'H5T_VARIABLE')
        field_c_base_base_type = field_c_base_type['base']
        self.assertEqual(field_c_base_base_type['class'], 'H5T_INTEGER')
        self.assertEqual(field_c_base_base_type['base'], 'H5T_STD_I32LE')
        
    def testCompoundArrayVlenStringTypeItem(self):
        dt_vlen = special_dtype(vlen=bytes)
        dt_arr = np.dtype((dt_vlen, (4,)))
        dt_compound = np.dtype([('VALUE1', np.float64), ('VALUE2', np.int64), ('VALUE3', dt_arr) ])
        typeItem = hdf5dtype.getTypeItem(dt_compound)
        
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeSize, 'H5T_VARIABLE')
        self.assertEqual(typeItem['class'], 'H5T_COMPOUND')
        fields = typeItem['fields']
        field_a = fields[0]
        self.assertEqual(field_a['name'], 'VALUE1')
        field_a_type = field_a['type']
        self.assertEqual(field_a_type['class'], 'H5T_FLOAT')
        self.assertEqual(field_a_type['base'], 'H5T_IEEE_F64LE')
        field_b = fields[1]
        self.assertEqual(field_b['name'], 'VALUE2')
        field_b_type = field_b['type']
        self.assertEqual(field_b_type['class'], 'H5T_INTEGER')
        self.assertEqual(field_b_type['base'], 'H5T_STD_I64LE')
        field_c = fields[2]
        field_c_type = field_c['type']
         
        self.assertEqual(field_c_type['class'], 'H5T_ARRAY')
        self.assertEqual(field_c_type['dims'], (4,))
        field_c_base_type = field_c_type['base']
        self.assertEqual(field_c_base_type['class'], 'H5T_STRING')
        self.assertEqual(field_c_base_type['length'], 'H5T_VARIABLE')
        self.assertEqual(field_c_base_type['charSet'], 'H5T_CSET_ASCII')
    
    def testOpaqueTypeItem(self):
        dt = np.dtype('V200')
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_OPAQUE')
        self.assertTrue('base' not in typeItem)
        self.assertEqual(typeSize, 200)

    def testVlenDataItem(self):
        dt = special_dtype(vlen=np.dtype('int32'))
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_VLEN')
        self.assertEqual(typeItem['size'], 'H5T_VARIABLE')
        baseItem = typeItem['base']
        self.assertEqual(baseItem['base'], 'H5T_STD_I32LE')
        self.assertEqual(typeSize, 'H5T_VARIABLE')

    def testCompoundTypeItem(self):
        dt = np.dtype([("temp", np.float32), ("pressure", np.float32), ("wind", np.int16)])
        typeItem = hdf5dtype.getTypeItem(dt)
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_COMPOUND')
        self.assertTrue('fields' in typeItem)
        fields = typeItem['fields']
        self.assertEqual(len(fields), 3)
        tempField = fields[0]
        self.assertEqual(tempField['name'], 'temp')
        self.assertTrue('type' in tempField)
        tempFieldType = tempField['type']
        self.assertEqual(tempFieldType['class'], 'H5T_FLOAT')
        self.assertEqual(tempFieldType['base'], 'H5T_IEEE_F32LE')
        self.assertEqual(typeSize, 10)

        typeItem = hdf5dtype.getTypeResponse(typeItem) # non-verbose format
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeItem['class'], 'H5T_COMPOUND')
        self.assertTrue('fields' in typeItem)
        fields = typeItem['fields']
        self.assertEqual(len(fields), 3)
        tempField = fields[0]
        self.assertEqual(tempField['name'], 'temp')
        self.assertTrue('type' in tempField)
        tempFieldType = tempField['type']
        self.assertEqual(tempFieldType['class'], 'H5T_FLOAT')
        self.assertEqual(tempFieldType['base'], 'H5T_IEEE_F32LE')
        self.assertEqual(typeSize, 10)
        
    def testCompoundofCompoundTypeItem(self):
        dt1 = np.dtype([("x", np.float32), ("y", np.float32)])
        dt2 = np.dtype([("a", np.float32), ("b", np.float32), ("c", np.float32)])
        dt = np.dtype([("field1", dt1), ("field2", dt2)])
        typeItem = hdf5dtype.getTypeItem(dt)
         
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeSize, 20)
        self.assertEqual(typeItem['class'], 'H5T_COMPOUND')
        self.assertTrue('fields' in typeItem)
        fields = typeItem['fields']
        self.assertEqual(len(fields), 2)
        field1 = fields[0]
         
        self.assertEqual(field1['name'], "field1")
        field1_type = field1['type']
        self.assertEqual(field1_type['class'], 'H5T_COMPOUND')
        field2 = fields[1]
        
        self.assertEqual(field2['name'], "field2")
        field2_type = field2['type']
        self.assertEqual(field2_type['class'], 'H5T_COMPOUND')
        

    def testCreateBaseType(self):
        dt = hdf5dtype.createDataType('H5T_STD_U32BE')
        self.assertEqual(dt.name, 'uint32')
        self.assertEqual(dt.byteorder, '>')
        self.assertEqual(dt.kind, 'u')

        dt = hdf5dtype.createDataType('H5T_STD_I16LE')
        self.assertEqual(dt.name, 'int16')
        self.assertEqual(dt.kind, 'i')

        dt = hdf5dtype.createDataType('H5T_IEEE_F64LE')
        self.assertEqual(dt.name, 'float64')
        self.assertEqual(dt.kind, 'f')

        dt = hdf5dtype.createDataType('H5T_IEEE_F32LE')
        self.assertEqual(dt.name, 'float32')
        self.assertEqual(dt.kind, 'f')

        typeItem = { 'class': 'H5T_INTEGER', 'base': 'H5T_STD_I32BE' }
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'int32')
        self.assertEqual(dt.kind, 'i')
        self.assertEqual(typeSize, 4)

    def testCreateBaseStringType(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_ASCII', 'length': 6 }
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'bytes48')
        self.assertEqual(dt.kind, 'S')
        self.assertEqual(typeSize, 6)

    def testCreateBaseUnicodeType(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_UTF8', 'length': 32 }
        try:
            dt = hdf5dtype.createDataType(typeItem)
            self.assertTrue(not dt is None)
            self.assertTrue(False)  # expected exception
        except TypeError:
            pass

    def testCreateNullTermStringType(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_ASCII',
            'length': 6, 'strPad': 'H5T_STR_NULLTERM'}
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)
        
        self.assertEqual(dt.name, 'bytes48')
        self.assertEqual(dt.kind, 'S')
        self.assertEqual(typeSize, 6)


    def testCreateVLenStringType(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_ASCII', 'length': 'H5T_VARIABLE' }
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'object')
        self.assertEqual(dt.kind, 'O')
        self.assertEqual(check_dtype(vlen=dt), bytes)
        self.assertEqual(typeSize, 'H5T_VARIABLE')
        
    def testCreateVLenUTF8Type(self):
        typeItem = { 'class': 'H5T_STRING', 'charSet': 'H5T_CSET_UTF8', 'length': 'H5T_VARIABLE' }
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'object')
        self.assertEqual(dt.kind, 'O')
        self.assertEqual(check_dtype(vlen=dt), str)
        self.assertEqual(typeSize, 'H5T_VARIABLE')

    def testCreateVLenDataType(self):
        typeItem = {'class': 'H5T_VLEN', 'base': 'H5T_STD_I32BE'}
        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeSize, 'H5T_VARIABLE')
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'object')
        self.assertEqual(dt.kind, 'O')
        

    def testCreateOpaqueType(self):
        typeItem = {'class': 'H5T_OPAQUE', 'size': 200}
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'void1600')
        self.assertEqual(dt.kind, 'V')
        self.assertEqual(typeSize, 200)

    def testCreateEnumType(self):
        typeItem = {
                "class": "H5T_ENUM",
                "base": {
                    "base": "H5T_STD_I16LE",
                    "class": "H5T_INTEGER"
                }, 
                "mapping": {
                    "GAS": 2,
                    "LIQUID": 1,
                    "PLASMA": 3,
                    "SOLID": 0
                }
            }

        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeSize, 2)
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'int16')
        self.assertEqual(dt.kind, 'i')    
        mapping = check_dtype(enum=dt)
        self.assertTrue(isinstance(mapping, dict))
        self.assertEqual(mapping["SOLID"], 0)
        self.assertEqual(mapping["LIQUID"], 1)
        self.assertEqual(mapping["GAS"], 2)
        self.assertEqual(mapping["PLASMA"], 3)

    def testCreateBoolType(self):
        typeItem = {
                "class": "H5T_ENUM",
                "base": {
                    "base": "H5T_STD_I8LE",
                    "class": "H5T_INTEGER"
                }, 
                "mapping": {
                    "TRUE": 1,
                    "FALSE": 0
                }
            }

        typeSize = hdf5dtype.getItemSize(typeItem)
        self.assertEqual(typeSize, 1)
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'bool')
        self.assertEqual(dt.kind, 'b')

    def testCreateCompoundType(self):
        typeItem = {
            'class': 'H5T_COMPOUND', 'fields':
                [{'name': 'temp',     'type': 'H5T_IEEE_F32LE'},
                 {'name': 'pressure', 'type': 'H5T_IEEE_F32LE'},
                 {'name': 'location', 'type': {
                     'length': 'H5T_VARIABLE',
                     'charSet': 'H5T_CSET_ASCII',
                     'class': 'H5T_STRING',
                     'strPad': 'H5T_STR_NULLTERM'}},
                 {'name': 'wind',     'type': 'H5T_STD_I16LE'}]
        }
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'void144')
        self.assertEqual(dt.kind, 'V')
        self.assertEqual(len(dt.fields), 4)
        dtLocation = dt[2]
        self.assertEqual(dtLocation.name, 'object')
        self.assertEqual(dtLocation.kind, 'O')
        self.assertEqual(check_dtype(vlen=dtLocation), bytes)
        self.assertEqual(typeSize, 'H5T_VARIABLE')

    
    def testCreateCompoundInvalidFieldName(self):
        typeItem = {
            'class': 'H5T_COMPOUND', 'fields': 
            [{'name': '\u03b1', 'type': {'base': 'H5T_STD_I32LE', 'class': 'H5T_INTEGER'}}, 
             {'name': '\u03c9', 'type': {'base': 'H5T_STD_I32LE', 'class': 'H5T_INTEGER'}}]
        }
        try:
            hdf5dtype.createDataType(typeItem)
            self.assertTrue(False)
        except TypeError:
            pass # expected


    def testCreateCompoundOfCompoundType(self):
        typeItem = {'class': 'H5T_COMPOUND', 'fields': 
        [{'name': 'field1', 'type': {'class': 'H5T_COMPOUND', 'fields': 
        [{'name': 'x', 'type': {'class': 'H5T_FLOAT', 'base': 'H5T_IEEE_F32LE'}}, 
         {'name': 'y', 'type': {'class': 'H5T_FLOAT', 'base': 'H5T_IEEE_F32LE'}}]}}, 
         {'name': 'field2', 'type': {'class': 'H5T_COMPOUND', 'fields': 
         [{'name': 'a', 'type': {'class': 'H5T_FLOAT', 'base': 'H5T_IEEE_F32LE'}}, 
         {'name': 'b', 'type': {'class': 'H5T_FLOAT', 'base': 'H5T_IEEE_F32LE'}}, 
         {'name': 'c', 'type': {'class': 'H5T_FLOAT', 'base': 'H5T_IEEE_F32LE'}}]}}]}
        dt =  hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'void160')
        self.assertEqual(dt.kind, 'V')
        self.assertEqual(len(dt.fields), 2)
        dt_field1 = dt[0]
        self.assertEqual(dt_field1.name, 'void64')
        self.assertEqual(dt_field1.kind, 'V')
        self.assertEqual(len(dt_field1.fields), 2)
        dt_field2 = dt[1]
        self.assertEqual(dt_field2.name, 'void96')
        self.assertEqual(dt_field2.kind, 'V')
        self.assertEqual(len(dt_field2.fields), 3)
        

    def testCreateCompoundTypeUnicodeFields(self):
        typeItem = {
            'class': 'H5T_COMPOUND', 'fields':
                [{'name': u'temp',     'type': 'H5T_IEEE_F32LE'},
                 {'name': u'pressure', 'type': 'H5T_IEEE_F32LE'},
                 {'name': u'wind',     'type': 'H5T_STD_I16LE'}]
        }
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)   
        self.assertEqual(dt.name, 'void80')
        self.assertEqual(dt.kind, 'V')
        self.assertEqual(len(dt.fields), 3)
        self.assertEqual(typeSize, 10)

    def testCreateArrayType(self):
        typeItem = {'class': 'H5T_ARRAY',
                    'base': 'H5T_STD_I64LE',
                    'dims': (3, 5) }
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)
        self.assertEqual(dt.name, 'void960')
        self.assertEqual(dt.kind, 'V')
        self.assertEqual(typeSize, 120)

    def testCreateArrayIntegerType(self):
        typeItem = {'class': 'H5T_INTEGER',
                    'base': 'H5T_STD_I64LE',
                    'dims': (3, 5) }
            
        try:
            hdf5dtype.createDataType(typeItem)
            self.assertTrue(False) # expected exception - dims used with non-array type
        except TypeError:
            pass # should get exception
         

    def testCreateCompoundArrayType(self):
        typeItem = {
            "class": "H5T_COMPOUND",
            "fields": [
                {
                    "type": {
                        "base": "H5T_STD_I8LE",
                        "class": "H5T_INTEGER"
                    },
                    "name": "a"
                },
                {
                    "type": {
                        "dims": [
                            10
                        ],
                        "base": {
                            "length": 1,
                            "charSet": "H5T_CSET_ASCII",
                            "class": "H5T_STRING",
                            "strPad": "H5T_STR_NULLPAD"
                        },
                    "class": "H5T_ARRAY"
                    },
                "name": "b"
                }
            ]
        }
        typeSize = hdf5dtype.getItemSize(typeItem)
        dt = hdf5dtype.createDataType(typeItem)   
        self.assertEqual(len(dt.fields), 2)
        self.assertTrue('a' in dt.fields.keys())
        self.assertTrue('b' in dt.fields.keys())
        self.assertEqual(typeSize, 11)



if __name__ == '__main__':
    #setup test files

    unittest.main()
