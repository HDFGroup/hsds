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

import weakref
import numpy as np


class Reference:
    """
    Represents an HDF5 object reference
    """

    @property
    def id(self):
        """Low-level identifier appropriate for this object"""
        return self._id

    @property
    def objref(self):
        """Weak reference to object"""
        return self._objref  # return weak ref to ref'd object

    def __init__(self, bind):
        """Create a new reference by binding to
        a group/dataset/committed type
        """
        self._id = bind._id
        self._objref = weakref.ref(bind)

    def __repr__(self):
        return "<HDF5 object reference>"

    def tolist(self):
        if type(self._id.id) is not str:
            raise TypeError("Expected string id")
        if self._id.objtype_code == "d":
            return [
                ("datasets/" + self._id.id),
            ]
        elif self._id.objtype_code == "g":
            return [
                ("groups/" + self._id.id),
            ]
        elif self._id.objtype_code == "t":
            return [
                ("datatypes/" + self._id.id),
            ]
        else:
            raise TypeError("Unexpected id type")


class RegionReference:
    """
    Represents an HDF5 region reference
    """

    @property
    def id(self):
        """Low-level identifier appropriate for this object"""
        return self._id

    @property
    def objref(self):
        """Weak reference to object"""
        return self._objref  # return weak ref to ref'd object

    def __init__(self, bind):
        """Create a new reference by binding to
        a group/dataset/committed type
        """
        self._id = bind._id
        self._objref = weakref.ref(bind)

    def __repr__(self):
        return "<HDF5 region reference>"


def special_dtype(**kwds):
    """Create a new h5py "special" type.  Only one keyword may be given.

    Legal keywords are:

    vlen = basetype
        Base type for HDF5 variable-length datatype. This can be Python
        str type or instance of np.dtype.
        Example: special_dtype( vlen=str )

    enum = (basetype, values_dict)
        Create a NumPy representation of an HDF5 enumerated type.  Provide
        a 2-tuple containing an (integer) base dtype and a dict mapping
        string names to integer values.

    ref = Reference | RegionReference
        Create a NumPy representation of an HDF5 object or region reference
        type."""

    if len(kwds) != 1:
        raise TypeError("Exactly one keyword may be provided")

    name, val = kwds.popitem()

    if name == "vlen":

        return np.dtype("O", metadata={"vlen": val})

    if name == "enum":

        try:
            dt, enum_vals = val
        except TypeError:
            msg = "Enums must be created from a 2-tuple "
            msg += "(basetype, values_dict)"
            raise TypeError(msg)

        dt = np.dtype(dt)
        if dt.kind not in "iu":
            raise TypeError("Only integer types can be used as enums")

        return np.dtype(dt, metadata={"enum": enum_vals})

    if name == "ref":
        dt = None
        if val is Reference:
            dt = np.dtype("S48", metadata={"ref": Reference})
        elif val is RegionReference:
            dt = np.dtype("S48", metadata={"ref": RegionReference})
        else:
            raise ValueError("Ref class must be Reference or RegionReference")

        return dt

    raise TypeError(f'Unknown special type "{name}"')


def check_dtype(**kwds):
    """Check a dtype for h5py special type "hint" information.  Only one
    keyword may be given.

    vlen = dtype
        If the dtype represents an HDF5 vlen, returns the Python base class.
        Currently only builting string vlens (str) are supported.  Returns
        None if the dtype does not represent an HDF5 vlen.

    enum = dtype
        If the dtype represents an HDF5 enumerated type, returns the dictionary
        mapping string names to integer values.  Returns None if the dtype does
        not represent an HDF5 enumerated type.

    ref = dtype
        If the dtype represents an HDF5 reference type, returns the reference
        class (either Reference or RegionReference).  Returns None if the dtype
        does not represent an HDF5 reference type.
    """

    if len(kwds) != 1:
        raise TypeError("Exactly one keyword may be provided")

    name, dt = kwds.popitem()

    if name not in ("vlen", "enum", "ref"):
        raise TypeError('Unknown special type "%s"' % name)

    try:
        return dt.metadata[name]
    except TypeError:
        return None
    except KeyError:
        return None


def getTypeResponse(typeItem):
    """
    Convert the given type item  to a predefined type string for
        predefined integer and floating point types ("H5T_STD_I64LE", et. al).
        For compound types, recursively iterate through the typeItem and do
        same conversion for fields of the compound type."""
    response = None
    if "uuid" in typeItem:
        # committed type, just return uuid
        response = "datatypes/" + typeItem["uuid"]
    elif typeItem["class"] in ("H5T_INTEGER", "H5T_FLOAT"):
        # just return the class and base for pre-defined types
        response = {}
        response["class"] = typeItem["class"]
        response["base"] = typeItem["base"]
    elif typeItem["class"] == "H5T_OPAQUE":
        response = {}
        response["class"] = "H5T_OPAQUE"
        response["size"] = typeItem["size"]
    elif typeItem["class"] == "H5T_REFERENCE":
        response = {}
        response["class"] = "H5T_REFERENCE"
        response["base"] = typeItem["base"]
    elif typeItem["class"] == "H5T_COMPOUND":
        response = {}
        response["class"] = "H5T_COMPOUND"
        fieldList = []
        for field in typeItem["fields"]:
            fieldItem = {}
            fieldItem["name"] = field["name"]
            fieldItem["type"] = getTypeResponse(field["type"])  # recurse call
            fieldList.append(fieldItem)
        response["fields"] = fieldList
    else:
        response = {}  # otherwise, return full type
        for k in typeItem.keys():
            if k == "base":
                if isinstance(typeItem[k], dict):
                    response[k] = getTypeResponse(typeItem[k])  # recurse call
                else:
                    response[k] = typeItem[k]  # predefined type
            elif k not in ("size", "base_size"):
                response[k] = typeItem[k]
    return response


def getTypeItem(dt, metadata=None):
    """
    Return type info.
          For primitive types, return string with typename
          For compound types return array of dictionary items
    """
    predefined_int_types = {
        "int8": "H5T_STD_I8",
        "uint8": "H5T_STD_U8",
        "int16": "H5T_STD_I16",
        "uint16": "H5T_STD_U16",
        "int32": "H5T_STD_I32",
        "uint32": "H5T_STD_U32",
        "int64": "H5T_STD_I64",
        "uint64": "H5T_STD_U64",
    }
    predefined_float_types = {
        "float16": "H5T_IEEE_F16",
        "float32": "H5T_IEEE_F32",
        "float64": "H5T_IEEE_F64",
    }
    # print(">getTypeItem:", dt.str)
    if not metadata and dt.metadata:
        metadata = dt.metadata
    # if metadata:
    #    print(">  metadata:", metadata)
    # if dt.shape:
    #    print(">  shape:", dt.shape)
    # if len(dt) > 1:
    #    print(">  len:", len(dt))

    type_info = {}
    if len(dt) > 1:
        # compound type
        names = dt.names
        type_info["class"] = "H5T_COMPOUND"
        fields = []
        for name in names:
            field = {"name": name}
            field["type"] = getTypeItem(dt[name])
            fields.append(field)
            type_info["fields"] = fields
    elif dt.shape:
        # array type
        if dt.base == dt:
            raise TypeError("Expected base type to be different than parent")
        # array type
        type_info["dims"] = dt.shape
        type_info["class"] = "H5T_ARRAY"
        # print(">  array type, metadata:", metadata)
        type_info["base"] = getTypeItem(dt.base, metadata=metadata)
    elif dt.kind == "O":
        # vlen string or data
        #
        # check for h5py variable length extension

        if metadata and "vlen" in metadata:
            vlen_check = metadata["vlen"]
            if vlen_check is not None and not isinstance(vlen_check, np.dtype):
                vlen_check = np.dtype(vlen_check)

        if metadata and "ref" in metadata:
            ref_check = metadata["ref"]
        else:
            ref_check = check_dtype(ref=dt.base)
        if vlen_check == bytes:
            type_info["class"] = "H5T_STRING"
            type_info["length"] = "H5T_VARIABLE"
            type_info["charSet"] = "H5T_CSET_ASCII"
            type_info["strPad"] = "H5T_STR_NULLTERM"
        elif vlen_check == str:
            type_info["class"] = "H5T_STRING"
            type_info["length"] = "H5T_VARIABLE"
            type_info["charSet"] = "H5T_CSET_UTF8"
            type_info["strPad"] = "H5T_STR_NULLTERM"
        elif isinstance(vlen_check, np.dtype):
            # vlen data
            type_info["class"] = "H5T_VLEN"
            type_info["size"] = "H5T_VARIABLE"
            type_info["base"] = getTypeItem(vlen_check)
        elif vlen_check is not None:
            #  unknown vlen type
            raise TypeError("Unknown h5py vlen type: " + str(vlen_check))
        elif ref_check is not None:
            # a reference type
            type_info["class"] = "H5T_REFERENCE"

            if ref_check is Reference:
                type_info["base"] = "H5T_STD_REF_OBJ"  # objref
            elif ref_check is RegionReference:
                type_info["base"] = "H5T_STD_REF_DSETREG"  # region ref
            else:
                raise TypeError("unexpected reference type")
        else:
            raise TypeError("unknown object type")
    elif dt.kind == "V":
        # void type
        type_info["class"] = "H5T_OPAQUE"
        type_info["size"] = dt.itemsize
        type_info["tag"] = ""  # todo - determine tag
    elif dt.base.kind == "S":
        # check for object reference
        ref_check = check_dtype(ref=dt.base)
        if ref_check is not None:
            # a reference type
            type_info["class"] = "H5T_REFERENCE"

            if ref_check is Reference:
                type_info["base"] = "H5T_STD_REF_OBJ"  # objref
            elif ref_check is RegionReference:
                type_info["base"] = "H5T_STD_REF_DSETREG"  # region ref
            else:
                raise TypeError("unexpected reference type")
        else:
            # Fixed length string type
            type_info["class"] = "H5T_STRING"
        type_info["length"] = dt.itemsize
        type_info["charSet"] = "H5T_CSET_ASCII"
        type_info["strPad"] = "H5T_STR_NULLPAD"
    elif dt.base.kind == "U":
        # Fixed length unicode type
        print("fixed UTF, itemsize:", dt.itemsize)
        ref_check = check_dtype(ref=dt.base)
        if ref_check is not None:
            raise TypeError("unexpected reference type")

        # Fixed length string type with unicode support
        type_info["class"] = "H5T_STRING"

        # this can be problematic if the encoding of the string is not valid,
        # or reqires too many bytes.  Use variable length strings to handle all
        # UTF8 strings correctly
        type_info["charSet"] = "H5T_CSET_UTF8"
        # convert from UTF32 length to a fixed length
        type_info["length"] = dt.itemsize
        type_info["strPad"] = "H5T_STR_NULLPAD"

    elif dt.kind == "b":
        # boolean type - h5py stores as enum
        # assume LE unless the numpy byteorder is '>'
        byteorder = "LE"
        if dt.base.byteorder == ">":
            byteorder = "BE"
        # this mapping is an h5py convention for boolean support
        mapping = {"FALSE": 0, "TRUE": 1}
        type_info["class"] = "H5T_ENUM"
        type_info["mapping"] = mapping
        base_info = {"class": "H5T_INTEGER"}
        base_info["base"] = "H5T_STD_I8" + byteorder
        type_info["base"] = base_info
    elif dt.kind == "f":
        # floating point type
        type_info["class"] = "H5T_FLOAT"
        byteorder = "LE"
        if dt.byteorder == ">":
            byteorder = "BE"
        if dt.name in predefined_float_types:
            # maps to one of the HDF5 predefined types
            float_type = predefined_float_types[dt.base.name]
            type_info["base"] = float_type + byteorder
        else:
            raise TypeError("Unexpected floating point type: " + dt.name)
    elif dt.kind == "i" or dt.kind == "u":
        # integer type

        # assume LE unless the numpy byteorder is '>'
        byteorder = "LE"
        if dt.base.byteorder == ">":
            byteorder = "BE"

        # numpy integer type - but check to see if this is the hypy
        # enum extension
        if metadata and "enum" in metadata:
            # yes, this is an enum!
            mapping = metadata["enum"]
            type_info["class"] = "H5T_ENUM"
            type_info["mapping"] = mapping
            if dt.name not in predefined_int_types:
                raise TypeError("Unexpected integer type: " + dt.name)
            # maps to one of the HDF5 predefined types
            base_info = {"class": "H5T_INTEGER"}
            base_info["base"] = predefined_int_types[dt.name] + byteorder
            type_info["base"] = base_info
        else:
            type_info["class"] = "H5T_INTEGER"
            base_name = dt.name

            if dt.name not in predefined_int_types:
                raise TypeError("Unexpected integer type: " + dt.name)

            type_info["base"] = predefined_int_types[base_name] + byteorder

    else:
        # unexpected kind
        raise TypeError(f"unexpected dtype kind: {dt.kind}")

    return type_info


def getItemSize(typeItem):
    """
    Get size of an item in bytes.
        For variable length types (e.g. variable length strings),
        return the string "H5T_VARIABLE"
    """
    # handle the case where we are passed a primitive type first
    if isinstance(typeItem, str) or isinstance(typeItem, bytes):
        for type_prefix in ("H5T_STD_I", "H5T_STD_U", "H5T_IEEE_F"):
            if typeItem.startswith(type_prefix):
                nlen = len(type_prefix)
                num_bits = typeItem[nlen:]
                if num_bits[-2:] in ("LE", "BE"):
                    num_bits = num_bits[:-2]
                try:
                    return int(num_bits) // 8
                except ValueError:
                    raise TypeError("Invalid Type")
        # none of the expect primative types mathched
        raise TypeError("Invalid Type")
    if not isinstance(typeItem, dict):
        raise TypeError("invalid type")

    item_size = 0
    if "class" not in typeItem:
        raise KeyError("'class' not provided")
    typeClass = typeItem["class"]

    if typeClass == "H5T_INTEGER":
        if "base" not in typeItem:
            raise KeyError("'base' not provided")
        item_size = getItemSize(typeItem["base"])

    elif typeClass == "H5T_FLOAT":
        if "base" not in typeItem:
            raise KeyError("'base' not provided")
        item_size = getItemSize(typeItem["base"])

    elif typeClass == "H5T_STRING":
        if "length" not in typeItem:
            raise KeyError("'length' not provided")
        item_size = typeItem["length"]

    elif typeClass == "H5T_VLEN":
        item_size = "H5T_VARIABLE"
    elif typeClass == "H5T_OPAQUE":
        if "size" not in typeItem:
            raise KeyError("'size' not provided")
        item_size = int(typeItem["size"])

    elif typeClass == "H5T_ARRAY":
        if "dims" not in typeItem:
            raise KeyError("'dims' must be provided for array types")
        if "base" not in typeItem:
            raise KeyError("'base' not provided")
        item_size = getItemSize(typeItem["base"])

    elif typeClass == "H5T_ENUM":
        if "base" not in typeItem:
            raise KeyError("'base' must be provided for enum types")
        item_size = getItemSize(typeItem["base"])

    elif typeClass == "H5T_REFERENCE":
        if "length" in typeItem:
            item_size = typeItem["length"]
        elif "base" in typeItem and typeItem["base"] == "H5T_STD_REF_OBJ":
            # obj ref values are in the form: "groups/<id>" or
            # "datasets/<id>" or "datatypes/<id>"
            item_size = 48
        else:
            raise KeyError("Unable to determine item size for reference type")
    elif typeClass == "H5T_COMPOUND":
        if "fields" not in typeItem:
            raise KeyError("'fields' not provided for compound type")
        fields = typeItem["fields"]
        if not isinstance(fields, list):
            raise TypeError("Type Error: expected list type for 'fields'")
        if not fields:
            raise KeyError("no 'field' elements provided")
        # add up the size of each sub-field
        for field in fields:
            if not isinstance(field, dict):
                raise TypeError("Expected dictionary type for field")
            if "type" not in field:
                raise KeyError("'type' missing from field")
            subtype_size = getItemSize(field["type"])  # recursive call
            if subtype_size == "H5T_VARIABLE":
                item_size = "H5T_VARIABLE"
                break  # don't need to look at the rest

            item_size += subtype_size
    else:
        raise TypeError("Invalid type class")

    # calculate array type
    if "dims" in typeItem and isinstance(item_size, int):
        dims = typeItem["dims"]
        for dim in dims:
            item_size *= dim

    return item_size


def getNumpyTypename(hdf5TypeName, typeClass=None):
    predefined_int_types = {
        "H5T_STD_I8": "i1",
        "H5T_STD_U8": "u1",
        "H5T_STD_I16": "i2",
        "H5T_STD_U16": "u2",
        "H5T_STD_I32": "i4",
        "H5T_STD_U32": "u4",
        "H5T_STD_I64": "i8",
        "H5T_STD_U64": "u8",
    }
    predefined_float_types = {
        "H5T_IEEE_F16": "f2",
        "H5T_IEEE_F32": "f4",
        "H5T_IEEE_F64": "f8",
    }

    if len(hdf5TypeName) < 3:
        raise Exception("Type Error: invalid typename: ")
    endian = "<"  # default endian
    key = hdf5TypeName
    if hdf5TypeName.endswith("LE"):
        key = hdf5TypeName[:-2]
    elif hdf5TypeName.endswith("BE"):
        key = hdf5TypeName[:-2]
        endian = ">"

    if key in predefined_int_types and (
        typeClass is None or typeClass == "H5T_INTEGER"
    ):
        return endian + predefined_int_types[key]
    if key in predefined_float_types and (
        typeClass is None or typeClass == "H5T_FLOAT"
    ):
        return endian + predefined_float_types[key]
    raise TypeError("Type Error: invalid type")


def createBaseDataType(typeItem):
    dtRet = None
    if isinstance(typeItem, str):
        # should be one of the predefined types
        dtName = getNumpyTypename(typeItem)
        dtRet = np.dtype(dtName)
        return dtRet  # return predefined type

    if not isinstance(typeItem, dict):
        raise TypeError("Type Error: invalid type")

    if "class" not in typeItem:
        raise KeyError("'class' not provided")
    typeClass = typeItem["class"]

    dims = ""
    if "dims" in typeItem:
        if typeClass != "H5T_ARRAY":
            raise TypeError("'dims' only supported for integer types")

        dims = None
        if isinstance(typeItem["dims"], int):
            dims = typeItem["dims"]  # make into a tuple
        elif not isinstance(typeItem["dims"], list) and not isinstance(
            typeItem["dims"], tuple
        ):
            raise TypeError("expected list or integer for dims")
        else:
            dims = typeItem["dims"]
        dims = str(tuple(dims))

    if typeClass == "H5T_INTEGER":
        if "base" not in typeItem:
            raise KeyError("'base' not provided")
        baseType = getNumpyTypename(typeItem["base"], typeClass="H5T_INTEGER")
        dtRet = np.dtype(dims + baseType)
    elif typeClass == "H5T_FLOAT":
        if "base" not in typeItem:
            raise KeyError("'base' not provided")
        baseType = getNumpyTypename(typeItem["base"], typeClass="H5T_FLOAT")
        dtRet = np.dtype(dims + baseType)
    elif typeClass == "H5T_STRING":
        if "length" not in typeItem:
            raise KeyError("'length' not provided")
        if "charSet" not in typeItem:
            raise KeyError("'charSet' not provided")

        if typeItem["length"] == "H5T_VARIABLE":
            if dims:
                msg = "ArrayType is not supported for variable len types"
                raise TypeError(msg)
            if typeItem["charSet"] == "H5T_CSET_ASCII":
                dtRet = special_dtype(vlen=bytes)
            elif typeItem["charSet"] == "H5T_CSET_UTF8":
                dtRet = special_dtype(vlen=str)
            else:
                raise TypeError("unexpected 'charSet' value")
        else:
            nStrSize = typeItem["length"]
            if not isinstance(nStrSize, int):
                raise TypeError("expecting integer value for 'length'")
            type_code = None
            if typeItem["charSet"] == "H5T_CSET_ASCII":
                type_code = "S"
            elif typeItem["charSet"] == "H5T_CSET_UTF8":
                # use the same type_code as ascii strings
                # (othewise, numpy will reserve bytes for UTF32 representation)
                type_code = "S"
            else:
                raise TypeError("unexpected 'charSet' value")
            # a fixed size string
            dtRet = np.dtype(dims + type_code + str(nStrSize))
    elif typeClass == "H5T_VLEN":
        if dims:
            msg = "ArrayType is not supported for variable len types"
            raise TypeError(msg)
        if "base" not in typeItem:
            raise KeyError("'base' not provided")
        baseType = createBaseDataType(typeItem["base"])
        dtRet = special_dtype(vlen=np.dtype(baseType))
    elif typeClass == "H5T_OPAQUE":
        if dims:
            msg = "Opaque Type is not supported for variable len types"
            raise TypeError(msg)
        if "size" not in typeItem:
            raise KeyError("'size' not provided")
        nSize = int(typeItem["size"])
        if nSize <= 0:
            raise TypeError("'size' must be non-negative")
        dtRet = np.dtype("V" + str(nSize))
    elif typeClass == "H5T_ARRAY":
        if not dims:
            raise KeyError("'dims' must be provided for array types")
        if "base" not in typeItem:
            raise KeyError("'base' not provided")
        arrayBaseType = typeItem["base"]
        if isinstance(arrayBaseType, dict):
            if "class" not in arrayBaseType:
                raise KeyError("'class' not provided for array base type")
            type_classes = ("H5T_INTEGER", "H5T_FLOAT", "H5T_STRING")
            if arrayBaseType["class"] not in type_classes:
                msg = "Array Type base type must be integer, float, or string"
                raise TypeError(msg)
        baseType = createDataType(arrayBaseType)
        metadata = None
        if baseType.metadata:
            metadata = dict(baseType.metadata)
            dtRet = np.dtype(dims + baseType.str, metadata=metadata)
        else:
            dtRet = np.dtype(dims + baseType.str)
        return dtRet  # return predefined type
    elif typeClass == "H5T_REFERENCE":
        if "base" not in typeItem:
            raise KeyError("'base' not provided")
        if typeItem["base"] == "H5T_STD_REF_OBJ":
            dtRet = special_dtype(ref=Reference)
        elif typeItem["base"] == "H5T_STD_REF_DSETREG":
            dtRet = special_dtype(ref=RegionReference)
        else:
            raise TypeError("Invalid base type for reference type")

    elif typeClass == "H5T_ENUM":
        if "base" not in typeItem:
            raise KeyError("Expected 'base' to be provided for enum type")
        base_json = typeItem["base"]
        if "class" not in base_json:
            raise KeyError("Expected class field in base type")
        if base_json["class"] != "H5T_INTEGER":
            msg = "Only integer base types can be used with enum type"
            raise TypeError(msg)
        if "mapping" not in typeItem:
            raise KeyError("'mapping' not provided for enum type")
        mapping = typeItem["mapping"]
        if len(mapping) == 0:
            raise KeyError("empty enum map")

        dt = createBaseDataType(base_json)
        if all(
            (
                dt.kind == "i",
                dt.name == "int8",
                len(mapping) == 2,
                "TRUE" in mapping,
                "FALSE" in mapping,
            )
        ):
            # convert to numpy boolean type
            dtRet = np.dtype("bool")
        else:
            # not a boolean enum, use h5py special dtype
            dtRet = special_dtype(enum=(dt, mapping))

    else:
        raise TypeError("Invalid type class")

    return dtRet


def createDataType(typeItem):
    """
    Create a numpy datatype given a json type
    """
    dtRet = None
    if type(typeItem) in (str, bytes):
        # should be one of the predefined types
        dtName = getNumpyTypename(typeItem)
        dtRet = np.dtype(dtName)
        return dtRet  # return predefined type

    if not isinstance(typeItem, dict):
        raise TypeError("invalid type")

    if "class" not in typeItem:
        raise KeyError("'class' not provided")
    typeClass = typeItem["class"]

    if typeClass == "H5T_COMPOUND":
        if "fields" not in typeItem:
            raise KeyError("'fields' not provided for compound type")
        fields = typeItem["fields"]
        if type(fields) is not list:
            raise TypeError("Type Error: expected list type for 'fields'")
        if not fields:
            raise KeyError("no 'field' elements provided")
        subtypes = []
        for field in fields:

            if not isinstance(field, dict):
                raise TypeError("Expected dictionary type for field")
            if "name" not in field:
                raise KeyError("'name' missing from field")
            if "type" not in field:
                raise KeyError("'type' missing from field")
            field_name = field["name"]
            if not isinstance(field_name, str):
                raise TypeError("field names must be strings")
            # verify the field name is ascii
            try:
                field_name.encode("ascii")
            except UnicodeEncodeError:
                raise TypeError("non-ascii field name not allowed")

            dt = createDataType(field["type"])  # recursive call
            if dt is None:
                raise Exception("unexpected error")
            subtypes.append((field["name"], dt))  # append tuple

        dtRet = np.dtype(subtypes)
    else:
        dtRet = createBaseDataType(typeItem)  # create non-compound dt
    return dtRet


def validateTypeItem(typeItem):
    """
    Validate a json type - call createDataType and if no exception,
       it's valid
    """
    createDataType(typeItem)
    # throws KeyError, TypeError, or ValueError


def getBaseTypeJson(type_name):
    """
    Return JSON representation of a predefined type string
    """
    predefined_int_types = (
        "H5T_STD_I8",
        "H5T_STD_U8",
        "H5T_STD_I16",
        "H5T_STD_U16",
        "H5T_STD_I32",
        "H5T_STD_U32",
        "H5T_STD_I64",
        "H5T_STD_U64",
    )
    predefined_float_types = ("H5T_IEEE_F16", "H5T_IEEE_F32", "H5T_IEEE_F64")
    type_json = {}
    # predefined typenames start with 'H5T' and end with "LE" or "BE"
    if all(
        (
            type_name.startswith("H5T_"),
            type_name[-1] == "E",
            type_name[-2] in ("L", "B"),
        )
    ):
        # trime of the "BE/"LE"
        type_prefix = type_name[:-2]
        if type_prefix in predefined_int_types:
            type_json["class"] = "H5T_INTEGER"
            type_json["base"] = type_name
        elif type_prefix in predefined_float_types:
            type_json["class"] = "H5T_FLOAT"
            type_json["base"] = type_name
        else:
            raise TypeError("Invalid type name")
    else:
        raise TypeError("Invalid type name")
    return type_json
