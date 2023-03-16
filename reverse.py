import io
import pathlib
from typing import Set, Dict

import google.protobuf.descriptor_pb2
from google.protobuf.descriptor import FileDescriptor

TYPE_DOUBLE = 1
TYPE_FLOAT = 2
TYPE_INT64 = 3
TYPE_UINT64 = 4
TYPE_INT32 = 5
TYPE_FIXED64 = 6
TYPE_FIXED32 = 7
TYPE_BOOL = 8
TYPE_STRING = 9
TYPE_GROUP = 10
TYPE_MESSAGE = 11
TYPE_BYTES = 12
TYPE_UINT32 = 13
TYPE_ENUM = 14
TYPE_SFIXED32 = 15
TYPE_SFIXED64 = 16
TYPE_SINT32 = 17
TYPE_SINT64 = 18

TYPE_LABELS = {
    TYPE_DOUBLE: "double",
    TYPE_FLOAT: "float",
    TYPE_INT64: "int64",
    TYPE_UINT64: "uint64",
    TYPE_INT32: "int32",
    TYPE_FIXED64: "fixed64",
    TYPE_FIXED32: "fixed32",
    TYPE_BOOL: "bool",
    TYPE_STRING: "string",
    TYPE_BYTES: "byes",
    TYPE_UINT32: "uint32",
    TYPE_SFIXED32: "sfixed32",
    TYPE_SFIXED64: "sfixed64",
    TYPE_SINT32: "sint32",
    TYPE_SINT64: "sint64",
}


def __generate_field(proto: io.TextIOBase,
                   field: google.protobuf.descriptor_pb2.FieldDescriptorProto,
                   map_entries: Dict[str, str],
                   level: str):
    type_str = __extract_field_type_str(field)
    is_map = False
    if type_str in map_entries:
        type_str = map_entries[type_str]
        is_map = True

    options = {}
    if field.options.packed:
        options['packed'] = "true"

    if field.label == field.LABEL_REPEATED and is_map == False:
        repeat = "repeated "
    else:
        repeat = ""

    proto.write(f"{level}{repeat}{type_str} {field.name} = {field.number}")

    if len(options) > 0:
        proto.write("[")
        sep = ""
        for o in field.options:
            v = field.options[o]
            proto.write(f"{o} = {v}{sep}")
            sep = ","
        proto.write("]")

    proto.write(";\n")


def __extract_field_type_str(field):
    if field.type == TYPE_ENUM or field.type == TYPE_MESSAGE:
        type_str = field.type_name.split(sep=".")[-1]
    elif field.type == TYPE_GROUP:
        raise NameError("GROUP")
    else:
        type_str = TYPE_LABELS[field.type]
    return type_str


def __generate_enum(proto: io.TextIOBase,
                  enum: google.protobuf.descriptor_pb2.DescriptorProto,
                  level: str = "",
                  ) -> None:
    proto.write(f"{level}enum {enum.name} {{\n")
    if enum.options.allow_alias:
        proto.write(f"{level} option allow_alias = true;\n")
    for v in enum.value:
        proto.write(f"{level}  {v.name} = {v.number};\n")
    proto.write(f"{level}}}\n")

    return


def __extract_map_shortcut(
        m: google.protobuf.descriptor_pb2.DescriptorProto) -> str:
    k = __extract_field_type_str(m.field[0])
    v = __extract_field_type_str(m.field[1])
    return f"map<{k},{v}>"


def __generate_message(proto: io.TextIOBase,
                       msg: google.protobuf.descriptor_pb2.DescriptorProto,
                       level: str = "",
                       ) -> None:
    proto.write(f"{level}message {msg.name}\n")
    proto.write(f"{level}{{\n")

    map_entries = {}
    for m in msg.nested_type:
        if m.options.map_entry:
            map_entries[m.name] = __extract_map_shortcut(m)
        else:
            __generate_message(proto, m, level + "  ")

    for e in msg.enum_type:
        __generate_enum(proto, e, level + "  ")

    for f in msg.field:
        __generate_field(proto, f, map_entries, level + "  ")

    proto.write(f"{level}}}\n")


def __generate_import(proto: io.TextIOBase, dep: str,
                      current_dir: pathlib.Path) -> None:
    if dep.startswith("google"):
        return

    up = ""
    target_file = pathlib.Path(f"protobuf/{dep}")
    current = current_dir
    while not target_file.is_relative_to(current):
        up += "../"
        current = current.parent

    rel = target_file.relative_to(current)
    proto.write(
        f"import \"{up}{rel}\";\n")  ## don't want to deal with path rewrites


def __generate_rpc_method(proto: io.TextIOBase, level: str,
                          m: google.protobuf.descriptor_pb2.MethodDescriptorProto) -> None:
    in_mode = "stream " if m.client_streaming else ""
    in_type = m.input_type.split(sep=".")[-1]

    ret_mode = "stream " if m.server_streaming else ""
    ret_type = m.output_type.split(sep=".")[-1]
    proto.write(
        f"{level}rpc {m.name} ({in_mode}{in_type}) returns ({ret_mode}{ret_type});\n")


def __generate_service(proto: io.TextIOBase,
                       s: google.protobuf.descriptor_pb2.ServiceDescriptorProto) -> None:
    proto.write(f"service {s.name} {{\n")
    for m in s.method:
        __generate_rpc_method(proto, "  ", m)
    proto.write(f"}}\n")


def __generate_file(data):
    fds = google.protobuf.descriptor_pb2.FileDescriptorSet()
    file = google.protobuf.descriptor_pb2.FileDescriptorProto()
    file.ParseFromString(data)
    fds.file.append(file)

    print(str(file))

    target_path = pathlib.Path(f"protobuf/{file.name}")

    if not target_path.parent.exists():
        target_path.parent.mkdir(exist_ok=True, parents=True)

    with open(target_path, 'w') as proto:
        # header
        proto.write(f"syntax = \"{file.syntax}\";\n")

        if len(file.package) > 0:
            proto.write(f"package {file.package};\n")

        for d in file.dependency:
            __generate_import(proto, d, target_path.parent)

        for s in file.service:
            __generate_service(proto, s)

        for x in file.enum_type:
            __generate_enum(proto, x)

        for x in file.message_type:
            __generate_message(proto, x)


def __reverse_grpc_descriptor(descriptor: FileDescriptor, processed: Set[str]) -> None:
    """"""
    if descriptor.name in processed:
        return

    for d in descriptor.dependencies:
        __reverse_grpc_descriptor(d, processed)

    __generate_file(descriptor.serialized_pb)
    processed.add(descriptor.name)


def reverse_descriptor(root_descriptor: FileDescriptor):
    """Reverse the descriptor for a GRPC library to stdout"""
    __reverse_grpc_descriptor(root_descriptor, set([]))

