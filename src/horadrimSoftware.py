import mmap
from os import getcwd
from re import X

from regex import W
from bplustree.bplustree import BPlusTree
import sys
from os import path, remove
import csv
import time
sys.path.append('./bplustree/bplustree')

current_path = getcwd()
PAGE_SIZE = 2048  # bytes (2 KB)
FILE_SIZE = 104857600  # bytes
PAGE_HEADER_SIZE = 128  # bytes
RECORD_SIZE = 240  # bytes (8 record per page)
# 8000 pages per file, first page reserved for file header
# to convert byte int to int, use int(byte)
# indexing for page and record positions in bplustree value encoding starts from 0
# File header size is a page size


# catalog_for_relations         0:20 --> relation name    21:23 --> primary key index

# tree value insertion happens as such:
# 24-->.rec file name     4-->inserted_page_number   1-->record_index_at_page
# nuances: for any parsed value, space is inserted at the beggining part. Also, in page header the index seaarch is from left to right
# except for headers, they have spaces at the end

# CHECK IF KEYS ARE PASSED AS STRING ALWAYS

catalog_for_relations_path = current_path + "/relation_metadata.txt"
if_catalog_exist = path.exists(catalog_for_relations_path)

catalog_for_relations = open(catalog_for_relations_path, "a+b")


# Horadrim definition language operations

# resizes the byte object
def format_byte(byte_object, ln):
    l = len(byte_object)
    l = ln - l
    return (b' '*l) + byte_object

# turns the integer to its exact byte representation


def format_string(stri, l):
    return (stri + " "*(l-len(stri)))


def intoa(i):
    t = check_type(i)
    if t == "int":
        return int(i)
    else:
        return i


def int_to_byte(integer, ln):
    byt = str(integer).encode()
    return format_byte(byt, ln)

# turns the string to its exact byte representation


def str_to_byte(string, ln):
    byt = string.encode()
    return format_byte(byt, ln)


if if_catalog_exist == False:
    wr = "{:<20} {:<2}\n".format(
        'relation_name', 'primary_key_index')
    catalog_for_relations.write(str_to_byte(wr, 24))  # 100 B

# naming convention for overlflow files is adding a V at the end of previous file name, not yet entegrated to the system


def overflow_file_name(name):
    return name[:-4] + "V" + ".rec"

# used to check if a record is okay as size I think??


def size_fit(array):
    for i in array:
        if (len(i) > 20) | (len(i) < 12):
            return False
    return True

# creates .rec, .db files and inserts file header to .rec file
# returns True if no error happens, otherwise returns False


def create_type(type_name, index_of_primary_key):
    try:
        # created a btree for the relation
        index_tree_path = current_path + "/" + type_name + ".db"
        tree = BPlusTree(index_tree_path, order=50)  # creating the index files
        tree.close()

        # created a data record file below
        file_path = current_path + "/" + type_name + ".rec"
        with open(file_path, "a+b") as file:
            file.write(b" "*FILE_SIZE)
            file.flush()
            mm = mmap.mmap(file.fileno(), FILE_SIZE, tagname=type_name)
            file_type = format_byte(b'relation_records$', 17)  # 17      1:17
            primary_key_field_number = int_to_byte(
                index_of_primary_key, 2) + b'$'  # 3     18:20
            unused_file_size = int_to_byte(
                FILE_SIZE, 10) + b'$'  # 11     21:31

            file_header = b'$' + file_type + primary_key_field_number + unused_file_size  # 32
            mm[:32] = file_header
            # first page is fully reserved for file header
            mm[32:2048] = b' '*2016
            mm.close()
            file.flush()
            return True
    except:
        return False

# returns the primary key index AS INTEGER TYPE WHICH STARTS FROM 1 ENDS AT WORST 12 if relation exists, otherwise returns False


def exists_in_system(relation_name):
    catalog_for_relations.seek(0)
    for rela in catalog_for_relations:
        index = rela[-3:-1]
        rela = rela[0:20].decode()
        rela = rela[0:rela.find(" ")]
        if rela == relation_name:
            catalog_for_relations.seek(0)
            return int(index)
    catalog_for_relations.seek(0)
    return False

# look if string is alphanumeric or just integer; return str, int or False


def check_type(string):
    if string.isnumeric():
        return "int"
    elif string.isalnum():
        return "str"
    else:
        return False

# doesn't check if such path exists for type_names. Adds the record to .rec file and does the necessary updates on it


def insert_record(type_name, fields, index_of_key):  # string, string, int
    """
    + check for validation of field types (str-int) by first extracting from system_catalog
    + find an empty place in records by traversing page by page and insert by using mmap and put the record there
    + add to index tree
    """
    record_path = current_path + "/" + type_name + ".rec"
    page_inserted = 0
    record_index_at_page = 0
    with open(record_path, "a+b") as file:
        mm = mmap.mmap(file.fileno(), 0)
        mm.read(2048)
        c = 0
        while(True):
            c += 1
            page_beggining_pointer = mm.tell()
            page = mm.read(2048)
            filled_record_data = page[0:3]
            try:  # page has been reached before
                if int(filled_record_data.decode()) == 255:
                    continue
                else:
                    dlbs = bin(int(filled_record_data.decode()))
                    index = 0
                    fl = True
                    for rec in dlbs[2:]:
                        if (rec == '0') & fl:
                            temp = dlbs
                            record_index_at_page = index
                            dlbs = dlbs[0:index+2] + "1"
                            if index != 7:
                                dlbs += temp[index+3:]
                            fl = False
                        index += 1
                    # below I reduce marked the record position as filled

                    mm[page_beggining_pointer:page_beggining_pointer +
                        3] = int_to_byte(int(dlbs, 2), 3)
                    page_inserted = c
                    record = ""
                    for field in fields:
                        record += "{:<20}".format(field)
                    record += " "*(20*(12-len(fields)))
                    kay = page_beggining_pointer + PAGE_HEADER_SIZE + record_index_at_page*RECORD_SIZE
                    mm[kay: kay +
                        RECORD_SIZE] = str_to_byte(record, RECORD_SIZE)
                    # below I reduce unused file size
                    mm[21:31] = int_to_byte(int(mm[21:31])-240, 10)
                    break
            except:  # page header doesn't exist yet
                # error while insert 217th line
                mm.write(b" "*2048)
                mm[page_beggining_pointer:page_beggining_pointer +
                    128] = b"128" + b" "*125
                record_index_at_page = 0
                # below I reduce marked the record position as filled
                page_inserted = c
                mm.seek(page_beggining_pointer +
                        PAGE_HEADER_SIZE + record_index_at_page*RECORD_SIZE)
                record = ""
                for field in fields:
                    record += "{:<20}".format(field)
                record += " "*(20*(12-len(fields)))
                mm[page_beggining_pointer+PAGE_HEADER_SIZE:page_beggining_pointer +
                    RECORD_SIZE+PAGE_HEADER_SIZE] = str_to_byte(record, 240)
                # below I reduce unused file size
                mm[21:31] = int_to_byte(int(mm[21:31])-240, 10)
                break
        mm.close()
        index_tree_path = current_path + "/" + type_name + ".db"
        tree = BPlusTree(index_tree_path, order=50)  # creating the index files
        value = str_to_byte(type_name + ".rec", 24) + int_to_byte(
            page_inserted, 4) + int_to_byte(record_index_at_page, 1)
        tree.insert(fields[index_of_key-1], value)
        tree.close()


def main():
    # the lines are in string format
    instruction_file = open(current_path + "/" + sys.argv[1], mode='a')
    instruction_file.write("\n")
    instruction_file.close()
    instruction_file = open(current_path + "/" + sys.argv[1], mode='r')
    output_file_path = current_path + "/" + sys.argv[2]
    log_file_path = current_path + "/horadrim-Log.csv"
    system_catalog_path = current_path + "/system_catalog.txt"

    # will write in byte format
    log_file = open(log_file_path, "a+")
    # to write in .csv format
    writer = csv.writer(log_file, lineterminator='\n')
    output_file = open(output_file_path, "a+b")

    path_exist = path.exists(system_catalog_path)
    # creating system catalog file
    # will have to write in byte format
    system_catalog = open(system_catalog_path, "a+b")
    if path_exist == False:
        u = "{:<20} {:<20} {:<20} {:<5} {:<2} \n".format(
            'field_name', 'field_type', 'relation', 'p_key', 'pt')
        system_catalog.write(str_to_byte(u, 73))
        # 100 B #the last two are primary key and position

    """
    field_name --> 0:20
    field_type --> 21:41
    relation --> 42:62
    primary_key --> 63:68
    position --> 69:71
    """

    log_file.flush()
    system_catalog.flush()
    output_file.flush()
    catalog_for_relations.flush()

    Lines = instruction_file.readlines()
    for line in Lines:  # reading all instructions
        elements = line.split()
        if len(elements) == 0:
            continue
        if elements[0] == "create":
            if elements[1] == "type":  # creating a type
                """
                    *** create type <type-name><number-of-fields><primary-key-order><field1-name><field1-type><field2-name>...
                    + create a btree file
                    + create a file
                    + create entry in system catalog
                    + add log info
                    + add to relation metadata table
                    #ERRORS:
                    + creating an existing type
                    + type with 6 <= num_of_fields <= 12, 12 <= name_of_fields <= 20
                    + 1 <= index_of_primary_key <= num_of_fields
                    + type of fields str or int
                    + name of a field in the relation should be alphanumeric
                """
                # check if type other than str or int is assigned
                if (elements[6::2].count("str") + elements[6::2].count("int")) != len(elements[6::2]):
                    log = [int_to_byte(int(time.time())), line, "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue
                # check if type already exists
                if exists_in_system(elements[2]) == True:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue
                # check for some other restrictions
                # BEWARE the index for orimary key starts from one
                if (not (1 <= int(elements[3]) <= 12)) | size_fit(elements[5::2]) | (not(1 <= int(elements[4]) <= int(elements[3]))):
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue
                # create type(type_name, index_of_primary_key)
                flag_q = False
                if create_type(elements[2], elements[4]):
                    for i in range(3, int(elements[3])+3):
                        is_pk = (((i-2)) == int(elements[4]))
                        put = " "*5
                        if is_pk:
                            put = "True "
                        else:
                            put = "False"
                        demet = i
                        demet = "0"+str(demet)
                        demet = demet[-2:]
                        # below
                        if not elements[2*i-1].isalnum():
                            log = [int(time.time()), line[:-1], "failure"]
                            writer.writerow(log)  # !!here may come a new line
                            flag_q = True
                            break
                        sys_cat = "{:<20} {:<20} {:<20} {:<5} {:<2} \n".format(
                            format_string(elements[2*i-1], 20), format_string(elements[2*i], 20), format_string(elements[2], 20), put, demet)
                        # BEWARE TRUE IS PROBLEMATIC HERE
                        system_catalog.write(str_to_byte(sys_cat, 73))

                    if flag_q:
                        continue
                    cfr = "{:<20} {:<2}\n".format(elements[2], elements[4])
                    catalog_for_relations.write(str_to_byte(cfr, 24))
                    log = [int(time.time()), line[:-1], "success"]
                    writer.writerow(log)  # !!here may come a new line
            else:  # creating a record
                """
                    **** create record <type-name><field1-value><field2-value>...
                    - add to record file
                    - add to btree index
                    #ERROR
                    + creating a record with an already existing primary key
                    + adding a record of a non-existing type (delete type from catalog_for_relations while traversing this)
                """
                # below I

                type_name = elements[2]
                # if such type doesn't exist error
                e = exists_in_system(type_name)
                if e == False:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue

                # checks if trying to add duplicate primary key
                index_tree_path = current_path + "/" + type_name + ".db"
                # creating the index files
                tree = BPlusTree(index_tree_path, order=50)
                if tree.__contains__(elements[e+2]):
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    tree.close()
                    continue
                tree.close()
                system_catalog.seek(0)
                catalog = system_catalog.readlines()
                system_catalog.seek(0)
                flag = False
                types = []
                error = False
                count = 0
                catalog = catalog[1:]
                for row in catalog:
                    if row[42:62].decode() == (type_name + " "*(20-len(elements[2]))):
                        if check_type(elements[3+count]) != row[21:24].decode():
                            log = [int(time.time()), line[:-1], "failure"]
                            writer.writerow(log)  # !!here may come a new line
                            error = True
                            break
                        count += 1
                        flag = True
                        continue
                    if flag:
                        break
                if error:
                    continue
                # inserts record to .rec file
                insert_record(elements[2], elements[3:], e)
                log = [int(time.time()), line[:-1], "success"]
                writer.writerow(log)  # !!here may come a new line
        elif elements[0] == "delete":
            if elements[1] == "type":
                """
                    + delete record files
                    + delete btree for type
                    + delete from system catalog
                    + delete from relation metadata
                    #ERRORS:
                    + deleting a non existing type
                """
                # below I check if the type exists in database and delete the fields from system catalog if exists
                catalog = system_catalog.readlines()
                have_found_relation = False
                for row in catalog:
                    if row[42:62] == elements[2] + " "*(20-len(elements[2])):
                        have_found_relation = True
                        catalog.remove[row]
                        continue
                    if have_found_relation:
                        system_catalog.seek(0)
                        break
                if have_found_relation:
                    with open(system_catalog_path, "w") as updated:
                        for row in catalog:
                            updated.write(row)
                else:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue

                # below I delete bplustree and record files
                type_file_path = current_path + "/" + elements[2] + ".rec"
                bplustree_file_path = current_path + "/" + elements[2] + ".db"

                if path.exists(type_file_path):
                    remove(type_file_path)
                else:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue

                if path.exists(bplustree_file_path):
                    remove(bplustree_file_path)
                else:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue

                log = [int(time.time()), line[:-1], "success"]
                writer.writerow(log)  # !!here may come a new line
                # below I delete from relations metadata
                catalog_for_relations.seek(0)
                h = catalog_for_relations.readlines()
                for rel in h:
                    if rel[:20].decode() == elements[2] + " "*(20-len(elements[2])):
                        h.remove(rel)
                with open(catalog_for_relations, "w") as updated:
                    for rel in h:
                        updated.write(rel)
            else:  # delete record
                """
                    *** delete record <type-name><primary-key>
                    + find the adress of it from bplustree and delete from .rec file
                    + delete the key-value pair from bplustree
                    #ERRORS:
                    + deleting a record with a non-existing primary key
                    + deleting a record of a non-existing type
                """
                # 2 type name    3 primary key value
                type_name = elements[2]
                if exists_in_system(elements[2]) == False:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue

                index_tree_path = current_path + "/" + type_name + ".db"
                # creating the index files
                tree = BPlusTree(index_tree_path, order=50)

                # error handling for nonexisting primary key
                if tree.__contains__(elements[3]) == False:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    tree.close()
                    continue

                value = tree.get(elements[3])
                page_of_record = int(value[24:28])
                nth_record = int(value[28:29])

                with open(current_path + "/" + elements[2] + ".rec", "a+b") as file:
                    mm = mmap.mmap(file.fileno(), 0)
                    mm.seek(PAGE_SIZE*page_of_record +
                            nth_record*RECORD_SIZE+PAGE_HEADER_SIZE)
                    t = mm.tell()
                    # could have made empty string here
                    # below im deleting from .rec file
                    mm[t:t+240] = b" "*240
                    mm[21:31] = int_to_byte(
                        int(mm[21:31]) + 240, 10)

                    # below im deleting from page header
                    b = bin(
                        int(mm[PAGE_SIZE*page_of_record:PAGE_SIZE*page_of_record + 3]))
                    b = '0'*(20-len(b[2:])) + b[2:]
                    b = b[0:nth_record] + "0"
                    if nth_record != (len(b)-2):
                        b += b[nth_record+1:]
                    mm[(PAGE_SIZE*page_of_record):(PAGE_SIZE *
                                                   page_of_record + 3)] = int_to_byte(int(b), 3)
                    mm.close()

                # below im deleting data from bplustree
                node = tree._search_in_tree(elements[3], tree._root_node)
                node.remove_entry(elements[3])
                tree.close()
                log = [int(time.time()), line[:-1], "success"]
                writer.writerow(log)  # !!here may come a new line
        elif elements[0] == "list":
            if elements[1] == "type":  # listing record types
                try:
                    relations_list = []
                    catalog_for_relations.seek(0)
                    g = catalog_for_relations.readlines()
                    for gs in g:
                        relations_list.append(gs[0:20])
                    relations_list = relations_list[1:]
                    relations_list.sort()
                    for i in relations_list:
                        i = i.decode()
                        output_file.write(i[0:i.find(" ")].encode() + b"\n")
                    log = [int(time.time()), line[:-1], "success"]
                    writer.writerow(log)  # !!here may come a new line
                except:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
            else:  # listing record names
                """
                *** list record <type-name>
                #ERROR
                + calling to list a type that doesn't exist will give error log
                + no type instance existing yet
                """
                type_name = elements[2]
                index_tree_path = current_path + "/" + type_name + ".db"
                # creating the index files
                if exists_in_system(type_name) == False:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue

                tree = BPlusTree(index_tree_path, order=50)
                value_list = list(tree.values())
                tree.close()
                if len(value_list) == 0:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    continue
                with open(current_path+"/"+type_name + ".rec", "a+b") as f:
                    print_to_out = ""
                    mm = mmap.mmap(f.fileno(), 0)
                    for val in value_list:
                        index = PAGE_SIZE * \
                            int(val[24:28]) + int(val[28:29]) * \
                            RECORD_SIZE + PAGE_HEADER_SIZE
                        record = mm[index:index+RECORD_SIZE]
                        for s in range(0, 12):
                            sub = record[s*20:(s+1)*20]
                            sub = sub.decode()
                            if sub != " "*20:
                                yr = sub.find(" ")
                                if yr != -1:
                                    sub = sub[0:yr]
                                # this could create problem when a field's value is actually empty string
                                print_to_out += sub
                                print_to_out += " "

                        print_to_out = print_to_out[:-1]
                        print_to_out += "\n"
                        output_file.write(print_to_out.encode())  # !!%%
                        print_to_out = ""
                    mm.close()

                    log = [int(time.time()), line[:-1], "success"]
                    writer.writerow(log)  # !!here may come a new line
        elif elements[0] == "update":
            """
            ** update record <type-name><primary-key><field1-value><field2-value>...
            #ERROR
            + if the given primary key doesn't get along with given primary key in update
            """
            type_name = elements[2]
            primary_key_value = elements[3]

            index_tree_path = current_path + "/" + type_name + ".db"
            # creating the index files
            p_k_i = exists_in_system(type_name)

            if p_k_i == False:
                log = [int(time.time()), line[:-1], "failure"]
                writer.writerow(log)  # !!here may come a new line
                continue

            tree = BPlusTree(index_tree_path, order=50)
            if tree.__contains__(primary_key_value) == False:
                log = [int(time.time()), line[:-1], "failure"]
                writer.writerow(log)  # !!here may come a new line
                tree.close()
                continue
            value = tree.get(primary_key_value)
            tree.close()

            index = PAGE_SIZE * \
                int(value[24:28]) + int(value[28:29]) * \
                RECORD_SIZE + PAGE_HEADER_SIZE

            record = ""

            for e in elements[4:]:
                record += (e + " "*(20 - len(e)))

            record += " "*(20*(12 - len(elements[4:])))

            with open(current_path+"/"+type_name + ".rec", "a+b") as f:
                mm = mmap.mmap(f.fileno(), 0)
                temp_rec = mm[index:index+RECORD_SIZE]
                temp_rec = temp_rec[20*(p_k_i - 1):20*p_k_i]
                if elements[3+p_k_i] != str(temp_rec[:len(elements[3+p_k_i])])[2:-1]:
                    log = [int(time.time()), line[:-1], "failure"]
                    writer.writerow(log)  # !!here may come a new line
                    mm.close()
                    print(elements[3+p_k_i])
                    print(temp_rec[:len(elements[3+p_k_i])])
                    print("failed")
                    continue
                mm[index:index+RECORD_SIZE] = str_to_byte(record, RECORD_SIZE)
                mm.close()

            log = [int(time.time()), line[:-1], "success"]
            writer.writerow(log)  # !!here may come a new line
        elif elements[0] == "search":
            """
                search record <type-name><primary-key>
                #ERROR
                + if such type does't exist
            """
            type_name = elements[2]
            primary_key_value = elements[3]
            pk = exists_in_system(type_name)
            if pk == False:
                log = [int(time.time()), line[:-1], "failure"]
                writer.writerow(log)  # !!here may come a new line
                continue

            tree = BPlusTree(current_path + "/"+type_name + ".db", order=50)
            if tree.__contains__(primary_key_value) == False:
                log = [int(time.time()), line[:-1], "failure"]
                writer.writerow(log)  # !!here may come a new line
                tree.close()
                continue

            value = tree.get(primary_key_value)
            tree.close()

            index = PAGE_SIZE * int(value[24:28]) + \
                int(value[28:29]) * RECORD_SIZE + PAGE_HEADER_SIZE
            record = ""

            with open(current_path+"/"+type_name + ".rec", "a+b") as f:
                mm = mmap.mmap(f.fileno(), 0)
                record = mm[index:index+RECORD_SIZE]
                mm.close()
            record = record.decode()
            search_out = ""
            for i in range(0, 12):
                if record[i*20:(i+1)*20] == " "*20:
                    break
                rec = record[i*20:(i+1)*20]
                oj = rec.find(" ")
                if oj != -1:
                    rec = rec[0:oj]
                search_out += rec
                if i != 11:
                    search_out += " "
            search_out = search_out[:-1]
            search_out += "\n"
            output_file.write(str_to_byte(search_out, len(search_out)))

            log = [int(time.time()), line[:-1], "success"]
            writer.writerow(log)  # !!here may come a new line
        elif elements[0] == "filter":
            """
            filter record <type-name><condition>

            - check for what the index of the variables is once you find the record

            #ERROR
            if type name doesn't exist
            if variable name in condition doesn't exist as primary key
            """

            type_name = elements[2]
            # check if the type doesn't exist in database
            # return the index of primary key in record if it exists
            primary_key_index = exists_in_system(type_name)

            if primary_key_index == False:
                log = [int(time.time()), line[:-1], "failure"]
                writer.writerow(log)  # !!here may come a new line
                continue

            system_catalog.seek(0)
            sc = system_catalog.readlines()
            sc = sc[1:]
            primary_key = ""  # primary_key_name
            for sce in sc:
                if (sce[42:62].decode() == type_name+" "*(20-len(type_name))) & (sce[63:68].decode().find("True") != -1):
                    primary_key += sce[0:20*primary_key_index].decode()
                    primary_key = primary_key[0:primary_key.find(" ")]
                    break

            # primary_key    find primary key here  check from system catalog what the name of primary key field is
            # BEWARE cut shrink the spaces at the end of primary key
            condition = elements[3]
            tree = BPlusTree(current_path + "/" + type_name + ".db", order=50)
            sub = []
            if condition.find(">") != -1:
                x = condition.find(">")
                left = condition[0:x]
                right = condition[x+1:]

                # below I find the primary key's index in a record
                key = tree.__iter__()
                key = list(key)
                key = list(map(intoa, key))
                key.sort()
                key = list(map(str, key))
                if left == primary_key:  # find the keys smaller than primary key
                    for k in range(0, len(key)):
                        if str(key[k]) > right:
                            sub = key[k:]
                            break
                else:
                    if right != primary_key:
                        log = [int(time.time()), line[:-1], "failure"]
                        writer.writerow(log)  # !!here may come a new line
                        continue
                    for k in range(0, len(key)):  # find the keys larger than primary key
                        if str(key[k]) > left:
                            sub = key[:k]
                            break
            elif condition.find("<") != -1:
                x = condition.find("<")
                left = condition[0:x]
                right = condition[x+1:]

                # below I find the primary key's index in a record
                key = tree.__iter__()
                key = list(key)
                key = list(map(intoa, key))
                key.sort()
                key = list(map(str, key))
                if left == primary_key:  # find the keys bigger than primary key
                    for k in range(0, len(key)):
                        if str(key[k]) > right:
                            sub = key[k:]
                            break
                else:
                    if right != primary_key:
                        log = [int(time.time()), line[:-1], "failure"]
                        writer.writerow(log)  # !!here may come a new line
                        continue
                    for k in range(0, len(key)):  # find the keys smaller than primary key
                        if str(key[k]) >= left:
                            sub = key[:k]
                            break
            elif condition.find("=") != -1:
                x = condition.find("=")
                left = condition[0:x]
                right = condition[x+1:]

                # below I find the primary key's index in a record
                if left != primary_key:
                    sub = [left]
                else:
                    if right != primary_key:
                        log = [int(time.time()), line[:-1], "failure"]
                        writer.writerow(log)  # !!here may come a new line
                        continue
            else:
                log = [int(time.time()), line[:-1], "failure"]
                writer.writerow(log)  # !!here may come a new line

            # find the records below
            for keya in sub:  # beware the sizes in
                value = tree.get(keya)
                index = index = PAGE_SIZE * \
                    int(value[24:28]) + int(value[28:29]) * \
                    RECORD_SIZE + PAGE_HEADER_SIZE
                with open(current_path + "/"+type_name + ".rec", "a+b") as rec_f:
                    mm = mmap.mmap(rec_f.fileno(), 0)
                    record = mm[index:index + RECORD_SIZE].decode()
                    record = record[:record.find(" "*20)]
                    out = ""
                    for re in range(0, (len(record)//20)+1):
                        rec = record[re*20:(re+1)*20]
                        oj = rec.find(" ")
                        if oj != -1:
                            rec = rec[0:oj]
                        out += rec + " "
                    out = out[:-1]
                    out += "\n"
                    output_file.write(str_to_byte(out, len(out)))
            log = [int(time.time()), line[:-1], "success"]
            writer.writerow(log)  # !!here may come a new line
            tree.close()
    output_file.close()
    system_catalog.close()
    catalog_for_relations.close()
    log_file.close()
    instruction_file.close()



if __name__ == '__main__':
    main()



#REFERENCES:
# https://github.com/NicolasLM/bplustree