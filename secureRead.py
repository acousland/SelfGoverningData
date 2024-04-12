import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa


def contains_PCI(file):
    _schema = pq.read_schema(file)    
    contains_pci = any(field.metadata and field.metadata.get(b'PCI') == b'True' for field in _schema)
    return(contains_pci)

def contains_PII(file):
    _schema = pq.read_schema(file)    
    contains_pii = any(field.metadata and field.metadata.get(b'PII') == b'True' for field in _schema)
    return(contains_pii)

def extract_data_owner(file):
    _schema = pq.read_schema(file)    
    data_owner_dict = {field.name: field.metadata[b'Data_Owner'].decode('utf-8')
                        for field in _schema
                        if field.metadata and b'Data_Owner' in field.metadata
                      }
    return(data_owner_dict)

def extract_data_owner_PCI(file):
    _schema = pq.read_schema(file)    
    data_owner_PCI_dict = {
        field.name: field.metadata[b'Data_Owner'].decode('utf-8')
        for field in _schema
        if field.metadata and b'PCI' in field.metadata and field.metadata[b'PCI'] == b'True'
        and b'Data_Owner' in field.metadata
    }
    return(data_owner_PCI_dict)

def secure_read(file, user_PCI_privelaged):
    output = ""
    if contains_PCI(file) and user_PCI_privelaged=="False":
        output += "Unfortunately, this file has information you are not privileged to view.\n"
        data_owner = extract_data_owner_PCI(file)
        for key in data_owner:
            output += "Please contact " + str(data_owner[key]) + " for access to " + str(key) + "\n"
    else:
        table = pq.read_table(file)
        output += str(table) + "\n"
    return(output)


def read_metadata(file_path):
    parquet_schema = pq.read_schema(file_path)

    # Collect the metadata strings
    metadata_strings = []
    for field in parquet_schema:
        field_metadata_string = f"Field: {field.name}\n"
        if field.metadata:
            metadata = {key.decode('utf-8'): value.decode('utf-8') 
                        for key, value in field.metadata.items()}
            field_metadata_string += f"Metadata: {metadata}\n"
        else:
            field_metadata_string += "No metadata available\n"
        field_metadata_string += "-" * 30 + "\n"
        metadata_strings.append(field_metadata_string)

    # Join and return the metadata strings
    return ''.join(metadata_strings)