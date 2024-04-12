import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

def create_payment_details():
    payment_details = pv.read_csv('data/raw/payment_details.csv')
    payment_details_schema = pa.schema([
        pa.field("Name", pa.string(), metadata={"PII": "False",
                                                "PCI": "False",
                                                "Sensitive": "True",
                                                "Governance_level": "Medium",
                                                "Data_Owner": "Head of Customer"}),
        pa.field("CC_number", pa.int64(), metadata={"PII": "False",
                                                    "PCI": "True",
                                                    "Sensitive": "True",
                                                    "Governance_level": "High",
                                                    "Data_Owner": "Chief Financial Officer"})])

    payment_extract = payment_details.cast(payment_details_schema)
    pq.write_table(payment_extract, 'data/parquet/payment_details.parquet')