import secureRead as sr
import setupSecureFile as setup

setup.create_payment_details()
results = sr.secure_read("data/parquet/payment_details.parquet", "True")
metadata = sr.read_metadata("data/parquet/payment_details.parquet")
print(results)
print(metadata)