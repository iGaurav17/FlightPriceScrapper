#  Created on 07-Jan-2025.
 
#   ####################################################################
#   Copyright (c) 2025 Optiontown, Inc. All Rights Reserved.
 
#   This software without any limitations is strictly confidential and
#   proprietary to Optiontown, Inc. Any unlawful copying, disclosure
#   or use shall be vigorously prosecuted.
#   ####################################################################
#   Changes (from 07-Jan-2025)
#   ####################################################################
  
#   07-Jan-2025 Gaurav Bhardwaj   : Bug 30535 - Google Flight Price Scrapper
                                    # db_Properties.py -stores db credentials

from encryption import decrypt_password, load_key

# DB_CONFIG = {
#     "user": "GAURAV_BHARDWAJ",
#     "password": "XZFAauJ1ZTvhaa",
#     "dsn": "192.168.64.137:1523/betadb"
# }

# DB_CONFIG = {
#     "user": "DEV_DUMP_BETA_NT_1",
#     "password": "DSKLFerlkJHG8",
#     "dsn": "192.168.64.137:1523/betadb"
# }

encrypted_pass="gAAAAABnkgGhuY7xRm0SsFZGFyb2MIr4NrEZuvuOfc7CpFQNI_VEh8M9IcVlWP8133O4JiQyEw55O7sE8G9bH70f2Id4JBlWqw=="
decrypted_pass = decrypt_password(encrypted_pass, key=load_key())

DB_CONFIG = {
    "user": "FLIGHT_PRICE",
    "password": decrypted_pass,
    "dsn": "192.168.64.21:1521/prim2"
}


