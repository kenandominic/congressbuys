from argparse import ArgumentParser
from datetime import datetime
from concurrent.futures import as_completed, ThreadPoolExecutor

import house
import os
import threading
import pandas as pd

from periodictransactionreport import PeriodicTransactionReport

parser = ArgumentParser()
parser.add_argument('-f', '--file', default='data', type=str)
parser.add_argument('-y', '--year', default=datetime.now().year, type=int)
args = parser.parse_args()

succeeded_ptrs=[]
failed_ptrs=[]

transactions_columns = ['document_id', 'id', 'owner', 'asset', 'transactiontype', 'date', 'notif_date', 'amount', 'capgains']
transactions = pd.DataFrame(columns=transactions_columns, index=['document_id'])

def update_transactions(extracted_data, lock):
    with lock:
        global transactions
        # Assuming extracted_data is a DataFrame
        transactions = pd.concat([transactions, extracted_data], ignore_index=True)

os.makedirs(args.file, mode=0o777, exist_ok=True)
ptrs = house.get_ptrs(args.year)
lock = threading.Lock()

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(house.extract_transactions, ptr): ptr for ptr in ptrs}
    for future in as_completed(futures):
        ptr = futures[future]
        try:
            extracted_transactions = future.result()
            extracted_transactions['document_id'] = ptr.document_id
            update_transactions(extracted_transactions, lock)
            succeeded_ptrs.append(ptr.to_dict())
        except Exception as e:
            failed_ptrs.append(ptr.to_dict())
            print(f'Failed extracting transactions from {ptr.document_id} for year {args.year}: {e}')

            
pd.DataFrame(succeeded_ptrs).to_csv(f'{args.file}/{args.year}_ptrs.csv')
pd.DataFrame(failed_ptrs).to_csv(f'{args.file}/{args.year}_failed_ptrs.csv')
transactions.to_csv(f'{args.file}/{args.year}_transactions.csv')
