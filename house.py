from io import BytesIO
from itertools import zip_longest
from pathlib import Path
from typing import Any, List
from urllib.parse import urljoin

import pandas as pd
import pdfplumber
import requests
import shutil
import xml.etree.ElementTree as ET
import zipfile

from periodictransactionreport import PeriodicTransactionReport

FILINGS_URL = 'https://disclosures-clerk.house.gov'
TMP_DIR = Path('/tmp/congressbuys')

def get_ptrs(year:int) -> List[PeriodicTransactionReport]:
    """
    returns a list of periodic transaction reports in a year
    """
    response = requests.get(urljoin(FILINGS_URL, f'public_disc/financial-pdfs/{year}FD.zip'))
    response.raise_for_status()
    
    with zipfile.ZipFile(BytesIO(response.content)) as z:
        z.extractall(TMP_DIR)
    
    xml_file = TMP_DIR.joinpath(f'{year}FD.xml')
    tree = ET.parse(xml_file)
    shutil.rmtree(TMP_DIR)
    root = tree.getroot()
    
    ptrs = []
    
    # Iterate through each Member element
    for member in root.findall('Member'):
        filing_type = member.find('FilingType').text
        if filing_type == 'P':
            ptrs.append(PeriodicTransactionReport(
                document_id=member.find('DocID').text,
                last=member.find('Last').text,
                first=member.find('First').text,
                state_dst=member.find('StateDst').text,
                year=int(member.find('Year').text),
                filing_date=member.find('FilingDate').text))

    return ptrs

"""
EXTRACTS TRANSACTIONS FROM A PTR PDF
"""

def download_ptr(ptr: PeriodicTransactionReport) -> BytesIO:
    """
    Downloads the PTR PDF and returns its contents as bytes.
    """
    resource = f'public_disc/ptr-pdfs/{ptr.year}/{ptr.document_id}.pdf'
    response = requests.get(urljoin(FILINGS_URL, resource))
    response.raise_for_status()
    return BytesIO(response.content)
  
def extract_tables(pdfbytes: BytesIO) -> List[List[List[Any]]]:
  """
  returns all tables from a pdf
  tables = [table -> row -> cell]
  """
  # crop the pages to just the tables within them
  cropped_pages = [page.crop(table.bbox) for page in pdfplumber.open(pdfbytes).pages 
                   for table in page.find_tables()]
  # get the rectangles of the column headers
  rects = cropped_pages[0].rects[:8]
  # get the x coordinates of the rectangles to use as the lines for columns
  lines = [rect['x0'] for rect in rects] + [rects[-1]['x1']]
  table_settings = {"explicit_vertical_lines": lines, "vertical_strategy": 'explicit'}
  # for cropped_page in cropped_pages:
  #   display(cropped_page.to_image().debug_tablefinder(table_settings))
  return [table for cropped_page in cropped_pages 
          for table in cropped_page.extract_tables(table_settings)]


def fix_table(table: List[List[str]]) -> List[List[str]]:
  """
  fixes a table
  table = [row -> cell]
  """
  fixed_table = []
  # the asset column can overflow into the following columns
  # 0: id, 1: owner, 2: asset, 3: transaction type, 4: date
  # 5 notification date, 6: amount, 7: cap gains
  for i in range(1, len(table)):
    row = [col.lower() for col in table[i]]
    
    # overflow from asset field will be on new lines
    asset = row[2].split('\n')
    transaction_type = row[3].split('\n')
    date = row[4].split('\n')
    notif_date = row[5].split('\n')
    
    # first line (element of array) will be the field value
    row[3] = transaction_type[0].split(' ')[0]
    row[4] = date[0]
    row[5] = notif_date[0]
    row[6] = row[6].replace('\n', ' ')

    # clear the field value
    transaction_type[0] = date[0] = notif_date[0] = ''
    fixed_asset = []

    # assuming that filing status is on line 2 and never overflows
    # reverse the fields and then join each field
    asset.reverse()
    transaction_type.reverse()
    date.reverse()
    notif_date.reverse()
    
    # Set the first elements of transaction_type, date, and notif_date to ''
    fixed_asset = [f'{a}{t}{d}{nd}' for a, t, d, nd in zip_longest(asset, transaction_type, date, notif_date, fillvalue='')]

    # reverse again to get the right order
    fixed_asset.reverse()
    row[2] = ' '.join(fixed_asset)
    fixed_table.append(row)
  # a row can get split up into multiple rows.
  return fixed_table

ASSET_TYPES = ['st', 'reit', 'rs', 'ps', 'op', 'ef']
ASSET_TYPE_PATTERN = '|'.join(ASSET_TYPES)
ASSET_TYPE_MATCH = rf'\[{ASSET_TYPE_PATTERN}]'
ASSET_TYPE_GROUP = rf'\[({ASSET_TYPE_PATTERN})]'
TRANSACTION_COLUMNS = ['id', 'owner', 'asset', 'transactiontype', 'date', 'notif_date', 'amount', 'capgains']

def extract_transactions(ptr: PeriodicTransactionReport) -> pd.DataFrame:
  
  df = pd.DataFrame(columns=TRANSACTION_COLUMNS)
  pdfbytes = download_ptr(ptr)
  fixed_tables = [fix_table(extracted_table) for extracted_table in extract_tables(pdfbytes)]

  for fixed_table in fixed_tables:
    df = pd.concat([df, pd.DataFrame(fixed_table, columns=TRANSACTION_COLUMNS)], ignore_index=True)

  return (
      df.drop(columns=['notif_date', 'capgains'])
        .dropna(subset=['amount'])
        .loc[df['amount'].str.strip().astype(bool)]
        .loc[df['asset'].str.contains(ASSET_TYPE_MATCH, regex=True)]
        .assign(
            assettype=lambda x: x['asset'].str.extract(ASSET_TYPE_GROUP),
            ticker=lambda x: x['asset'].str.extract(r'\((.*?)\)')
        )
        .set_index('id')
        .map(lambda x: x.replace('\x00', '_').strip() if isinstance(x, str) else x)
    )