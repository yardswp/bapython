import os
from glob import glob
from pandas import *
from dotenv import find_dotenv, load_dotenv


load_dotenv(find_dotenv())


def payments(file_name: str) -> DataFrame:
    print(f'loading {file_name}')
    dir_name = os.getenv('BA_FILES_DIR')
    return concat(
        [
            read_excel(full_name, 'Payments').set_index(['Membership ID', 'Date'])
            for full_name
            in glob(dir_name + '\\' + file_name + '.xls?')
        ]
    )
    
    
def write_member_financials():
    NOW = Timestamp.today()
    print(f'writing to Member Financials {NOW.isoformat()}')
    with ExcelWriter('Member Financials ' + NOW.isoformat().replace(':', '-') + '.xlsx') as writer:
        balances.to_excel(writer, sheet_name='Balances')
        payment_history.to_excel(writer, sheet_name='Payment History')


file_names = ['Card Issuances', 'Cheques', 'Gifts', 'PayPal', 'Statements']

print('processing payment history')
payment_history = concat([payments(file_name) for file_name in file_names]).sort_index()

print('processing balances')
balances = payment_history\
    .reset_index(names=['Membership ID', 'Date'])\
    .groupby('Membership ID')\
    .agg(**{'Balance': ('Amount', 'sum')})

if __name__ == '__main__':
    write_member_financials()