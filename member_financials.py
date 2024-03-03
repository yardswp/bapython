import os

from dotenv import find_dotenv, load_dotenv
from pandas import DataFrame, ExcelWriter, Timestamp, concat, notna, offsets
from xlsxwriter import Workbook

from utils import loadFromExcel


def payments(file_name: str) -> DataFrame:
    dir_name = os.getenv('BA_FILES_DIR', '')
    return loadFromExcel(file_name, 'Payments').set_index(['Membership ID', 'Date'])


def write_member_financials():
    now = Timestamp.today()
    print(f'writing to Member Financials {now.isoformat()}')
    with ExcelWriter('Member Financials ' + now.isoformat().replace(':', '-') + '.xlsx') as writer:
        balances.to_excel(writer, sheet_name='Balances')
        payment_history.to_excel(writer, sheet_name='Payment History')


def write_previous_month_payments():
    last_month_end = Timestamp.today().normalize() - offsets.MonthBegin()
    last_month_start = last_month_end - offsets.MonthBegin()
    statement_history = loadFromExcel('Statements', 'Statements 30-91-79 27933660')
    statement_history = statement_history[
            (statement_history['Transaction Date'] >= last_month_start) &
            (statement_history['Transaction Date'] < last_month_end)]\
        .sort_index(ascending=False)
    statement_history['Amount'] = statement_history['Credit Amount'].fillna(0) - statement_history[
        'Debit Amount'].fillna(0)
    statement_history = statement_history[
            ['Transaction Date', 'Transaction Description', 'Amount']
        ]
    (max_row, max_col) = statement_history.shape
    payments_filename = f'Payments {last_month_start.month_name()} {last_month_end.year}.xlsx'
    print(f'Writing {payments_filename}')
    with ExcelWriter(payments_filename, engine='xlsxwriter', datetime_format='d mmmm yyyy') as writer:
        statement_history.to_excel(
            writer, sheet_name='Payments', index=False, header=False, startrow=1)
        payments_sheet = writer.sheets['Payments']
        payments_sheet.add_table(1, 0, max_row, max_col - 1, {
            'header_row': False,
            'style': 'Table Style Medium 4',
            'name': 'Output_for_Tony'
        })
        payments_sheet.set_column(0,0, 14)
        payments_sheet.set_column(1,1, 85)
        payments_sheet.set_column(2,2, 7)


file_names = ['Card Issuances', 'Cheques', 'Gifts', 'PayPal', 'Statements']

print('processing payment history')
payment_history = concat([payments(file_name) for file_name in file_names]).sort_index()

print('processing balances')
balances = payment_history\
    .reset_index(names=['Membership ID', 'Date'])\
    .groupby('Membership ID')\
    .agg(Balance=('Amount', 'sum'))

if __name__ == '__main__':
    write_member_financials()
    write_previous_month_payments()
