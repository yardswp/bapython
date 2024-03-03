import math

from members import *
from member_financials import *


def get_account_fee(r):
    if r['Associate']:
        return 10
    else:
        match r['Post Zone']:
            case 'Barbican':
                return 5
            case 'UK':
                return 8
            case 'Europe':
                return 11
            case _:
                return 14


def create_affordability_row(r):
    fee = get_account_fee(r)
    balance = r['Balance']
    return {
        **r,
        'Balance': balance,
        'Membership Fee': fee,
        'Can Afford': balance >= fee
    }


def create_reprint_row(r):
    if r['Reset Issuance']:
        card_renewal_date = NOW + offsets.MonthBegin() * 12
        return {
            'Processing Date': NOW,
            'Card Issuance': NOW,
            'Renewal Date': card_renewal_date,
            'Card End Date': card_renewal_date + offsets.MonthEnd(),
            'Membership Fee': 0,
            'Issuance Count': r['Issuance Count'],
            'Anticipatory': False
        }
    else:
        return {
            'Processing Date': NOW,
            'Card Issuance': r['Card Issuance.Card Issuance'],
            'Renewal Date': r['Card Issuance.Renewal Date'],
            'Card End Date': r['Card Issuance.Card End Date'],
            'Membership Fee': 0,
            'Issuance Count': r['Issuance Count'],
            'Anticipatory': False
        }


def create_issuance(r):
    card_issuance = NOW if isnull(r['Renewal Date']) or r['Renewal Date'] < NOW else r['Renewal Date']
    renewal_date = card_issuance + offsets.MonthBegin() * 12
    return {
        'Processing Date': NOW,
        'Card Issuance': card_issuance,
        'Renewal Date': renewal_date,
        'Card End Date': renewal_date + offsets.MonthEnd(),
        'Membership Fee': r['Membership Fee'],
        'Issuance Count': r['Issuance Count'],
        'Anticipatory': not r['Can Afford']
    }


def timestamp_to_long_date_with_ordinal(ts: Timestamp):
    day, month_name, year = ts.day, ts.month_name(), ts.year
    day_ten, day_digit = math.trunc(day / 10), day % 10
    day_ordinal = 'th'
    if (not day_ten == 1) and 0 < day_digit < 4:
        match day_digit:
            case 1:
                day_ordinal = 'st'
            case 2:
                day_ordinal = 'nd'
            case 3:
                day_ordinal = 'rd'
    return f'{day}{day_ordinal} of {month_name} {year}'


def create_card_row_creator():
    filename_count = 0

    def create_card_row(r):
        nonlocal filename_count
        filename_count = filename_count + 1
        filename = f'Card_{filename_count:0=4}'
        year = r['Card End Date'].year - (2 if r['Card End Date'].month < 4 else 1)
        return {
            'p': 'p',
            'n': filename,
            'mn': r['index'],
            'ad': r['Address 1'],
            'nm': r['Full Name'],
            'd': timestamp_to_long_date_with_ordinal(r['Card End Date']),
            'year': year,
            'sy': r['Card Issuance'].year,
            'em': r['Card End Date'].month_name()[0:3],
            'ey': r['Card End Date'].year,
            'pw': (competitions.loc[year] if year in competitions.index else {'Text': ''})['Text'],
            'an': r['Anticipatory']
        }

    return create_card_row


print('loading/processing competitions')
competitions = \
    read_excel(files_dir + '\\Competitions.xlsx').apply(
        lambda r: [
            r['Year'] + 1,
            'This year''s picture is by ' + r['Winner'] + ', winner of the ' + str(r['Year'] + 1) +
            ' Barbican Photo Competition.'], axis=1, result_type='expand')\
    .rename(columns={0: 'Year', 1: 'Text'})\
    .set_index('Year')

print('processing accounts')
extant_accounts = accounts[isnull(accounts['Cancelled'])]\
    .drop(columns=['Cancelled'])\
    .join(balances)\
    .apply(create_affordability_row, axis=1, result_type='expand')


print('loading force_reprints')
force_reprints = read_excel(files_dir + '\\Force Reprints.xlsx')
print('processing force_reprints')
force_reprints['Reset Issuance'] = notna(force_reprints['Reset Issuance']) & force_reprints['Reset Issuance']
force_reprints =\
    force_reprints.set_index(
        'Membership ID'
    ).join(
        issuance[issuance['Card End Date'] > NOW].groupby('Membership ID').agg(**{
            'Card Issuance.Card Issuance': ('Card Issuance', 'max'),
            'Card Issuance.Renewal Date': ('Renewal Date', 'max'),
            'Card Issuance.Card End Date': ('Card End Date', 'max'),
            'Issuance Count': ('Membership ID', 'count'),
        }),
        how='inner'
    ).apply(
        create_reprint_row,
        axis=1,
        result_type='expand'
    )

print('processing card_renewal_dates')
card_renewal_dates =\
    concat(
        [issuance.set_index('Membership ID'), force_reprints]
    ).groupby('Membership ID').agg(**{
        'Renewal Date': ('Renewal Date', 'max'),
        'Issuance Count': ('Renewal Date', 'count'),
    })

print('processing end_dates')
end_dates = extant_accounts[['Membership Fee', 'Can Afford']].join(card_renewal_dates)
end_dates = end_dates[
        (
                (
                        end_dates['Renewal Date'].isna() |
                        (NOW >= end_dates['Renewal Date'])
                ) &
                end_dates['Can Afford']
        ) |
        (
                (NOW <= end_dates['Renewal Date']) &
                (end_dates['Renewal Date'] < NOW + offsets.MonthEnd() * 13)
        )]\
    .apply(create_issuance, axis=1, result_type='expand')
end_dates = concat([end_dates, force_reprints])\
    .apply(
        lambda r: {
            **r,
            'Letter Date': r['Card Issuance'] if r['Card Issuance'] > NOW else NOW,
            'Previous Issuance': r['Issuance Count'] > 0
        },
        axis=1,
        result_type='expand'
    )

print('processing new_issuances')
new_issuances =\
    end_dates\
    .reset_index(names='Membership ID')\
    .sort_values(
        by=['Anticipatory', 'Letter Date', 'Membership ID']
    )[
        ['Membership ID', 'Processing Date', 'Card Issuance', 'Renewal Date', 'Card End Date', 'Membership Fee',
         'Anticipatory']
    ]

print('processing commencing_accounts')
commencing_accounts =\
    end_dates\
    .join(extant_accounts.drop(columns=['Membership Fee']))\
    .join(members[members['Count'] == 1][['Email', 'Telephone']])\
    .reset_index(names='Membership Number')\
    .sort_values(
        by=['Letter Date', 'Anticipatory', 'Membership Number']
    )[
        [
            'Addressee', 'Informal Greeting', 'Address Line 1', 'Address Line 2', 'City', 'County', 'Post Code',
            'Country', 'Membership Number', 'Telephone', 'Email', 'Letter Date', 'Previous Issuance', 'Anticipatory'
        ]
    ]


print('processing renewal_letter_accounts')
renewal_letter_accounts = commencing_accounts[commencing_accounts['Previous Issuance']]\
    .drop(columns='Previous Issuance')

print('processing new_letter_accounts')
new_letter_accounts = commencing_accounts[~commencing_accounts['Previous Issuance']]\
    .drop(columns=['Letter Date', 'Previous Issuance', 'Anticipatory'])

print('processing cards')
cards = end_dates.join(extant_accounts.drop(columns=['Membership Fee']))\
    .join(properties['Address 1'], on='Property Code')\
    .join(members[['Full Name', 'Count']])\
    .reset_index()\
    .sort_values(
        by=['Previous Issuance', 'Anticipatory', 'Letter Date', 'index', 'Count'], kind='stable'
    )\
    .apply(
    create_card_row_creator(),
    axis=1,
    result_type='expand'
)

print(f'writing to Cards to Print {NOW.isoformat()}')
with ExcelWriter('Cards to Print ' + NOW.isoformat().replace(':', '-') + '.xlsx') as writer:
    new_letter_accounts.to_excel(writer, sheet_name='New Letter Accounts', index=False)
    renewal_letter_accounts.to_excel(writer, sheet_name='Normal Letter Accounts', index=False)
    new_issuances.to_excel(writer, sheet_name='New Issuances', index=False)
    cards.to_excel(writer, sheet_name='Cards', index=False)
