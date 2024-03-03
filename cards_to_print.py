import math
import sys
from typing import Callable

from members import *
from member_financials import *


advance_months = 0
# This does not work as expected, as it will include anyone who could possibly be renewed
# I think that I would rather that it only included people who had been members in the previous n months
include_anticipatory = False

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


def create_reprint_row_creator() -> Callable[[object], object]:
    card_renewal_date = month_begin + offsets.MonthBegin() * 12
    card_end_date = card_renewal_date + offsets.MonthEnd()
    return lambda r: {
        'Processing Date': NOW,
        'Card Issuance': month_begin,
        'Renewal Date': card_renewal_date,
        'Card End Date': card_end_date,
        'Membership Fee': 0,
        'Issuance Count': r['Issuance Count'],
        'Anticipatory': False
    } if r['Reset Issuance'] else {
        'Processing Date': NOW,
        'Card Issuance': r['Card Issuance.Card Issuance'],
        'Renewal Date': r['Card Issuance.Renewal Date'],
        'Card End Date': r['Card Issuance.Card End Date'],
        'Membership Fee': 0,
        'Issuance Count': r['Issuance Count'],
        'Anticipatory': False
    }


def create_issuance(r):
    card_issuance = month_begin if isnull(r['Renewal Date']) or r['Renewal Date'] < NOW else r['Renewal Date']
    renewal_date = card_issuance + offsets.MonthBegin() * 12
    end_date = renewal_date + offsets.MonthEnd()
    return {
        'Processing Date': NOW,
        'Card Issuance': card_issuance,
        'Renewal Date': renewal_date,
        'Card End Date': end_date,
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
        filename_count += 1
        filename = f'Card_{filename_count:0=4}'
        year = r['Card End Date'].year - (2 if r['Card End Date'].month < 4 else 1)
        return {
            'p': 'p',
            'n': filename,
            'mn': r['Membership Number'],
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
        create_reprint_row_creator(),
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

print('loading preprints')
preprints = read_excel(files_dir + '\\' + 'Preprints.xlsx')\
    .drop(columns=['Card End Date', 'Addressee', 'Informal addressee', 'Address Line 1', 'Done'])
preprints['Preprinted'] = True

print('processing end_dates')
end_dates = extant_accounts[['Membership Fee', 'Can Afford']].join(card_renewal_dates)
print('\tfiltering out accounts that aren\'t ready to renew, and creating issuances for the remaining')
end_date_filter = end_dates['Renewal Date'].isna() | (NOW >= end_dates['Renewal Date'])
if advance_months > 0:
    end_date_filter = end_date_filter | (NOW <= end_dates['Renewal Date']) & (end_dates['Renewal Date'] < month_end + offsets.MonthEnd() * advance_months)
if not include_anticipatory:
    end_date_filter = end_date_filter & end_dates['Can Afford']
end_dates = end_dates[end_date_filter]\
    .apply(create_issuance, axis=1, result_type='expand')
print('\tadding forced reprints, letter dates and previous issuance')
end_dates = concat([end_dates, force_reprints])\
    .apply(
        lambda r: {
            **r,
            'Letter Date': r['Card Issuance'] if r['Card Issuance'] > month_begin else month_begin,
            'Previous Issuance': r['Issuance Count'] > 0
        },
        axis=1,
        result_type='expand')\
    .reset_index(names='Membership Number')\
    .set_index(['Membership Number', 'Letter Date'])\
    .join(preprints.set_index(['Membership Number', 'Letter Date']))\
    .reset_index()\
    .sort_values(by=['Letter Date', 'Previous Issuance', 'Anticipatory', 'Membership Number'])\
    .set_index('Membership Number')
end_dates['Preprinted'].replace(NaN, False, inplace=True)

used_preprints = preprints\
    .set_index(['Membership Number', 'Letter Date'])\
    .join(
        end_dates
            .reset_index()[['Membership Number', 'Letter Date']]
            .set_index(['Membership Number', 'Letter Date']),
        how='inner')\
    .reset_index()

end_dates = end_dates[~end_dates['Preprinted']]

print('processing new_issuances')
new_issuances =\
    end_dates\
    .reset_index()[
        ['Membership Number', 'Processing Date', 'Card Issuance', 'Renewal Date', 'Card End Date', 'Membership Fee',
         'Anticipatory']
    ]

print('processing commencing_accounts')
commencing_accounts =\
    end_dates\
    .join(extant_accounts.drop(columns=['Membership Fee']))\
    .join(members[members['Count'] == 1][['Email', 'Telephone']])\
    .reset_index()[
        [
            'Addressee', 'Informal Greeting', 'Address Line 1', 'Address Line 2', 'City', 'County', 'Post Code',
            'Country', 'Membership Number', 'Telephone', 'Email', 'Letter Date', 'Previous Issuance', 'Anticipatory'
        ]
    ]

print('processing new_letter_accounts')
new_letter_accounts = commencing_accounts[~commencing_accounts['Previous Issuance']]\
    .drop(columns=['Letter Date', 'Previous Issuance', 'Anticipatory'])


print('processing renewal_letter_accounts')
renewal_letter_accounts = commencing_accounts[commencing_accounts['Previous Issuance']]\
    .drop(columns='Previous Issuance')

print('processing cards')
cards = end_dates.join(extant_accounts.drop(columns=['Membership Fee']))\
    .join(properties['Address 1'], on='Property Code')\
    .join(members[['Full Name', 'Count']])\
    .reset_index()\
    .sort_values(
        by=['Letter Date', 'Previous Issuance', 'Anticipatory', 'Membership Number', 'Count'])\
    .apply(
        create_card_row_creator(),
        axis=1,
        result_type='expand')
    
cards_10up = cards.copy(deep=False)
cards_10up[['n','c']] = [(int(i/10), int(i%10)) for i in range(len(cards_10up))]
print('processing 10-up cards')
cards_10up = cards_10up\
    .groupby('n')\
    .apply(
        lambda df:
            df.apply(
                lambda r:
                    {
                        k + str(r['c']): v 
                        for (k, v)
                        in r.items()
                        if not k in ['an', 'n', 'c', 'p']},
                axis=1,
                result_type='expand'))\
    .groupby('n')\
    .agg(
        lambda s:
            (
                [
                    (int(v) if isinstance(v, float) else v)
                    for v
                    in s
                    if not isinstance(v, float)
                        or not math.isnan(v)][:1]
                or [NaN])[0])\
    .reset_index(names='n')
cards_10up.insert(1, 'p', 'p')

current_accounts = accounts\
    .join(
        concat(
            [
                current_members_accounts.reset_index(names='Membership Number'),
                new_issuances[['Membership Number']]
            ])\
            .drop_duplicates()\
            .sort_values(by='Membership Number').set_index('Membership Number'),
        how='inner')
zone_mapping = {
    'Zone 3': 0,
    'Zone 2': 1,
    'Zone 1': 2,
    'Europe': 3,
    'UK': 4,
    'Barbican': 5
}
zone_mapper = lambda z: zone_mapping[z]
current_accounts['Zone Order'] = current_accounts['Post Zone'].map(zone_mapper)
    
offsite_accounts = current_accounts[current_accounts['Post Zone'] != 'Barbican']\
    .reset_index(names='Membership Number')\
    .sort_values(by=['Zone Order', 'Membership Number'])[
        ['Addressee', 'Informal Greeting', 'Address Line 1', 'Address Line 2', 'City',
         'County', 'Post Code', 'Country', 'Post Zone', 'Membership Number']]
    
post_zones = current_accounts\
    .reset_index()\
    .groupby('Post Zone').agg(**{'Count': ('Membership Number', 'count')})\
    .reset_index()
post_zones['Zone Order'] = post_zones['Post Zone'].map(zone_mapper)
post_zones = post_zones.sort_values('Zone Order')[['Post Zone', 'Count']]

if __name__ == '__main__':
    print(f'writing to Cards to Print {NOW.isoformat()}')
    with ExcelWriter('Cards to Print ' + NOW.isoformat().replace(':', '-') + '.xlsx') as writer:
        new_letter_accounts.to_excel(writer, sheet_name='New Letter Accounts', index=False)
        renewal_letter_accounts.to_excel(writer, sheet_name='Normal Letter Accounts', index=False)
        new_issuances.to_excel(writer, sheet_name='New Issuances', index=False)
        used_preprints.to_excel(writer, sheet_name='Preprints', index=False)
        cards.to_excel(writer, sheet_name='Single File Cards', index=False)
        cards_10up.to_excel(writer, sheet_name='Multi File Cards', index=False)
    
    print(f'writing to Addresses {NOW.isoformat()}')
    with ExcelWriter('Addresses ' + NOW.isoformat().replace(':', '-') + '.xlsx') as writer:
        offsite_accounts.to_excel(writer, sheet_name='Offsite Members', index=False)
        post_zones.to_excel(writer, sheet_name='Post Zones', index=False)
        
