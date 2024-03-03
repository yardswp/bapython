from cards_to_print import *


fee_mappings = {0: None, 5: 'UK', 8: 'UK', 10: 'UK', 11: 'EU', 14: 'International'}


def write_postal_batches():
    excel_write('postal batches ', [
        ('Postal Batches', postal_batches),
        ('Zone Resolved', zone_resolved_issuance),
        ('Working', self_joined_issuance)
    ], NOW)


def excel_write(prefix: str, sheets: list[tuple[str, DataFrame]], now: Timestamp = None):
    if now is None:
        now = Timestamp.today()
    file_name = f'{prefix}{now.isoformat().replace(':', '-')}.xlsx'
    print(f'writing to {file_name}')
    with ExcelWriter(file_name) as writer:
        for sheet in sheets:
            sheet[1].to_excel(writer, sheet_name=sheet[0])


def create_resolved_columns(row_dict):
    fee = row_dict['Membership Fee Original'] if row_dict['Has Fee'] else row_dict['Membership Fee Joined'] if row_dict['Replace Fee'] else 0
    batch_date = row_dict['Processing Date Original']
    letters = 1
    membership_cards = row_dict['Members']
    preprinted_letters = 0
    preprinted_cards = 0
    if not isna(row_dict['Preprinted']):
        batch_date = row_dict['Card Issuance Original']
        letters, preprinted_letters = preprinted_letters, letters
        membership_cards, preprinted_cards = preprinted_cards, membership_cards
    return {
        'Batch': batch_date.strftime('%Y%m'),
        'Fee': fee,
        'Zone': fee_mappings[fee],
        'Valid': fee > 0,
        'Letters': letters,
        'Cards': membership_cards,
        'Preprinted Letters': preprinted_letters,
        'Preprinted Cards': preprinted_cards
    }


print('processing postal batches')
indexed_issuance = issuance.set_index('Membership ID')
self_joined_issuance = indexed_issuance\
    .join(indexed_issuance, lsuffix=' Original', rsuffix=' Joined')\
    .join(members.groupby('Membership ID').agg(**{'Members': ('Count', 'max')}))\
    .reset_index()
self_joined_issuance = preprints\
    .set_index(['Membership Number', 'Letter Date'])\
    .merge(
        self_joined_issuance,
        how='right',
        left_index=True,
        right_on=['Membership ID', 'Card Issuance Original'])\
    .sort_values('Processing Date Joined', ascending=False)
self_joined_issuance['Has Fee'] =\
    ((self_joined_issuance['Membership Fee Original'] > 0) &
     (self_joined_issuance['Processing Date Original'] == self_joined_issuance['Processing Date Joined']))
self_joined_issuance['Previous Prospectives'] =\
    ((self_joined_issuance['Membership Fee Original'] == 0) &
     (self_joined_issuance['Processing Date Original'] > self_joined_issuance['Processing Date Joined']))
self_joined_issuance['Closest Prospective'] = ~self_joined_issuance.duplicated(
    ['Membership ID', 'Processing Date Original', 'Membership Fee Original', 'Membership Fee Joined',
     'Previous Prospectives'])
self_joined_issuance['Replace Fee'] =\
    ((self_joined_issuance['Membership Fee Original'] == 0) &
     (self_joined_issuance['Membership Fee Joined'] > 0) &
     self_joined_issuance['Previous Prospectives'] &
     self_joined_issuance['Closest Prospective'])
self_joined_issuance =\
    concat([self_joined_issuance,
            self_joined_issuance.apply(create_resolved_columns, axis=1, result_type='expand')],
           axis=1)

zone_resolved_issuance = self_joined_issuance[self_joined_issuance['Valid'] > 0][
    ['Batch', 'Zone', 'Letters', 'Cards', 'Preprinted Letters', 'Preprinted Cards']]
postal_batches = zone_resolved_issuance.groupby(['Batch', 'Zone']).agg(**{
    'Letters': ('Letters', 'sum'),
    'Cards': ('Cards', 'sum'),
    'Preprinted Letters': ('Preprinted Letters', 'sum'),
    'Preprinted Cards': ('Preprinted Cards', 'sum')})

if __name__ == '__main__':
    write_postal_batches()
