from re import search

from pandas import *
from numpy import NaN


NOW = Timestamp.today()


def trim_normalise_string(x: any):
    if not isinstance(x, str):
        return x

    x = x.strip()
    if len(x) == 0:
        return NaN

    return x


def sanitise_name_string(name, with_space=True):
    if isna(name) or (isinstance(name, str) and len(name) == 0):
        return ''
    if with_space:
        return name + ' '
    return name


def create_formal_name(title, firstname, surname):
    title = sanitise_name_string(title)
    firstname = (firstname[0] + ' ') if isinstance(firstname, str) and len(firstname) > 0 else ''
    surname = sanitise_name_string(surname, False)

    return title + firstname + surname


def create_informal_name(title, firstname, surname):
    return create_formal_name(title, firstname, surname) if isna(firstname) or len(firstname) == 1 else firstname


def create_full_name(title, firstname, middlename, surname):
    title = sanitise_name_string(title)
    firstname = sanitise_name_string(firstname)
    middlename = sanitise_name_string(middlename)
    surname = sanitise_name_string(surname, False)

    return title + firstname + middlename + surname


def create_addressee(name_series):
    name_series = name_series.sort_index().reset_index(level=[0, 1], drop=True)
    if len(name_series) > 1:
        return name_series[0] + ' & ' + name_series[1]
    return name_series[0]


def create_informal_greeting(name_series):
    name_series = name_series.sort_index().reset_index(level=[0, 1], drop=True)
    names_len = len(name_series)
    if names_len > 1:
        greeting = name_series[0]
        for idx in range(1, names_len):
            greeting = greeting + (' and ' if idx == names_len - 1 else ', ') + name_series[idx]
        return greeting
    return name_series[0]


files_dir = 'C:\\Users\\jimda\\OneDrive\\BA Membership Files'
print("loading properties")
properties =\
    read_excel(files_dir + '\\Properties.xlsx', 'Properties')\
    .set_index('Property Code')

print("loading normal_members")
normal_members =\
    read_excel(files_dir + '\\Member Details.xlsm', 'Member')\
    .join(properties, on='Property Code')\
    .rename(columns={
        'Comment (YELLOW HIGHLIGHT = OLD COMMENT)': 'Comment',
        'Alt Address 1': 'Offsite Address Line 1',
        'Alt Address 2': 'Offsite Address Line 2',
        'Alt Post Code': 'Offsite Post Code',
        'City': 'Offsite City',
        'Address 1': 'Onsite Address 1',
        'Address 2': 'Onsite Address 2',
        'Address 4': 'Onsite City',
        'Post Code': 'Onsite Post Code'})

print("processing normal_members")
normal_members = concat([
    normal_members,
    normal_members.apply(
        lambda row:
        {
            'Associate': False,
            'Offsite': True,
            'Address Line 1': row['Offsite Address Line 1'],
            'Address Line 2': row['Offsite Address Line 2'],
            'City': row['Offsite City'],
            'Post Code': row['Offsite Post Code'],
        } if row['Diff Address'] == 'Y' else {
            'Associate': False,
            'Offsite': False,
            'Address Line 1': row['Onsite Address 1'],
            'Address Line 2': row['Onsite Address 2'],
            'City': row['Onsite City'],
            'Post Code': row['Onsite Post Code'],
        },
        axis=1,
        result_type='expand'
    )],
    axis='columns')\
    .drop(['Diff Address', 'Serial Number', 'Alt Address 4', 'Block Name', 'Barbican Address', 'Last Sub Paid',
           'Card Issuance', 'Payment Type', 'BANK ACCOUNT', 'Offsite Address Line 1', 'Offsite Address Line 2',
           'Offsite City', 'Offsite Post Code', 'Onsite Address 1', 'Onsite Address 2', 'Onsite City',
           'Onsite Post Code', 'Block Code'],
          axis=1)\


print("loading associate_members")
associate_members = read_excel(files_dir + '\\Member Details.xlsm', 'Associates')
print("processing associate_members")
associate_members[['Associate', 'Post Zone', 'Offsite', 'Country']] = [True, 'UK', True, 'United Kingdom']
associate_members = associate_members.rename(columns={
    'Contact Title 1': 'Title 1',
    'Contact first name 1': 'First name 1',
    'Contact middlename 1': 'Middlename 1',
    'Contact surname 1': 'Surname 1',
    'Company': 'Alt Addressee',
    'Alt Address 1': 'Address Line 1',
    'Alt Address 2': 'Address Line 2',
    'Alt Post Code': 'Post Code',
    'Alt Address 4': 'City'
}).drop(['Serial Number', 'Last Sub Paid', 'Amount Paid', 'Alt Address 3'], axis=1)

print("processing all_members")
all_members = concat([normal_members, associate_members]).set_index('Membership Number')

print("processing members")
members = all_members[
    [
        col for
        col in
        all_members.columns
        if search('E mail|((Title|First name|Middlename|Surname|Telephone|E mail|Mailing List) \\d)$', col)
    ]]\
    .stack()\
    .reset_index(1)\
    .rename(columns={
        'level_1': 'Field Name',
        0: 'Value'
    }).apply(
    lambda row: {
        'Field Name': row['Field Name'][0:len(row['Field Name']) - 2]
        if row['Field Name'][-1].isdigit()
        else row['Field Name'],
        'Count': int(row['Field Name'][len(row['Field Name']) - 1])
        if row['Field Name'][-1].isdigit()
        else 1,
        'Value': row['Value']
    },
    axis=1,
    result_type='expand'
).reset_index(names='Membership ID')\
    .set_index(['Membership ID', 'Count', 'Field Name'])\
    .unstack(level=2)\
    .reset_index(level=1, col_level=1)
members.columns = members.columns.droplevel(0)
members = members\
    .applymap(trim_normalise_string)[
        members['First name'].notna() | members['Middlename'].notna() | members['Surname'].notna()]
members = concat(
    [
        members,
        members.apply(
            lambda row: {
                'Formal Name': create_formal_name(row['Title'], row['First name'], row['Surname']),
                'Informal Name': create_informal_name(row['Title'], row['First name'], row['Surname']),
                'Full Name': create_full_name(row['Title'], row['First name'], row['Middlename'], row['Surname']),
            },
            axis=1,
            result_type='expand'
        )
    ],
    axis='columns'
).rename(columns={'E mail': 'Email'})


print("loading issuance")
issuance =\
    read_excel(files_dir + "\\Card Issuances.xlsx", "Card Issuance")
print("processing issuance")
current_members_accounts = \
    issuance[issuance['Card End Date'] >= NOW][['Membership ID']]\
    .set_index('Membership ID')


print("processing accounts")
accounts = all_members\
    .join(members
          .reset_index(names='Membership ID')
          .set_index(['Membership ID', 'Count'])
          .groupby(['Membership ID']).agg(**{
                'Normal Addressee': ('Formal Name', create_addressee),
                'Informal Greeting': ('Informal Name', create_informal_greeting)
          })
          .join(all_members[['Alt Addressee']])
          .reset_index(names='Membership ID')
          .apply(
                lambda row: {
                    'Membership ID': row['Membership ID'],
                    'Informal Greeting': row['Informal Greeting'],
                    'Addressee': row['Normal Addressee'] if isna(row['Alt Addressee']) else row['Alt Addressee'],
                    'Current Member': row['Membership ID'] in current_members_accounts.index,
                },
                axis=1,
                result_type='expand'
          )
          .set_index('Membership ID')
          )[
        [
            'Date first joined', 'Cancelled', 'Treasurere ref', 'Payment Type', 'Comment', 'Property Code', 'Old Code',
            'Offsite', 'Post Zone', 'Address Line 1', 'Address Line 2', 'City', 'County', 'Post Code', 'Country',
            'Associate', 'Informal Greeting', 'Addressee', 'Current Member'
        ]]

print("processing current_members")
current_members = accounts[accounts['Current Member'] == True].drop('Current Member', axis=1)
