from members import *


def output_members(mbrs, prefix):
    mbrs.drop(columns=['Mailing List', 'Associate'])\
        .to_csv(
        prefix + 'members' + NOW.isoformat().replace(':', '-') + '.csv',
        index=False
    )


def output_different_member_types(mbrs):
    output_members(mbrs[mbrs['Associate']], 'associate_')
    output_members(mbrs[~mbrs['Associate']], 'normal_')
    output_members(mbrs[notna(mbrs['Mailing List']) & mbrs['Mailing List']], 'mailing_list_')


def gather_member_details(for_date: Timestamp, mbrs: DataFrame, accs: DataFrame, issu: DataFrame):
    from_date = for_date - offsets.MonthBegin() * 13

    curr = concat([
        issu[issu['Card End Date'] > from_date].set_index('Membership ID')[[]],
        accs[accs['Date first joined'] > from_date][[]]
    ])
    curr = curr.groupby(curr.index).first()

    return mbrs[notnull(mbrs['Email'])][['Email', 'Informal Name', 'Full Name', 'Mailing List']].join(
        accounts[['Associate']]
    )   .join(curr, how='inner')\
        .sort_index()\
        .reset_index(names='Membership ID')


member_email_details = gather_member_details(NOW, members, accounts, issuance)

output_different_member_types(member_email_details)
