import logging
import os
import csv
from datetime import datetime, timedelta
from pathlib import Path

def new_transaction_lists(impact, t_lists, account_name, network_name):
    """Transforms new transaction records by adding the Impact tracker ID for the environment (desktop/mobile),
        finding the correct Impact partner  ID from the network publisher ID, and determining the commission rate.
        See README for contract payout groups requirements for correct payments.

    Arguments:
        new_transactions {list} -- list of transaction dicts. See below for required key value pairs in the dictionary
        account_name {string} -- Network program/account name
        network_name {string} -- Network name

    Returns:
        tuple -- (list of Impact conversions upload headers, list of amended transaction dicts)
    """
    headrow = ["CampaignId","ActionTrackerId","EventDate","OrderId","MediaPartnerId",'CustomerStatus',"CurrencyCode","Amount","Category","Sku","Quantity",'Text1','PromoCode','Country','OrderLocation','Text2','Date1','Note','Numeric1','OrderStatus','VoucherCode']
    t_list = []
    for l in t_lists:
        for t in l:
            try:
                mpid = impact.existing_partner_dict[t['publisherId']]
            except KeyError as e:
                try:
                    mpid = impact.existing_partner_dict[str(t['publisherId'])]
                except KeyError as e:
                    logging.warning(f'No valid partner {t["publisherId"]} found for {network_name} transaction {t["id"]}')
                    # mpid = 'NOMPID'
                    continue
            try:
                commission_rate = int(round(float(t['commissionAmount']['amount']) / float(t['saleAmount']['amount']), 2) * 100)
            except ZeroDivisionError as e:
                commission_rate = 0
            if t['device'] == 'Desktop':
                at_id = impact.desktop_action_tracker_id
            elif t['device'] == 'Mobile':
                at_id = impact.mobile_action_tracker_id

            transaction = [
                impact.program_id,
                at_id,
                t['transactionDate'],
                t['id'],
                mpid,
                t['status'],
                t['saleAmount']['currency'],
                t['saleAmount']['amount'],
                'cat',
                'sku',
                1,
                account_name,
                account_name,
                # t['voucherCode'],
                t['customerCountry'],
                t['customerCountry'],
                t['advertiserCountry'],
                t['transactionDate'],
                account_name,
                commission_rate,
                'pending',
                t['voucherCode']
                ]
            t_list.append(transaction)  
    return headrow, t_list

def modified_transaction_lists(impact, approved, declined):
    """Transforms modified transaction records by adding the Impact tracker ID for the environment (desktop/mobile) and the Reason Code for the modification.
    Updates the amount in the case of an approval and zeroes the amount in the case of a reversal.

    Arguments:
        approved {list} -- List of approved transactions (dictionaries)
        declined {list} -- List of declined/reversed transactions (dictionaries)

    Returns:
        tuple -- Two items, the Impact modification file standard headers, and the amended list of transactions to be modifed
    """        
    headrow = ['ActionTrackerID','Oid','Amount','Reason']
    t_list = []
    for t in approved:
        if t['device'] == 'Desktop':
            at_id = impact.desktop_action_tracker_id
        elif t['device'] == 'Mobile':
            at_id = impact.mobile_action_tracker_id
        transaction = [
            at_id,
            t['id'],
            t['saleAmount']['amount'],
            'VALIDATED_ORDER'
            ]
        t_list.append(transaction)
    for t in declined:
        if t['device'] == 'Desktop':
            at_id = impact.desktop_action_tracker_id
        elif t['device'] == 'Mobile':
            at_id = impact.mobile_action_tracker_id
        transaction = [
            at_id,
            t['id'],
            0,
            'RETURNED'
            ]
        t_list.append(transaction)

    return headrow, t_list

def prepare_transactions(impact, account_id, network, target_date):
    """Extract transactions from the network object methods (date formats are specific to each network) and pass them to the transformation functions
    TODO :: move the date formatting to the network objects
    TODO :: historical data functions

    Arguments:
        account_id {string} -- Network specific authentication token to get publisher list
        network {object} -- Network object (currently one of AWin, Admitad, Linkshare)
    Keyword Arguments:
        historical {bool} -- Not currently implemented (default: {False})

    Returns:
        tuple -- (list of approved transactions, list of declined, list of pending, end datetime object) (all amended to fit Impact formatting requirements)
    """
    approved = []
    pending = []
    declined = []
    logging.info(f'Getting {network.network_name} transactions and modifications for account {account_id}')
    start,end = network.date_formatter(target_date)

    if network.network_name == 'Linkshare':
        approved, pending, declined = network.get_all_transactions(account_id, start, end)
        declined = []

    else:            
        approved = network.get_all_transactions(account_id, start, end, 'approved')
        declined = network.get_all_transactions(account_id, start, end, 'declined')
        pending = network.get_all_transactions(account_id, start, end, 'pending')

    return approved, declined, pending, target_date

def transactions_process(impact, account_id, account_name, network, target_date):
    """Main function running the extract and transform process for the network transaction data. Writes the results to CSV ready for upload.

    Arguments:
        account_id {string} -- Network account ID
        account_name {string} -- Network program/account name
        network {object} -- Network object (currently one of AWin, Admitad, Linkshare)

    Keyword Arguments:
        historical {bool} -- Not implemented (default: {False})

    Returns:
        file_path_m {string} -- path to the modifications file 
        file_path_p {string} -- path to the pending transactions file 

    """        
    approved, declined, pending, end = prepare_transactions(impact, account_id, network, target_date)
    pending_folders = f'transactions/{end.year}/{end:%m}/{end:%d}/{account_name.replace(" ","_")}'
    mod_folders = f'modifications/{end.year}/{end:%m}/{end:%d}/{account_name.replace(" ","_")}'

    try:
        os.makedirs(pending_folders)
        os.makedirs(mod_folders)

    except OSError as e:
        pass

    file_path_p = Path(f'{pending_folders}/{account_name.replace(" ","_")}_{end:%Y-%m-%d}.csv')
    file_path_m = Path(f'{mod_folders}/{account_name.replace(" ","_")}_{end:%Y-%m-%d}.csv')

    logging.info(f'Approved transactions {len(approved)}')
    logging.info(f'New pending transactions {len(pending)}')
    logging.info(f'Declined transactions {len(declined)}')

    headrow, t_list = new_transaction_lists(impact, [approved, declined, pending], account_name, network.network_name)
    with open(file_path_p , 'w', newline="") as f:
        csvwriter = csv.writer(f, delimiter = ',')
        csvwriter.writerow(headrow)
        csvwriter.writerows(t_list)

    headrow, t_list = modified_transaction_lists(impact, approved, declined)
    with open(file_path_m , 'w', newline="") as f:
        csvwriter = csv.writer(f, delimiter = ',')
        csvwriter.writerow(headrow)
        csvwriter.writerows(t_list)

    return file_path_m, file_path_p