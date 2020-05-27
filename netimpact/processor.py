import logging
import os
import csv
from datetime import datetime, timedelta

def prepare_transactions(account_id, network, time_period='yesterday', start=None, end=None):
    """Extract transactions from the network object methods (date formats are specific to each network) and pass them to the transformation functions
    TODO :: move the date formatting to the network objects
    TODO :: historical data functions

    Arguments:
        account_id {string} -- Network specific authentication token to get publisher list
        network {object} -- Network object (currently one of AWin, Admitad, Linkshare)
    Keyword Arguments: 
        time_period {string} -- One of ['yesterday'] (default: {False})

    Returns:
        tuple -- (list of approved transactions, list of declined, list of pending, end datetime object) (all amended to fit Impact formatting requirements)
    """
    approved = []
    pending = []
    declined = []
    logging.info(f'Getting {network.network_name} transactions and modifications for account {account_id}')

    if time_period == 'yesterday':
        delta = 1
    else:
        delta = 0

    if network.network_name == 'Awin':
        date_format = '%Y-%m-%d'
        start = (datetime.now() - timedelta(delta)).strftime(date_format)
        end = (datetime.now() - timedelta(delta)).strftime(date_format)
    elif network.network_name == 'Linkshare':
        date_format = '%Y-%m-%d'
        start = (datetime.now() - timedelta(delta - 1)).strftime(date_format)
        end = (datetime.now() - timedelta(delta)).strftime(date_format)
    elif network.network_name == 'Admitad':
        date_format = '%d.%m.%Y'
        start = (datetime.now() - timedelta(delta - 1)).strftime(date_format)
        end = (datetime.now() - timedelta(delta)).strftime(date_format)

    if network.network_name == 'Linkshare':
        approved, pending, declined = network.get_all_transactions(account_id, start, end)
        declined = []

    else:            
        approved = network.get_all_transactions(account_id, start, end, 'approved')
        declined = network.get_all_transactions(account_id, start, end, 'declined')
        pending = network.get_all_transactions(account_id, start, end, 'pending')

    return approved, declined, pending, (datetime.now() - timedelta(delta)).strftime('%Y-%m-%d')


def transactions_process(account_id, account_name, network, historical=False):
    """Main function running the extract and transform process for the network transaction data. Writes the results to CSV ready for upload.

    Arguments:
        account_id {string} -- Impact account ID
        account_name {string} -- Network program/account name
        network {object} -- Network object (currently one of AWin, Admitad, Linkshare)

    Keyword Arguments:
        historical {bool} -- Not implemented (default: {False})

    Returns:
        datetime -- End date
    """        
    approved, declined, pending, end = self.prepare_transactions(account_id, network, historical)
    try:
        os.makedirs(f'transactions/{end}')
    except OSError as e:
        pass
    logging.info(f'Approved transactions {len(approved)}')
    logging.info(f'New pending transactions {len(pending)}')
    logging.info(f'Declined transactions {len(declined)}')

    headrow, t_list = self.new_transaction_lists(pending, account_name, network.network_name)
    with open(f'transactions/{end}/{account_name}_{end}_pending.csv' , 'w', newline="") as f:
        csvwriter = csv.writer(f, delimiter = ',')
        csvwriter.writerow(headrow)
        csvwriter.writerows(t_list)

    headrow, t_list = self.modified_transaction_lists(approved, declined)
    with open(f'transactions/{end}/{account_name}_{end}_mods.csv' , 'w', newline="") as f:
        csvwriter = csv.writer(f, delimiter = ',')
        csvwriter.writerow(headrow)
        csvwriter.writerows(t_list)
    return end