#!/usr/bin/env python3
import requests
import logging
import time

class AWin:
    """AWin object for interacting with the AWin advertiser API

    Arguments:
        token {string} -- AWin API auth token
    """    
    network_name = 'Awin'
    def __init__(self, token):    
        self.token = token

    def get_pubs(self, acct):
        """GET the list of websites connected to the Awin program as publishers and extract the relevant partner information

        Arguments:
            acct {string} -- Awin account ID

        Returns:
            list -- A list of dictionaries containing publisher name, id, and site url
        """         
        url = f"https://api.awin.com/advertisers/{acct}/publishers?&accessToken={self.token}"
        r = requests.get(url)
        while r.status_code == 429:
            time.sleep(60)
            r = requests.get(url)
        pub_list = r.json()
        for p in pub_list:
            p['site'] = 'https://www.awinpub.com'
        return pub_list

    def transaction_request(self, acct, start, end, status):
        """GET all transactions over the given time period with a given status and transform
        the transactions into dictionaries consumable by the Impact transaction upload object

        Arguments:
            acct {string} -- Awin account ID
            start {datetime} -- Datetime object for the beginning of the transaction time period
            end {datetime} -- Datetime object for the end of the transaction time period
            status {string} -- Status of the transactions to be pulled - one of [pending, approved, declined]. Defaults to pending if incorrect input is given.


        Returns:
            list -- List of dictionaries containing transaction data
        """
        url = f"https://api.awin.com/advertisers/{acct}/transactions/?startDate={start}T00%3A00%3A00&endDate={end}T23%3A59%3A59&timezone=GMT&accessToken={self.token}&status={status}"
        if status == 'approved' or status == 'declined':
            url += '&dateType=validation'
        r = requests.get(url)
        while r.status_code == 429:
            time.sleep(60)
            r = requests.get(url)
        return r.json()

    def get_all_transactions(self, acct, start, end, status):
        """GET all transactions over the given time period with a given status and transform
        the transactions into dictionaries consumable by the Impact transaction upload object
        TODO :: Add flexibility for the identifier of new vs returning and device type

        Arguments:
            acct {string} -- Awin account ID
            start {datetime} -- Datetime object for the beginning of the transaction time period
            end {datetime} -- Datetime object for the end of the transaction time period
            status {string} -- Status of the transactions to be pulled - one of [pending, approved, declined]. Defaults to pending if incorrect input is given.


        Returns:
            list -- List of dictionaries containing transaction data
        """     

        transaction_list = self.transaction_request(acct, start, end, status)
        for t in transaction_list:
            if status == 'pending':
                try:
                    for cg in t['transactionParts']:
                        if 'New Customer' in cg['commissionGroupName']:
                            t['status'] = 'New'
                        else:
                            t['status'] = 'Returning'
                except KeyError as e:
                    logging.warn(f'No CG on transaction {t["id"]}')
                    t['status'] = 'Returning'
            if 'Mobile' in t['type']:
                t['device'] = 'Mobile'
            else:
                t['device'] = 'Desktop'

        return transaction_list

    def get_agg_transactions(self, acct, start, end):
        url = f"https://api.awin.com/advertisers/{acct}/reports/publisher?startDate={start}&endDate={end}&timezone=GMT&accessToken={self.token}"