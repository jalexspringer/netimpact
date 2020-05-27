from datetime import datetime, timedelta
from itertools import zip_longest
import json
import time
import os
import csv
import ftplib
from pathlib import Path
from urllib import request
import logging

import requests

class Impact:
    def __init__(self,
                sid,token,
                ftp_un,
                ftp_p,
                program_id,
                desktop_action_tracker_id,
                mobile_action_tracker_id,
                reload_groups=True):
        """Impact Partnership Cloud object for creating and modifying partners, transactions, and partner groups in a given program

        Arguments:
            sid {string} -- Impact account SID
            token {string} -- Impact account token
            ftp_un {string} -- Impact program FTP credentials
            ftp_p {string} -- Impact program FTP credentials
            program_id {string} -- Impact program ID
            desktop_action_tracker_id {string} -- Event type/action tracker ID for the desktop event
            mobile_action_tracker_id {string} -- Event type/action tracker ID for the mobile event

        Keyword Arguments:
            reload_groups {bool} -- Get group IDs for the account. Can take some time as the Impact Groups list API endpoint also lists all partners. (default: {True})
        """                   
        self.root_adv_api = f'https://{sid}:{token}@api.impact.com'
        self.adv_api_stem = f'{self.root_adv_api}/Advertisers/{sid}/'
        self.ftp_un = ftp_un
        self.ftp_p = ftp_p
        self.program_id = program_id
        self.desktop_action_tracker_id = desktop_action_tracker_id
        self.mobile_action_tracker_id = mobile_action_tracker_id
        self.io_id = self.get_contract() # Get Impact IO ID - note that this only works at the moment with a program with a single IO TODO :: Allow for IO selection on partner creation
        self.existing_partner_dict = self.get_partners()

        if reload_groups:
            self.groups_dict = self.get_groups()
        else:
            self.groups_dict = {'Linkshare UK': '17203',
                                'Linkshare US': '17204',
                                'Linkshare AU': '17205',
                                'Linkshare Asia': '17206',
                                'Linkshare Concierge': '17207',
                                'Admitad RU': '17213',
                                'Awin FR': '17214',
                                'Awin IE': '17217',
                                'Awin BE': '17218',
                                'Awin NL': '17219',
                                'Awin SE': '17225',
                                'Awin NO': '17226',
                                'Awin DK': '17227',
                                'Awin FI': '17228',
                                'Awin IT': '17229',
                                'Awin EE': '17230',
                                'Awin AU': '17231',
                                'Awin ES': '17232',
                                'Awin DE': '17233',
                                'Awin AT': '17235',
                                'Awin CH': '17236',
                                'Awin ROW': '17237',
                                'Awin UK': '17238',
                                'Awin': '17260',
                                'Linkshare': '17261',
                                'Admitad': '17262'}

    def create_group(self, group_name):
        """Creates new partner group

        Arguments:
            group_name {string} -- Group name

        Returns:
            string -- Group ID
        """        
        import requests
        url = self.adv_api_stem + f'Campaigns/{self.program_id}/MediaPartnerGroups'
        params = {
            'Name': group_name
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept':'application/json'}
        return requests.post(url, params=params, headers=headers).json()['Uri'].split('/')[-1]

    def create_partner(self, pub, io_id, group_name, network_name, dupe=0):
        """Create new partner attached to the program contract and tagged with the network and network program as the groups with.
        Note that the Impact partner creation API does not currently correctly assign groups on partner creation, and it returns
        a 500 error on successful creation instead of the partner ID as listed in the documentation.

        Arguments:
            pub {dict} -- Publisher dictionary
            io_id {string} -- Contract/Insertion Order ID
            group_name {string} -- Group name
            network_name {string} -- Network name

        Keyword Arguments:
            dupe {int} -- counter that determines a string to add to the end of the publisher name for uniqueness (default: {0})

        Returns:
            requests.response  -- Impact API response to the partner creation POST
        """        
        url = self.adv_api_stem + 'MediaPartners'
        publisher_account_name = pub['name'] + f' - {network_name} - AS'
        if dupe == 1:
            publisher_account_name = f'{pub["name"]} - {network_name} - {group_name.split(" ")[-1]} {randomString(2)}'
        elif dupe == 2:
            logging.warning(f'Second naming error creating partner record {pub["name"]}')
            publisher_account_name += group_name.split('_')[-1] + '2'
        params = {
            'AccountName': publisher_account_name,
            'AddressLine1': '123 Mickey Mouse Lane',
            'City': 'Disneyland',
            'PostalCode': '93101',
            'Country': 'UK',
            'PhoneCountry': 'UK',
            'PhoneNumber': '4343861194',
            'PromotionalMethods': 'CONTENT',
            'InsertionOrderId': int(io_id),
            'TaxId': '123456689',
            'OrganizationType': 'OTHER',
            'Username': randomString(10),
            'FirstName': 'Minnie',
            'LastName': 'Mouse',
            'Password': 'l3tme1n!',
            'EmailAddress': 'minnielovesmicky@gmail.com',
            'MPGroupIds': self.groups_dict[group_name],
            'MPValue1': pub["id"],
            'MPValue2': pub['name']
        }
        if  pub['site'].startswith('http'):
            params['Website'] = pub['site']
        else:
            params['Website'] = 'http://' + pub['site']
        headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept':'application/json'}
        return requests.post(url, params=params, headers=headers)

    def get_groups(self):
        """Pull the list of groups on the program and creates a dict for easy lookup

        Returns:
            dict -- {Group Name : Group ID}
        """
        url = self.adv_api_stem + f'Campaigns/{self.program_id}/MediaPartnerGroups.json'
        r = requests.get(url)
        groups_dict = {}
        while r.status_code == 429:
            time.sleep(120)
            r = requests.get(url)
        for g in r.json()['Groups']:
            groups_dict[g['Name']] = g['Id']
        print(groups_dict)
        return groups_dict

    def add_to_group(self, group_id, partner_ids):
        """Add the list of partners to a specified group

        Arguments:
            group_id {string} -- Group ID
            partner_ids {list} -- List of partner IDs to add to the group
        """
        url = self.adv_api_stem + f'Campaigns/{self.program_id}/MediaPartnerGroups/{group_id}'
        for p in zip_longest(*(iter(partner_ids),) * 500):
            try:
                ps = ",".join(p)
            except TypeError:
                pa=''
                for a in p:
                    if a is not None:
                        pa += a + ','
                ps = pa[:-1]
            headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept':'application/json'}
            params = {
                'MediaPartnersAdd': ps
            }
            r = requests.put(url, params=params, headers=headers)

    def get_partners(self):
        """Pull the full list of partners linked to the program and create a dict with partner IDs and existing network IDs

        Returns:
            dict -- {Network Partner ID : Impact Partner ID}
        """
        logging.debug('Refreshing full impact partner list.')
        url = self.adv_api_stem + f'MediaPartners?CampaignId={self.program_id}&PageSize=1000'
        existing_partner_dict = {}

        headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept':'application/json'}
        while True:
            r = requests.get(url, headers=headers)
            while r.status_code == 429:
                time.sleep(30)
                r = requests.get(url, headers=headers)
            while r.status_code == 520:
                time.sleep(30)
                r = requests.get(url, headers=headers)
            rj = r.json()

            printProgressBar(int(rj['@page']), int(rj['@numpages']), prefix = f'Page {int(rj["@page"])}/{int(rj["@numpages"])}:', suffix = 'Complete', length = 50)
            time.sleep(2)
            for g in r.json()['MediaPartners']:
                if g['MPValue1'] != '':
                    existing_partner_dict[str(g['MPValue1'])] = g['Id']
            if rj['@page'] != rj['@numpages']:
                url = self.root_adv_api + rj['@nextpageuri']
            else:
                break
        return existing_partner_dict

    def get_contract(self):
        """Get Impact IO ID - note that this only works at the moment with a program with a single IO TODO :: Allow for IO selection on partner creation

        Returns:
            string -- Inserion Order/Contract ID
        """        
        url = self.adv_api_stem + f'Campaigns/{self.program_id}/Contracts.json'
        r = requests.get(url)
        while r.status_code == 429:
            time.sleep(60)
            r = requests.get(url)
        return r.json()['Contracts'][0]['TemplateTerms']['TemplateId']

    def partner_update(self, group_name, network_name, network_pubs):
        """Iterate through a list of publisher dicts and create (or try to create publishers). Get new publisher IDs in the process and add to a list to assign correct groups
        Check for new pubs, update groups. Impact's MP creation does not return the ID so if there are new partners need to re-run the entire partner lookup to find the new partners
        MPValue1 is the network partner ID

        Arguments:
            group_name {string} -- Group name (network program name)
            network_name {string} -- Network name
            network_pubs {list} -- list of dicts of publisher information of pubs to create

        Returns:
            tuple -- (Total new partners created, Total partners in program processed)
        """
        new_partners = []
        partnercount = 0
        group_add = []
        for pub in network_pubs:
            partnercount += 1
            printProgressBar(partnercount, len(network_pubs), prefix = f'Updating publisher {partnercount}/{len(network_pubs)}:', suffix = 'Complete', length = 50)
            try:
                group_add.append(self.existing_partner_dict[str(pub["id"])])
            except KeyError as e:
                logging.info(f'Creating new partner, {pub}')
                r = self.create_partner(pub, self.io_id, group_name, network_name)
                while r.status_code == 429:
                    logging.error(r.text)
                    time.sleep(300)
                    pass
                else:
                    if r.status_code == 400:
                        logging.warning(r.text)
                        r = self.create_partner(pub, self.io_id, group_name, network_name, 1)
                        if r.status_code == 400:
                            r = self.create_partner(pub, self.io_id, group_name, network_name, 2)
                        new_partners.append(pub['name'])
                    else:
                        new_partners.append(pub['name'])
        logging.info(f'Adding partners to group {self.groups_dict[group_name]}')

        self.add_to_group(self.groups_dict[group_name], group_add)
        self.add_to_group(self.groups_dict[network_name], group_add)

        return new_partners, partnercount

    def new_publisher_validation(self, account_id, account_name, network):
        """Uses the network object to get full publisher list and add new publishers to the Impact program with the correct group names (Network name, network program name)

        Arguments:
            account_id {string} -- Network specific authentication token to get publisher list
            account_name {string} -- Network program/account name
            network {object} -- Network object (currently one of AWin, Admitad, Linkshare)
        """
        network_pubs = network.get_pubs(account_id)
        logging.info(f'Updating partner list for {account_name}')
        new_partners, total_count = self.partner_update(account_name, network.network_name, network_pubs)
        message = f'New partners in {account_name}: {len(new_partners)}. Total partner count: {total_count}\n'
        logging.info(message)
        if len(new_partners) > 0:
            self.existing_partner_dict = self.get_partners()
            self.partner_update(account_name, network.network_name, network_pubs)

    def new_transaction_lists(self, new_transactions, account_name, network_name):
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
        headrow = ["CampaignId","ActionTrackerId","EventDate","OrderId","MediaPartnerId",'CustomerStatus',"CurrencyCode","Amount","Category","Sku","Quantity",'Text1','PromoCode','Country','OrderLocation','Text2','Date1','Note','Numeric1']
        t_list = []
        for t in new_transactions:
            try:
                mpid = self.existing_partner_dict[t['publisherId']]
            except KeyError as e:
                try:
                    mpid = self.existing_partner_dict[str(t['publisherId'])]
                except KeyError as e:
                    logging.warning(f'No valid partner {t["publisherId"]} found for {network_name} transaction {t["id"]}')
                    # mpid = 'NOMPID'
                    continue
            try:
                commission_rate = int(round(float(t['commissionAmount']['amount']) / float(t['saleAmount']['amount']), 2) * 100)
            except ZeroDivisionError as e:
                commission_rate = 0
            if t['device'] == 'Desktop':
                at_id = self.desktop_action_tracker_id
            elif t['device'] == 'Mobile':
                at_id = self.mobile_action_tracker_id

            transaction = [
                self.program_id,
                at_id,
                # '2020-05-03T23:59:59',
                # 'NOW',
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
                commission_rate
                ]
            t_list.append(transaction)
        return headrow, t_list

    def modified_transaction_lists(self, approved, declined):
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
                at_id = self.desktop_action_tracker_id
            elif t['device'] == 'Mobile':
                at_id = self.mobile_action_tracker_id
            transaction = [
                at_id,
                t['id'],
                t['saleAmount']['amount'],
                'VALIDATED_ORDER'
                ]
            t_list.append(transaction)
        for t in declined:
            if t['device'] == 'Desktop':
                at_id = self.desktop_action_tracker_id
            elif t['device'] == 'Mobile':
                at_id = self.mobile_action_tracker_id
            transaction = [
                at_id,
                t['id'],
                0,
                'RETURNED'
                ]
            t_list.append(transaction)

        return headrow, t_list

    def prepare_transactions(self, account_id, network, historical=False):
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
        if network.network_name == 'Awin':
            date_format = '%Y-%m-%d'
            start = (datetime.now() - timedelta(1)).strftime(date_format)
            end = (datetime.now() - timedelta(1)).strftime(date_format)
        elif network.network_name == 'Linkshare':
            date_format = '%Y-%m-%d'
            start = (datetime.now() - timedelta(2)).strftime(date_format)
            end = (datetime.now() - timedelta(1)).strftime(date_format)
        elif network.network_name == 'Admitad':
            date_format = '%d.%m.%Y'
            start = (datetime.now() - timedelta(2)).strftime(date_format)
            end = (datetime.now() - timedelta(1)).strftime(date_format)

        # if historical:
        #     start = (datetime.now() - timedelta(60)).strftime(date_format)
        #     end = (datetime.now() - timedelta(31)).strftime(date_format)
        #     for t in network.get_all_transactions(account_id, start, end, 'pending').json():
        #         pending.append(t)
        #     for t in network.get_all_transactions(account_id, start, end, 'approved').json():
        #         approved.append(t)
        #     for t in network.get_all_transactions(account_id, start, end, 'declined').json():
        #         declined.append(t)

        #     start = (datetime.now() - timedelta(30)).strftime(date_format)
        #     end = (datetime.now() - timedelta(1)).strftime(date_format)
        #     for t in network.get_all_transactions(account_id, start, end, 'pending').json():
        #         pending.append(t)
        if network.network_name == 'Linkshare':
            approved, pending, declined = network.get_all_transactions(account_id, start, end)
            declined = []

        else:            
            approved = network.get_all_transactions(account_id, start, end, 'approved')
            declined = network.get_all_transactions(account_id, start, end, 'declined')
            pending = network.get_all_transactions(account_id, start, end, 'pending')

        return approved, declined, pending, (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    def transactions_process(self, account_id, account_name, network, historical=False):
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

    def transactions_update(self, account_id, account_name, network, upload):
        """Orchestration function for the ETL pipeline. Runs required functions then uploads to the Impact batch conversion/modification FTP server for the account

        Arguments:
            account_id {string} -- Impact account ID
            account_name {string} -- Network program/account name
            network {object} -- Network object (currently one of AWin, Admitad, Linkshare)

        Keyword Arguments:
            test {bool} -- If True, does not upload the resulting transaction files to Impact and leaves them in the local 'transactions' folder for review (default: {False})
        """
        awin_call = 0 # AWin rate limits to 20 calls per minute.
        while awin_call > 17:
            time.sleep(60)
            awin_call = 0

        end = self.transactions_process(account_id,account_name, network)
        awin_call += 3

        file_path_p = Path(f'transactions/{end}/{account_name}_{end}_pending.csv')
        file_path_m = Path(f'transactions/{end}/{account_name}_{end}_mods.csv')

        if upload:
                try:
                    with ftplib.FTP('batch.impactradius.com', self.ftp_un, self.ftp_p) as ftp, open(file_path_p, 'rb') as file:
                        ftp.storbinary(f'STOR {file_path_p.name}', file)                
                        logging.info(f'{account_name} {end} pending transactions batch uploaded.')
                    os.remove(file_path_p)

                except ftplib.error_perm as e:
                    logging.error(f"FTP Upload error for {file_path_p}. Re-upload this file in a few minutes.")

                try:
                    with ftplib.FTP('batch.impactradius.com', self.ftp_un, self.ftp_p) as ftp, open(file_path_m, 'rb') as file:
                        ftp.storbinary(f'STOR {file_path_m.name}', file)                   
                        logging.info(f'{account_name} {end} modified transactions batch uploaded.')
                    os.remove(file_path_m)


                except ftplib.error_perm as e:
                    logging.error(f"FTP Upload error for {file_path_m}. Re-upload this file in a few minutes.")

    def get_all_transactions(self, acct, start=None, end=None, timeRange=None, status='pending'):
        transactions = [] # Replace this with whatever network call needs to happen to get a pub list
        url = f'{self.adv_api_stem}Reports/adv_action_listing.json'
    
        if status == 'pending':
            st = 'PENDING'
        elif status == 'approved':
            st = 'APPROVED'
        elif status == 'declined':
            st = 'REVERSED'
        else:
            st = 'PENDING'

        params ={
            'compareEnabled':'false',
            'SUPERSTATUS_MS':st,
            'SUBAID':{self.program_id},
        }

        if start:
            params['START_DATE']=start,
            params['END_DATE']=end,
            params['timeRange']='CUSTOM'
        elif timeRange:
            params['timeRange']=timeRange,
        

        headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept':'application/json'}

        while True:
            r = requests.get(url, headers=headers, params=params)
            while r.status_code == 429:
                time.sleep(30)
                r = requests.get(url, headers=headers)
            while r.status_code == 520:
                time.sleep(30)
                r = requests.get(url, headers=headers)
            rj = r.json()

            printProgressBar(int(rj['@page']), int(rj['@numpages']), prefix = f'Page {int(rj["@page"])}/{int(rj["@numpages"])}:', suffix = 'Complete', length = 50)
            time.sleep(2)
            for t in r.json()['Records']:
                transaction = {
                    'transactionDate': t['Action_Date'],
                    'id': t['OID'],
                    'saleAmount': {'amount':t['Sale_Amount'], 'currency':t['Original_Currency']},
                    'commissionAmount': {'amount':t['Payout'], 'currency':t['Original_Currency']},
                    'publisherId': t['MP_Id'],
                    'status': t['Customer_Status'],
                    'voucherCode': t['PromoCode'],
                    'customerCountry': t['CustomerCountry'],
                    'advertiserCountry': t['CustomerCountry']
                }
                if 'Mobile' in t['Action_Tracker']:
                    transaction['device'] = 'Mobile'
                else:
                    transaction['device'] = 'Desktop'
                transactions.append(transaction)
            if rj['@page'] != rj['@numpages']:
                url = self.root_adv_api + rj['@nextpageuri']
            else:
                break

        return transactions


# Helper functions
def pretty_print_POST(req):
    print('{}\n{}\r\n{}\r\n\r\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
    ))

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()

def randomString(stringLength=4):
    import string
    import random
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

# class Clicker():
#     def __init__(self,tracking_link):
#         self.tracking_link = tracking_link

#         def fire_click(self, mpid, user_agent):
#             to_click = self.tracking_link.format(mpid)
#             print(to_click)
#             req = request.Request(to_click, headers=user_agent)
#             landing_page = request.urlopen(req).geturl()
#             cid = landing_page.split('irclickid=')[1].split('&')[0]
#             print(landing_page)
#             return cid, landing_page

#         def create_clicks(self, account_id, existing_partner_dict):
#             start = (datetime.now() - timedelta(0)).strftime('%Y-%m-%d')
#             end = (datetime.now() - timedelta(0)).strftime('%Y-%m-%d')
#             agg_transactions = get_agg_transactions(account_id, start, end).json()
#             print(agg_transactions)
#             user_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'}

#             for a in agg_transactions:
#                 try:
#                     mpid = existing_partner_dict[str(a['publisherId'])]
#                     clickid, lp = self.fire_click(mpid, user_agent)
#                     time.sleep(.5)
#                     new_clickid, lp = self.fire_click(mpid, user_agent)
#                     logging.info(f'Sending {mpid} clicks - {a["clicks"]}')
#                     for c in range(a['clicks']-1):
#                         time.sleep(.5)
#                         while clickid == new_clickid:
#                             new_clickid, lp = self.fire_click(mpid, user_agent)
#                         printProgressBar(c+1, a['clicks'],
#                                         prefix=f'Sending {mpid} click {c+1} of {a["clicks"]}',
#                                         suffix=f'{lp}',
#                                         length=50)

#                 except KeyError as e:
#                     logging.info(f'{a["publisherId"]} click active but not in program')
    # name = 'Awin_DE'
    # account_id = 'XXXX'
    # threads = []
    # # for name,account_id in account_ids.items():
    #     logging.info(f'Create and start thread for {name} click creation')
    #     x = threading.Thread(target=create_clicks, args=(account_id, existing_partner_dict))
    #     threads.append(x)
    #     x.start()

    # for index, thread in enumerate(threads):
    #     logging.info("Main    : before joining thread %d.", index)
    #     thread.join()
    #     logging.info("Main    : thread %d done", index)