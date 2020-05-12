import requests
import http.client
import mimetypes
import json
from urllib.parse import urlencode
from base64 import b64encode

class Admitad:
    """Admitad object for interacting with the Admitad advertiser API

    Arguments:
        client_id {string} -- Admitad client ID
        client_secret {string} -- Admitad client secret

    Keyword Arguments:
        token {str} -- If provided this string is used instead of generating a new token (default: {False})
    """    
    network_name = 'Admitad'
    def __init__(self, client_id, client_secret, token=False):    
        super().__init__()
        self.client_id = client_id
        self.client_secret = client_secret
        if token:
            self.headers = {'Authorization': f'Bearer {token}'}
        else:
            self.headers = {'Authorization': f'Bearer {self.get_token()}'}

    def get_token(self):
        """Generate authorisation token from Admitad

        Returns:
            string -- Admitad access token
        """        
        auth = b64encode(bytes((self.client_id + ':' + self.client_secret),'utf-8'))

        conn = http.client.HTTPSConnection("api.admitad.com")
        payload = ''

        headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {auth.decode("utf-8")}'
        }
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'scope': 'advertiser_websites advertiser_statistics advertiser_info'
         }
        encoded_args = urlencode(data)
        conn.request("POST", f"/token/?{encoded_args}", payload, headers)
        res = conn.getresponse()
        data = res.read()
        return json.loads(data)['access_token']


    def get_pubs(self, acct):
        """GET the list of websites connected to the Admitad program as publishers and extract the relevant partner information

        Arguments:
            acct {string} -- Admitad account ID

        Returns:
            list -- A list of dictionaries containing publisher name, id, and site url
        """        
        conn = http.client.HTTPSConnection("api.admitad.com")
        payload = ''
        conn.request("GET", f"/advertiser/{acct}/websites/?limit=1000", payload, self.headers)
        res = conn.getresponse()
        data = res.read()
        admitad_sites = json.loads(data.decode("utf-8"))['results']
        pub_list = []
        for ap in admitad_sites:
            pub = {
                'name': f"{ap['website']['name']} - {ap['user']['name']}",
                'id': ap['website']['id'],
                'site': ap['website']['site_url']
            }
            pub_list.append(pub)
        return pub_list

    def get_all_transactions(self, acct, start, end, status):
        """GET all transactions over the given time period with a given status and transform
        the transactions into dictionaries consumable by the Impact transaction upload object
        TODO :: Add flexibility for the identifier of new vs returning and device type

        Arguments:
            acct {string} -- Admitad account ID
            start {datetime} -- Datetime object for the beginning of the transaction time period
            end {datetime} -- Datetime object for the end of the transaction time period
            status {string} -- Status of the transactions to be pulled - one of [pending, approved, declined]. Defaults to pending if incorrect input is given.

        Returns:
            list -- List of dictionaries containing transaction data
        """        
        if status == 'pending':
            st = 0
        elif status == 'approved':
            st = 1
        elif status == 'declined':
            st = 2
        else:
            st = 0
        conn = http.client.HTTPSConnection("api.admitad.com")
        payload = ''
        conn.request("GET", f"/advertiser/{acct}/statistics/actions/?start_date={start}&end_date={end}&status={st}&limit=5000", payload, self.headers)
        res = conn.getresponse()
        data = res.read()
        transactions = json.loads(data.decode("utf-8"))['results']
        transaction_list = []
        for t in transactions:
            try:
                if t['product_id'] == 1636:
                    nvr = 'New'
                else:
                    nvr = 'Returning'
            except KeyError as e:
                nvr = 'Returning'
            transaction = {
                'transactionDate': t['action_time'],
                'id': t['id'],
                'saleAmount': {'amount':t['order_sum'], 'currency':t['currency']},
                'commissionAmount': {'amount':t['payment_webmaster'], 'currency':t['currency']},
                'publisherId': t['website_id'],
                'status': nvr,
                'voucherCode': t['promocode'],
                'customerCountry': t['action_country'],
                'advertiserCountry': t['action_country']
            }
            try:
                if 'мобильный' in t['product_name']:
                    transaction['device'] = 'Mobile'
                else:
                    transaction['device'] = 'Desktop'
                transaction_list.append(transaction)
            except TypeError as e:
                print(t)
                transaction['device'] = 'Desktop'
        return transaction_list