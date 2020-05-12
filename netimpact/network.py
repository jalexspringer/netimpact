class Network:
    network_name = 'NETWORK'
    def __init__(self):
        '''
        Pass network API credentials/secrets/tokens to the object. Use them in the two required functions below to get publishers and transactions
        '''
        pass

    def get_pubs(self, acct):
        publishers = [{'name':'example','id':'example_id','site_url':'example_url'}] # Replace this with whatever network call needs to happen to get a pub list
        pub_list = []
        for p in publishers:
            pub = {
                'name': {p['name']},
                'id': p['id'],
                'site': p['site_url']
            }
            pub_list.append(pub)
        return pub_list

    def get_all_transactions(self, acct, start, end, status):
        transactions = [{'name':'example','id':'example_id','site_url':'example_url'}] # Replace this with whatever network call needs to happen to get a pub list

        transaction_list = []
        for t in transactions:
            transaction = {
                    'transactionDate': t['action_time'],
                    'id': t['id'],
                    'saleAmount': {'amount':t['order_total'], 'currency':t['order_currency']},
                    'commissionAmount': {'amount':t['commission'], 'currency':t['commission_currency']},
                    'publisherId': t['website_id'],
                    'status': t['customer_status'],
                    'voucherCode': t['promocode'],
                    'customerCountry': t['action_country'],
                    'advertiserCountry': t['action_country']
                }
            transaction_list.append(transaction)
        return transaction_list

