import requests
import csv
import logging, os
import json
from datetime import datetime, timedelta

class Linkshare:
    """Linkshare object for interacting with the Linkshare advertiser API"""
    network_name = 'Linkshare'
    countries = {"BD": "Bangladesh", "BE": "Belgium", "BF": "Burkina Faso", "BG": "Bulgaria", "BA": "Bosnia and Herzegovina", "BB": "Barbados", "WF": "Wallis and Futuna", "BL": "Saint Barthelemy", "BM": "Bermuda", "BN": "Brunei", "BO": "Bolivia", "BH": "Bahrain", "BI": "Burundi", "BJ": "Benin", "BT": "Bhutan", "JM": "Jamaica", "BV": "Bouvet Island", "BW": "Botswana", "WS": "Samoa", "BQ": "Bonaire, Saint Eustatius and Saba ", "BR": "Brazil", "BS": "Bahamas", "JE": "Jersey", "BY": "Belarus", "BZ": "Belize", "RU": "Russia", "RW": "Rwanda", "RS": "Serbia", "TL": "East Timor", "RE": "Reunion", "TM": "Turkmenistan", "TJ": "Tajikistan", "RO": "Romania", "TK": "Tokelau", "GW": "Guinea-Bissau", "GU": "Guam", "GT": "Guatemala", "GS": "South Georgia and the South Sandwich Islands", "GR": "Greece", "GQ": "Equatorial Guinea", "GP": "Guadeloupe", "JP": "Japan", "GY": "Guyana", "GG": "Guernsey", "GF": "French Guiana", "GE": "Georgia", "GD": "Grenada", "UK": "United Kingdom", "GA": "Gabon", "SV": "El Salvador", "GN": "Guinea", "GM": "Gambia", "GL": "Greenland", "GI": "Gibraltar", "GH": "Ghana", "OM": "Oman", "TN": "Tunisia", "JO": "Jordan", "HR": "Croatia", "HT": "Haiti", "HU": "Hungary", "HK": "Hong Kong", "HN": "Honduras", "HM": "Heard Island and McDonald Islands", "VE": "Venezuela", "PR": "Puerto Rico", "PS": "Palestinian Territory", "PW": "Palau", "PT": "Portugal", "SJ": "Svalbard and Jan Mayen", "PY": "Paraguay", "IQ": "Iraq", "PA": "Panama", "PF": "French Polynesia", "PG": "Papua New Guinea", "PE": "Peru", "PK": "Pakistan", "PH": "Philippines", "PN": "Pitcairn", "PL": "Poland", "PM": "Saint Pierre and Miquelon", "ZM": "Zambia", "EH": "Western Sahara", "EE": "Estonia", "EG": "Egypt", "ZA": "South Africa", "EC": "Ecuador", "IT": "Italy", "VN": "Vietnam", "SB": "Solomon Islands", "ET": "Ethiopia", "SO": "Somalia", "ZW": "Zimbabwe", "SA": "Saudi Arabia", "ES": "Spain", "ER": "Eritrea", "ME": "Montenegro", "MD": "Moldova", "MG": "Madagascar", "MF": "Saint Martin", "MA": "Morocco", "MC": "Monaco", "UZ": "Uzbekistan", "MM": "Myanmar", "ML": "Mali", "MO": "Macao", "MN": "Mongolia", "MH": "Marshall Islands", "MK": "Macedonia", "MU": "Mauritius", "MT": "Malta", "MW": "Malawi", "MV": "Maldives", "MQ": "Martinique", "MP": "Northern Mariana Islands", "MS": "Montserrat", "MR": "Mauritania", "IM": "Isle of Man", "UG": "Uganda", "TZ": "Tanzania", "MY": "Malaysia", "MX": "Mexico", "IL": "Israel", "FR": "France", "IO": "British Indian Ocean Territory", "SH": "Saint Helena", "FI": "Finland", "FJ": "Fiji", "FK": "Falkland Islands", "FM": "Micronesia", "FO": "Faroe Islands", "NI": "Nicaragua", "NL": "Netherlands", "NO": "Norway", "NA": "Namibia", "VU": "Vanuatu", "NC": "New Caledonia", "NE": "Niger", "NF": "Norfolk Island", "NG": "Nigeria", "NZ": "New Zealand", "NP": "Nepal", "NR": "Nauru", "NU": "Niue", "CK": "Cook Islands", "XK": "Kosovo", "CI": "Ivory Coast", "CH": "Switzerland", "CO": "Colombia", "CN": "China", "CM": "Cameroon", "CL": "Chile", "CC": "Cocos Islands", "CA": "Canada", "CG": "Republic of the Congo", "CF": "Central African Republic", "CD": "Democratic Republic of the Congo", "CZ": "Czech Republic", "CY": "Cyprus", "CX": "Christmas Island", "CR": "Costa Rica", "CW": "Curacao", "CV": "Cape Verde", "CU": "Cuba", "SZ": "Swaziland", "SY": "Syria", "SX": "Sint Maarten", "KG": "Kyrgyzstan", "KE": "Kenya", "SS": "South Sudan", "SR": "Suriname", "KI": "Kiribati", "KH": "Cambodia", "KN": "Saint Kitts and Nevis", "KM": "Comoros", "ST": "Sao Tome and Principe", "SK": "Slovakia", "KR": "South Korea", "SI": "Slovenia", "KP": "North Korea", "KW": "Kuwait", "SN": "Senegal", "SM": "San Marino", "SL": "Sierra Leone", "SC": "Seychelles", "KZ": "Kazakhstan", "KY": "Cayman Islands", "SG": "Singapore", "SE": "Sweden", "SD": "Sudan", "DO": "Dominican Republic", "DM": "Dominica", "DJ": "Djibouti", "DK": "Denmark", "VG": "British Virgin Islands", "DE": "Germany", "YE": "Yemen", "DZ": "Algeria", "US": "United States", "UY": "Uruguay", "YT": "Mayotte", "UM": "United States Minor Outlying Islands", "LB": "Lebanon", "LC": "Saint Lucia", "LA": "Laos", "TV": "Tuvalu", "TW": "Taiwan", "TT": "Trinidad and Tobago", "TR": "Turkey", "LK": "Sri Lanka", "LI": "Liechtenstein", "LV": "Latvia", "TO": "Tonga", "LT": "Lithuania", "LU": "Luxembourg", "LR": "Liberia", "LS": "Lesotho", "TH": "Thailand", "TF": "French Southern Territories", "TG": "Togo", "TD": "Chad", "TC": "Turks and Caicos Islands", "LY": "Libya", "VA": "Vatican", "VC": "Saint Vincent and the Grenadines", "AE": "United Arab Emirates", "AD": "Andorra", "AG": "Antigua and Barbuda", "AF": "Afghanistan", "AI": "Anguilla", "VI": "U.S. Virgin Islands", "IS": "Iceland", "IR": "Iran", "AM": "Armenia", "AL": "Albania", "AO": "Angola", "AQ": "Antarctica", "AS": "American Samoa", "AR": "Argentina", "AU": "Australia", "AT": "Austria", "AW": "Aruba", "IN": "India", "AX": "Aland Islands", "AZ": "Azerbaijan", "IE": "Ireland", "ID": "Indonesia", "UA": "Ukraine", "QA": "Qatar", "MZ": "Mozambique"}
    def __init__(self, transaction_report_name, pub_report_name):
        self.transaction_report_name = transaction_report_name
        self.pub_report_name = pub_report_name

    def get_pubs(self, acct_token):
        """GET the list of websites connected to the Linkshare program as publishers and extract the relevant partner information

        Arguments:
            acct_token {string} -- Linkshare account specific token

        Returns:
            list -- A list of dictionaries containing publisher name, id, and site url
        """    
        pub_list = []
        url = f'https://ran-reporting.rakutenmarketing.com/en/reports/{self.pub_report_name}/filters?'
        params = {
            'date_range': 'yesterday',
            'include_summary': 'N',
            'tz': 'GMT',
            'date_type':'transaction',
            'token': acct_token
        }
        r = requests.get(url, params=params)
        if r.status_code:
            data = r.content.decode('utf-8')
            cr = csv.reader(data.splitlines(), delimiter=',')
            results_list = list(cr)
            for lsp in results_list[1:]:
                pub = {
                    'name': lsp[4],
                    'id': lsp[3],
                    'site': lsp[5]
                }
                
                pub_list.append(pub)
            return pub_list
        else:
            logging.error('Linkshare Publisher Report request error')
            logging.error(r.text)
            return []

    def get_all_transactions(self, acct, start, end):
        """GET all transactions over the given time period with a given status and transform the transactions into dictionaries consumable by the Impact transaction upload object
        TODO :: Add flexibility for the identifier of new vs returning and device type

        Arguments:
            acct {string} -- Rakuten account token
            start {datetime} -- Datetime object for the beginning of the transaction time period
            end {datetime} -- Datetime object for the end of the transaction time period

        Returns:
            list -- List of dictionaries containing transaction data
        """       
        transactions = [{'name':'example','id':'example_id','site_url':'example_url'}] # Replace this with whatever network call needs to happen to get a pub list
        url = f'https://ran-reporting.rakutenmarketing.com/en/reports/{self.transaction_report_name}/filters?'
        params = {
            'start_date': start,
            'end_date': end,
            'include_summary': 'N',
            'tz': 'GMT',
            'date_type': 'transaction',
            'token': acct
            }
        r = requests.get(url, params=params)
        if r.status_code:
            data = r.content.decode('utf-8')
            cr = csv.reader(data.splitlines(), delimiter=',')
            transactions = list(cr)
        pending = []
        approved = []
        declined = []
        expected = ['\ufeffConsumer Country','Gross Commissions','Gross Sales','Order ID','Publisher ID','Transaction Date','Transaction Time','Process Date','Customer Status', 'Currency']
        headers = transactions[0]
        for e in expected:
            if e not in headers:
                logging.error(f'Linkshare report does not contain correct header - did not find {e}')
                logging.warn(headers)
                return [], [], []
        for t in transactions[1:]:
            country = ''
            for k,v in self.countries.items():
                if t[headers.index('\ufeffConsumer Country')] in v:
                    country = k
            transaction = {
                    'id': t[headers.index('Order ID')],
                    'saleAmount': {'amount':float(t[headers.index('Gross Sales')].replace(',','')), 'currency':t[headers.index('Currency')]},
                    'commissionAmount': {'amount':t[headers.index('Gross Commissions')], 'currency':t[headers.index('Currency')]},
                    'publisherId': t[headers.index('Publisher ID')],
                    'voucherCode': '',
                    'customerCountry': country,
                    'advertiserCountry': country,
                    'device' : 'Desktop'
                }
            if t[headers.index('Customer Status')] == 'New':
                transaction['status'] = 'New'
            else:
                transaction['status'] = 'Returning'
            date_components = t[headers.index('Transaction Date')].split('/')
            for v in date_components:
                if len(v) < 2:
                    date_components[date_components.index(v)] = f'0{v}'
            timing = t[headers.index('Transaction Time')]
            transaction['transactionDate'] = f'20{date_components[2]}-{date_components[0]}-{date_components[1]}T{timing}'
            
            if (t[headers.index('Transaction Date')] == t[headers.index('Process Date')]) and (transaction['saleAmount']['amount'] > 0.0):
                pending.append(transaction)
            else:
                if float(transaction['commissionAmount']['amount']) == 0.0:
                    declined.append(transaction)
                else:
                    approved.append(transaction)


        return approved, pending, declined