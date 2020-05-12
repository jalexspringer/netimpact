# Netimpact - a network data importer for Impact Partnership Cloud

## Docker Instructions
Build the image with the config.toml file in the folder:

    docker build -t jalexspringer/netimpact .

Run it with the CLI commands (see CLI help below):

Get new partners:

    docker run --rm -ti jalexspringer/netimpact -p admitad,awin,ls

ETL yesterday's transactions:

    docker run --rm -ti jalexspringer/netimpact -t awin,admitad,ls

Note that if you have built the image with the config.toml file in the root you won't need to pass anything with -c. If for some reason you don't want to do that (distributing the image?) you'll need to run the container with a mounted volume and reference the volume. Plan is to do this as well as mount transactions folder to keep the transaction CSVs.

## CLI Help
    Usage: netimpact [OPTIONS] NETWORKS

    Default CLI method to get new partners and transactions from the provided
    NETWORKS and create them in the Imapct program.

    NETWORKS is the comma seperated list of networks to operate on (currently
    accepts awin,linkshare,admitad)

    Options:
    --test              If flag is included then transactions will be pulled
                        from the listed networks and put in the transactions
                        folder locally instead of uploaded to Impact.

    -g, --groups        refresh group list?
    -t, --transactions  get and upload transactions?
    -p, --partners      get and upload new partners?
    -c, --config TEXT   relative filepath to the TOML config file (see README
                        for formatting requirements). Defaults to looking for a
                        config.toml file in the current directory.

    --help              Show this message and exit.

## Network Objects

Currently implemented - Awin, Admitad, and Linkshare(Rakuten)

## Impact account requirements

* IO references Numeric1: payout groups set to look for Numeric1 and use that as the percentage of sale, up to 20%
* add clickid to query string
* ftp credentials/submission for action tracker
* open ended locking
* non-payable IO
* currently can have only one contract on account
* one mobile and one desktop action tracker, both in the contract with the same payout groups
* groups created for each network program name.
* contracts dont get IDs until one partner is added to them. Manually add a partner.

Imports active partners, clicks, transactions and validations from Awin, Admitad, and Linkshare on anywhere from a 30 to 1 minute range, creates partner accounts specifically for this, and then batches transactions as well as reversing/locking anything validated across the same time frame.

Cost comes in by calculating the actual percentage and passing it to Numeric1. Payout groups are used for each percentage point up to 20 - there are no fractions of a percentage point payout in any of the network terms.

Partners are assigned a group for each region/network they belong to (there are many partners joined to multiple), and each transaction is tagged with account region, network, customer country, promo code, customer status, etc. Basically everything the networks could spit out short of product level data.

*TODO :: Working on a way to do clicks without it looking like a DDOS attack, but I think can make the PLA do what I want.*

Handles net vs. gross rev., and treating all of it as direct Impact tracked conversions with no attribution issues.

Also gives us a mapping from partner IDs - I put those in MPValue1.

## Linkshare report requirements

Note the Linkshare reports in each account require a specific set of headers (any order is fine), as the API only allows for pulling existing reports:

    'Consumer Country','Gross Commissions','Gross Sales','Order ID','Publisher ID','Transaction Date','Transaction Time','Process Date','Customer Status', 'Currency'


## Package config file example (TOML)

    [Impact]
    impact_sid = 'XXXXXXXXXX'
    impact_token = 'XXXXXXXXXX'
    ftp_un = 'FTP USERNAME'
    ftp_p = 'FTP PASS'
    program_id = 'XXXXX'
    desktop_action_tracker_id = 'XXXXX'
    mobile_action_tracker_id = 'XXXXX'
      
    [Awin]
    oauth = "XXXXXXXXXXXXX"
    [Awin.account_ids]
    'Awin Example 1' = 'XXXX'
    'Awin Example 2' = 'XXXX'

    [Linkshare]
    [Linkshare.report_names]
    'Transactions' = 'transaction-report'
    'Publishers' = 'publisher-report'
    [Linkshare.account_ids]
    'Linkshare AU' = 'XXXXX'
    'Linkshare Asia' = 'XXXXX'
    'Linkshare Concierge' = 'XXXXX'
    'Linkshare UK' = 'XXXXX'
    'Linkshare US' = 'XXXXX'

    [Admitad]
    client_id='XXXXX'
    client_secret='XXXXX'
    account_id = 'XXXX'
    account_name = 'Example_Account'
    [Admitad.account_ids]
    'Example_Account' = 'XXXX'

## Tests
### TODO :: Implement tests