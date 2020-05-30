#!/usr/bin/env python3

import netimpact.processor as P
from netimpact.awin import AWin
from netimpact.admitad import Admitad
from netimpact.linkshare import Linkshare
from netimpact.impact import Impact #, Clicker
import click
import toml
import boto3
from s3fs import S3FileSystem
import pandas as pd

import logging,os,time
from datetime import datetime, timedelta, date
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

@click.argument('networks')
@click.option('--config', '-c', default='config.toml', help='relative filepath to the TOML config file (see README for formatting requirements). Defaults to looking for a config.toml file in the current directory.')
@click.option('--partners', '-p', is_flag=True, default=False, help='get and upload new partners?')
@click.option('--transactions', '-t', is_flag=True, default=False, help='get and upload transactions?')
@click.option('--groups', '-g', is_flag=True, default=False, help='refresh group list?')
@click.option('--no_upload', is_flag=True, default=False, help='If flag is included then transactions will be pulled from the listed networks and put in the transactions folder locally instead of uploaded to Impact.')
@click.option('--s3_upload', '-s', default=False, help='Name of the S3 bucket to upload data to. References ~/.aws/credentials')
@click.option('--target_date', '-d', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(date.today()-timedelta(1)), help='Transactions from what day? Format: %Y-%m-%d')
@click.command()
def cli(networks,config,partners,transactions,groups,no_upload,s3_upload,target_date):
    """Default CLI method to get new partners and transactions from the
    provided NETWORKS and create them in the Impact program.

    NETWORKS is the comma seperated list of networks to operate on (currently accepts awin,linkshare,admitad)
    """
    c = toml.load(config)
    logging.info(f'Starting import process for {networks}')
    i = Impact(
        c['Impact']['impact_sid'],
        c['Impact']['impact_token'],
        c['Impact']['ftp_un'],
        c['Impact']['ftp_p'],
        c['Impact']['program_id'],
        c['Impact']['desktop_action_tracker_id'],
        c['Impact']['mobile_action_tracker_id'],
        reload_groups=groups
        )

    network_list = []
    network_names = []
    if 'awin' in networks.lower():
        network_list.append(AWin(c['Awin']['oauth']))
        network_names.append('awin')

    if 'admitad' in networks.lower():
        network_list.append(Admitad(c['Admitad']['client_id'], c['Admitad']['client_secret']))
        network_names.append('admitad')
    
    if 'ls' in networks.lower() or 'linkshare' in networks.lower() or 'rakuten' in networks.lower():
        network_list.append(Linkshare(c['Linkshare']['report_names']['Transactions'], c['Linkshare']['report_names']['Publishers']))
        network_names.append('linkshare')

    for n in network_list:
        logging.info(f'Running data import for {n.network_name} into Impact program {i.program_id}')
        for account_name,account_id in c[n.network_name]['account_ids'].items():
            if account_name not in i.groups_dict.keys():
                i.groups_dict[account_name] = i.create_group(account_name)
            if partners:
                i.new_publisher_validation(account_id, account_name, n)
            if transactions:
                file_path_m, file_path_p = P.transactions_process(i, account_id, account_name, n, target_date)
                if no_upload:
                    pass
                else:
                    i.batch_to_impact(file_path_m, file_path_p)
                if s3_upload:
                    session = boto3.Session(
                        aws_access_key_id=c['S3']['access_key'],
                        aws_secret_access_key==c['S3']['secret_access_key'],
                    )
                    s3 = boto3.resource('s3')
                    df = pd.read_csv(file_path_p)
                    pq_file = f'{str(file_path_p).rstrip(".csv")}.parquet'
                    df.to_parquet(pq_file)
                    s3.Object(c['S3']['bucket'], pq_file).put(Body=open(pq_file, 'rb'))
                    s3.Object(c['S3']['bucket'], str(file_path_m)).put(Body=open(str(file_path_m), 'rb'))