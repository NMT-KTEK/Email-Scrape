#!/usr/bin/env python
#
# a client to scrape the KTEK email inbox and compile a list of promo tracks to fetch

import sys
import re
import getpass
import email
import email.message
import email.parser
import email.header
import email.policy
import datetime
from pprint import pprint
import csv

from urllib.parse import unquote

import imapclient.exceptions
from imapclient import IMAPClient

import bs4
from bs4 import BeautifulSoup

import time
import progressbar

EMAIL_ACCOUNT = "ktekradio@gmail.com"
EMAIL_FOLDER = "INBOX"

EMAIL_CSV_FILE = "emails_with_links.csv"
LINKS_CSV_FILE = "email_links.csv"

EMAILS_PER_BATCH = 100

link_regx = re.compile(r'<a[^>]+href=[\'"]"(.*?)[\'" ]?[^>]*>(.*)?</a>')
link_regx_pt = re.compile(r'(http[s]?://.*?)\s')


Emails_With_Links = {}
Links = []

def process_message(uid, msg :email.message.EmailMessage):

    try:
        raw_date = msg.get('Date')
        local_date = None

        # Now convert to local date-time
        date_tuple = email.utils.parsedate_tz(raw_date)
        if date_tuple:
            local_date = datetime.datetime.fromtimestamp(
                email.utils.mktime_tz(date_tuple))
    except:
        raw_date = None
        local_date = None


    links = []
    try:
        body = msg.get_body(('html', 'plain'))
        if body:
            if body.get_content_type() == 'text/plain':
                links = links_from_plaintext(body.get_content())
            elif body.get_content_type() == 'text/html':
                links = links_from_html(body.get_content())
    except:
        pass

    try:
        msg_to = msg.get('To')
    except:
        msg_to = None

    try:
        msg_from = msg.get('From')
    except:
        msg_from = None

    try:
        msg_sub = msg.get('Subject')
    except:
        msg_sub = None

    try:
        msg_id = msg.get('Message-ID')
    except:
        msg_id = None

    info = {
        'uid': uid,
        'to': msg_to,
        'from': msg_from,
        'subject': msg_sub,
        'raw_date': raw_date,
        'local_date': local_date,
        'message-id': msg_id,
        'links': links,
        'num-links': len(links)
    }

    return info

def links_from_html(body):
    links = []
    if body:
        try:
            soup = BeautifulSoup(body, 'lxml')
            #pprint(soup)
            for tag in soup.findAll('a', href=True):
                tag : bs4.Tag
                if tag.has_attr('href'):
                    links.append((tag['href'], str(tag.string)))
        except bs4.ParserRejectedMarkup as err:
            print("Could not parse HTML: ", err)
    return links
        

def links_from_plaintext(body):
    links = []
    if body:
        for m in link_regx_pt.finditer(body):
            # print("Link found (altpat)! -> " + m.group(0))
            links.append((m.group(0), None))
    return links


# def uid2msgid(server, uid):
#         """ Convert an IMAP UID to a Gmail MSGID """

#         typ, data = server.uid( r'fetch', uid, r'(X-GM-MSGID)')

#         msgid_dec = re.search( r'X-GM-MSGID ([0-9]+)', data[0] ).group(1)
#         msgid_hex = hex( int( msgid_dec ) )

#         return msgid_hex[2:]


def process_mailbox(server):
    """
    scan emails for links.
    """

    messages = server.search('UNSEEN')
    print('There are {} UNSEEN messages\n'.format(len(messages)))

    n = EMAILS_PER_BATCH
    message_groups = [messages[i * n:(i + 1) * n] for i in range((len(messages) + n - 1) // n )]  
    num_groups = len(message_groups)
    print("Spliting into {} groups of {}".format(num_groups, n))

    widgets=[
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(), progressbar.Percentage(),
        ' (', progressbar.ETA(), ') ',
        '[', progressbar.SimpleProgress() ,']'
    ]

    with progressbar.bar.ProgressBar(widgets=widgets, redirect_stdout=True) as  pbar:
        pbar.max_value = num_groups
        for g_num, group in pbar(zip(range(num_groups), message_groups)):
            print('\nProcessing group {} ...\n'.format(g_num + 1))
            for uid, message_data in server.fetch(group, ['RFC822', 'X-GM-MSGID']).items():
                msg = email.message_from_bytes(message_data[b'RFC822'], policy=email.policy.default)
                msg_info = process_message(uid, msg)
                msgid_dec = message_data[b'X-GM-MSGID']
                msgid = hex( int( msgid_dec ) )[2:]
                record_msg(msg_info, msgid)
                print_msg_info(msg_info, msgid)


def record_msg(msg_info : dict, msgid : str):
    global Emails_With_Links
    global Links
    if msg_info['links']:
        for link in msg_info['links']:
            Links.append({'msgid': str(msgid), 
                          'href': link[0], 
                          'str': link[1]})
        msg_info['msgid'] = msgid
        Emails_With_Links[str(msgid)] = {k:msg_info[k] for k in msg_info if k != 'links'} 
        

        
def print_msg_info(msg_info : dict, msgid : str):

 
    print('UID: {}\n'.format(msg_info['uid']),
        'MSGID: {}\n'.format(msgid),
        'Message-ID: {}\n'.format(msg_info['message-id']),
        'From: {}\n'.format(msg_info['from']), 
        'To: {}\n'.format(msg_info['to']), 
        'Subject: {}\n'.format(msg_info['subject']),
        'Raw Date: {}\n'.format(msg_info['raw_date']),
        'Local Date: {}\n'.format(msg_info['local_date']))


if __name__ == "__main__":

    with IMAPClient('imap.gmail.com', use_uid=True, ssl=True) as server:
        print('Using Account:', EMAIL_ACCOUNT)
        try:
            res = server.login(EMAIL_ACCOUNT, getpass.getpass())
            pprint(res)

            print("Processing mailbox...\n")
            res = server.select_folder(EMAIL_FOLDER, readonly=True)
            num_msgs = res[b'EXISTS']
            print('There are {} messages in {}'.format(num_msgs, EMAIL_FOLDER))

            process_mailbox(server)

            print("\nWriting to files....")
            widgets=[
                ' [', progressbar.Timer(), '] ',
                progressbar.Bar(), progressbar.Percentage(),
                ' (', progressbar.ETA(), ') ',
            ]

            with progressbar.bar.ProgressBar(widgets=widgets, redirect_stdout=True) as  pbar:

                pbar.max_value = len(Emails_With_Links)
                print("Writing email summery: {} Emails".format(len(Emails_With_Links)))
                with open(EMAIL_CSV_FILE, 'wb', newline='',  encoding='utf-8') as email_csv:
                    
                    email_fieldnames = ['uid', 'msgid', 'message-id', 
                                        'from', 'to', 'subject', 
                                        'raw_date', 'local_date', 'num-links']
                    email_writer = csv.DictWriter(email_csv, fieldnames=email_fieldnames)

                    email_writer.writeheader()

                    for row in pbar(Emails_With_Links.values()):
                        email_writer.writerow(row)
                        
            print("\nEmail summery written to {}".format(EMAIL_CSV_FILE))


            with progressbar.bar.ProgressBar(widgets=widgets, redirect_stdout=True) as  pbar:  
                pbar.max_value = len(Links)  
                print("Writing link info: {} Links...".format(len(Links)))
                with open(LINKS_CSV_FILE, 'wb', newline='',  encoding='utf-8') as links_csv:

                    links_fieldnames = ['msgid', 'href', 'str']
                    links_writer = csv.DictWriter(links_csv, fieldnames=links_fieldnames)

                    links_writer.writeheader()

                    for row in pbar(Links):
                        links_writer.writerow(row)

            print("\nLink summery writtne to {}".format(LINKS_CSV_FILE))
    

        except imapclient.exceptions.LoginError as err:
            print("LOGIN FAILED!!! ", str(err))
            sys.exit(1)
        
        

    # with open_connection() as M:

    #     typ, mailboxes = M.list()
    #     if typ == 'OK':
    #         print("Mailboxes:")
    #         pprint(mailboxes)

    #     typ, data = M.select(EMAIL_FOLDER, readonly=True)
    #     if typ == 'OK':
    #         print("Processing mailbox...\n")
    #         num_msgs = int(data[0])
    #         print('There are {} messages in {}'.format(num_msgs, EMAIL_FOLDER))
    #         process_mailbox(M)
    #     else:
    #         print("ERROR: Unable to open mailbox ", typ)
