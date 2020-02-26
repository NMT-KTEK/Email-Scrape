---
author: Rachel Powers <powers.e.rachel@gmail.com>
date: 2020/2/25
---

# KTEK NMT Student Radio Email Scraper

This is the KTEK email scraper. 

a set of `python 3.8` scripts to scrape a Gmail inbox via `IMAP` for emails with links 
And then try to filter down thous links to the ones that may be useful for acquiring promo tracks
for the station.

by tracking the `msgid` attacked to the email the link came from the list of email that a human needs to go over
can be significantly reduced.

This project uses `pipenv` to control dependencies.

use
```
$ pipenv install
```
to set up the python virtual environment and install the dependiinces listed in the `Pipfile`

## Step 1. *scrape_client.py*

`scrape_client.py` is the file that will do the actual scraping of the INBOX via IMAP

The email account and folder are configured at the top of the file
in `EMAIL_ACCOUNT` and `EMAIL_FOLDER` respectively.

The Gmail account must have IMAP configured and access granted for insecure apps. This access only needs to be configured for the duration of the tool's use.

Run
```shell
$ pipenv run ./scrape_client.py
```

to start the client. You will be prompted for the password to the email and the scrape will begin.

The results will be stored in `emails_with_links.csv` and `email_links.csv` in the current directory
as currently configured. different names can be configured in `EMAIL_CSV_FILE` and `LINKS_CSV_FILE` at the top of the file.

The client will pull down UNREAD emails in batches of `100` (configurable in `EMAILS_PER_BATCH` at the top of the file) and scan them for links. it will work with both HTML and PlainText emails, defaulting to the HTML version first.

The summery of the emails with links in them will be written to `EMAIL_CSV_FILE`:

The summery of every link found, as well as the `msgid` it was found in, will be written to `LINKS_CSV_FILE`.

Depending on inbox size and the number of emails to fetch this processes cna take several hours. it is preferable to run it in a terminal multiplexer so that it cna run undisturbed.

These files can end up hundreds of thousands of lines long, easily over 100MB in size, so be careful when opening them with an editor.

## Step 2. *filter_links.py*

This is where the magic happens.

Run
```shell
$ pipenv run ./filter_links.py
```
to perform the filtering operation.

This file is also configured at the top. `LINKS_CSV_FILE` must be the same as was used in the previous step.

Filter patterens are loaded form the `YAML` file `FILTER_PATTERNS_FILE` (`filter_patterns.yml`)

These patterns are matched against the `netloc`, `link text`, and `URL` of the link in order to filter out links that are obviously useless in order to lighten the load of the next step. These patterns can be simple substring matches or regular expressions. a single match will exclude a link. The default settings in this file were the patterns observed during the creation of this tool. `HOSTNAME_FILE`,  `NETLOC_FILE`, `LINK_DUP_FILE`, and `LINK_DEDUP_FILE` are summaries written to aid in the creation of new patterns if the current are not sufficient.

The tool will also filter duplicate links with the same `href` down to the first encountered so that the link only has to be checked once and potentially duplicate emails can be discarded.

This preliminary filtering my be sufficient in some cases but if too many emails remain the next step can be taken.

Then the hard job of hitting every remaining link with a `HTTP HEAD` request is done with `cURL` via `pycurl`. `LINKS_PER_BATCH` will controll the number of links hit at once, `CURL_TIMEOUT` controls how long `libcurl` will wait for a timeout.

Depending on the number of links this process can take over 24 hours.

The tool will use rotating `USER AGENT` headers and, if selected, will fetch a list of proxies from `http://free-proxy-list.net`that help prevent getting throttled for request flooding.

Requests made through a proxy that fail to connect will be retried with out the proxy and a count is kept track of the number of times a proxy fails. if this count reaches 3 the proxy is removed form the list. If for any reason the proxy list is eventually emptied it will be re fetched (hopefully with fresh proxies, they tend to die and get replaced in periods of just hours to days). The count of failures is not reset however of if a proxy that was previously removed fails again it is removed again.

After the `cURL` process a new summery with `HTTP STATUS` the `effective-url` (the url used after any redirects) of the link, and the number of redirects encountered is written to `DEDUP_CURL_LINK_FILE`.

If this part of the procces has been run be fore and `DEDUP_CURL_LINK_FILE` already exists the user is given the option ot load the data form there instead of re-running the `cURL` section. this is intended so that the filters cna be again refined for the last part without having to constantly re-scrape the `URL`s.

Then the process of filtering is done again but this time removing failed status codes and using the filters on the `effective-url`. another de-duplication is done of the `effective-url` so that links that end up at the same place are considered the same.

The remaining list of links after all this is written to `LINK_DEDUP_CURL_FILE`.

## Step 3. filter_email.py

The final step is to cross reference the final list of potentially useful links and their `msgid`s with the original list of emails. and send a request via IMAP to [DELETE/ OR / TAG / AND OR/ MARK AS READ] any emil with no useful links[, and then TAG the emails with useful links for a human to go through]?



