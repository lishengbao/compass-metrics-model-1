import os
import time
import json
import random
import requests
import logging
import pandas as pd
from opensearchpy import helpers
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime,
                                          datetime_to_utc)
from compass_common.utils.utils import get_all_org
from compass_common.utils.opensearch_client_utils import get_elasticsearch_client
from compass_common.utils.uuid_utils import get_uuid


logger = logging.getLogger(__name__)
MAX_BULK_UPDATE_SIZE = 100

USER_AGENT_LIST = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; "
    ".NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR "
    "2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR "
    "3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"]


def requests_with_headers(url, headers):
    """ Send request to fetch data """
    logger.info(url)
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.info(f"""
            Response from: {url},
            error code: {response.status_code}, 
            error reason: {response.reason},
            error text: {response.text},
            headers: {str(headers)}
        """)
    return response


class ContributorMember:
    def __init__(self, json_file, origin, member_index, api_token):
        """Fetch all users who are members of an organization

        Args:
            json_file: The path of json file containing repository message.
            origin: Repository data origin, choose from github, Gitee.
            member_index: Save the fetch data.
            api_token: Repository data origin api token
        """
        self.all_org = get_all_org(json_file, origin)
        self.origin = origin
        self.member_index = member_index
        self.api_token = api_token
        self.client = None

    def run(self, elastic_url):
        self.client = get_elasticsearch_client(elastic_url)
        exist = self.client.indices.exists(index=self.member_index)
        if not exist:
            self.client.indices.create(index=self.member_index, body=self.get_member_index_mapping())
        for org_url in self.all_org:
            self.delete_members(org_url)
            if self.origin == "github":
                self.fetch_github_members(org_url)
            if self.origin == "gitee":
                self.fetch_gitee_members(org_url)

    def delete_members(self, org_url):
        query = {
            "query": {
                "match_phrase": {
                    "tag.keyword": org_url
                }
            }
        }
        self.client.delete_by_query(index=self.member_index, body=query)

    def fetch_github_members(self, org_url):
        """ Fetch github members data """
        owner = org_url.split('/')[-1]
        contributor_message = []
        headers = {
            'user-Agent': random.choice(USER_AGENT_LIST),
            'Accept': 'application/vnd.github.star+json',
            'Authorization': 'Bearer ' + self.api_token
        }
        page = 1
        while True:
            url = f'https://api.github.com/orgs/{owner}/members?per_page=100&page={page}'
            res = requests_with_headers(url, headers)
            time.sleep(1)
            if res.status_code != 200:
                break
            page += 1
            members_text = json.loads(res.text)
            if (len(members_text)) == 0:
                break
            for message in members_text:
                user_message = {
                    "_index": self.member_index,
                    "_id": get_uuid(org_url, str(message["id"])),
                    "_source": {
                        "uuid": get_uuid(org_url, str(message["id"])),
                        "user_login": message["login"],
                        "tag": org_url,
                        "owner": owner,
                        'metadata__enriched_on': datetime_utcnow().isoformat()
                    }
                }
                contributor_message.append(user_message)
                if len(contributor_message) > MAX_BULK_UPDATE_SIZE:
                    helpers.bulk(client=self.client, actions=contributor_message)
                    contributor_message = []
        helpers.bulk(client=self.client, actions=contributor_message)

    def fetch_gitee_members(self, org_url):
        """ Fetch gitee members data """
        owner = org_url.split('/')[-1]
        headers = {'user-Agent': USER_AGENT_LIST[-1]}
        contributor_message = []
        page = 1
        while True:
            url = f'https://gitee.com/api/v5/orgs/{owner}/members?access_token={self.api_token}' \
                  f'&per_page=100&page={page}'
            res = requests_with_headers(url, headers)
            if res.status_code != 200:
                break
            page += 1
            members_text = json.loads(res.text)
            if (len(members_text)) == 0:
                break
            for message in members_text:
                user_message = {
                    "_index": self.member_index,
                    "_id": get_uuid(org_url, str(message["id"])),
                    "_source": {
                        "uuid": get_uuid(org_url, str(message["id"])),
                        "user_login": message["login"],
                        "tag": org_url,
                        "owner": owner,
                        'metadata__enriched_on': datetime_utcnow().isoformat()
                    }
                }
                contributor_message.append(user_message)
                if len(contributor_message) > MAX_BULK_UPDATE_SIZE:
                    helpers.bulk(client=self.client, actions=contributor_message)
                    contributor_message = []
        helpers.bulk(client=self.client, actions=contributor_message)

    def get_member_index_mapping(self):
        mapping = {
            "mappings": {
                "properties": {
                    "metadata__enriched_on": {
                        "type": "date"
                    },
                    "owner": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "tag": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "user_login": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "uuid": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            }
        }
        return mapping