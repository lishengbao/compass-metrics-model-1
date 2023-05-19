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
from compass_common.utils.utils import get_all_repo
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
        return None
    return response


class ContributorObserve:
    def __init__(self, json_file, origin, observe_types, observe_index, api_token, bigquery_credentials_file, from_date, end_date):
        """Fetch star, fork, watch contributor data

        Args:
            json_file: The path of json file containing repository message.
            origin: Repository data origin, choose from github, Gitee.
            observe_types: The fetch data type, you can choose star, fork, watch
            observe_index: Save the fetch data.
            api_token: Repository data origin api token
            bigquery_credentials_file: Google application credentials file path,
                                     due to the limitation of 40000 records in the GitHub Star REST API,
                                     data beyond the limit is obtained through BigQuery.
                                     See https://cloud.google.com/iam/docs/keys-create-delete?hl=zh-cn#iam-service-account-keys-create-console
                                     to generate credentials
            from_date: The start time of fetch data
            end_date: The end time of fetch data
        """
        self.all_repo = get_all_repo(json_file, origin)
        self.origin = origin
        self.observe_types = observe_types
        self.observe_index = observe_index
        self.api_token = api_token
        self.from_date = from_date
        self.end_date = end_date
        self.client = None

        if bigquery_credentials_file:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = bigquery_credentials_file
            self.bigquery_client = bigquery.Client()

    def run(self, elastic_url):
        self.client = get_elasticsearch_client(elastic_url)
        for repo_url in self.all_repo:
            if self.origin == "github":
                if "star" in self.observe_types:
                    self.fetch_github_star_contributor(repo_url)
                if "fork" in self.observe_types:
                    self.fetch_github_fork_contributor(repo_url)
            if self.origin == "gitee":
                if "star" in self.observe_types:
                    self.fetch_gitee_star_contributor(repo_url)
                if "fork" in self.observe_types:
                    self.fetch_gitee_fork_contributor(repo_url)
                if "watch" in self.observe_types:
                    self.fetch_gitee_watch_contributor(repo_url)

    def fetch_github_star_contributor(self, repo_url):
        """ Fetch github star contributor data """
        type = "star"
        owner = repo_url.split('/')[-2]
        repo = repo_url.split('/')[-1]
        fetch_star_count = 0 
        bigquery_from_date = "2011-02-12"

        def get_repo_star_count():
            """ Get the star count from the repo information """
            headers = {
                'user-Agent': random.choice(USER_AGENT_LIST),
                'Accept': 'application/vnd.github.star+json',
                'Authorization': 'Bearer ' + self.api_token
            }
            url = f'https://api.github.com/repos/{owner}/{repo}'
            res = requests_with_headers(url, headers)
            if res:
                repo_text = json.loads(res.text)
                return repo_text.get("watchers", None)
            return None

        def fetch_by_api():
            """ Use the github rest api to fetch star contributors """
            nonlocal fetch_star_count
            nonlocal bigquery_from_date
            contributor_message = []
            headers = {
                'user-Agent': random.choice(USER_AGENT_LIST),
                'Accept': 'application/vnd.github.star+json',
                'Authorization': 'Bearer ' + self.api_token
            }
            url = f'https://api.github.com/repos/{owner}/{repo}/stargazers?per_page=100&page=1'
            while True:
                res = requests_with_headers(url, headers)
                time.sleep(1)
                if res is None:
                    continue
                stars_text = json.loads(res.text)
                fetch_star_count += len(stars_text)
                for message in stars_text:
                    if not (self.from_date <= message["starred_at"] < self.end_date):
                        continue
                    user_message = {
                        "_index": self.observe_index,
                        "_id": get_uuid(repo_url, str(message["user"]["id"]), type),
                        "_source": {
                            "uuid": get_uuid(repo_url, str(message["user"]["id"]), type),
                            "user_login": message["user"]["login"],
                            "tag": repo_url,
                            "owner": owner,
                            "repo": repo,
                            "type": type,
                            "grimoire_creation_date": message["starred_at"],
                            'metadata__enriched_on': datetime_utcnow().isoformat()
                        }
                    }
                    contributor_message.append(user_message)
                    bigquery_from_date = message["starred_at"]
                    if len(contributor_message) > MAX_BULK_UPDATE_SIZE:
                        helpers.bulk(client=self.client, actions=contributor_message)
                        contributor_message = []
                if 'next' not in res.links.keys():
                    break
                url = res.links['next']['url']
            helpers.bulk(client=self.client, actions=contributor_message)

        def fetch_by_bigquery(from_date, end_date):
            """ Use the Google BigQuery to fetch star contributors """
            nonlocal fetch_star_count
            contributor_message = []
            table_list = [x.strftime('%Y%m') for x in list(pd.date_range(
                freq="MS",
                start=datetime_to_utc(str_to_datetime(from_date) - relativedelta(months=1)),
                end=datetime_to_utc(str_to_datetime(end_date))))]
            for table in table_list:
                # if fetch_star_count >= repo_star_count:
                #     break
                query = f"""
                    select 
                        g.actor.id as actor_id,
                        g.actor.login as actor_login, 
                        g.created_at as created_at
                    from 
                        `githubarchive.month.{table}` as g 
                    where 
                        g.type = 'WatchEvent' 
                        and g.repo.name = '{owner}/{repo}' 
                """
                logger.info(query)
                query_data = self.bigquery_client.query(query)
                for row in query_data:
                    user_message = {
                        "_index": self.observe_index,
                        "_id": get_uuid(repo_url, str(row.actor_id), type),
                        "_source": {
                            "uuid": get_uuid(repo_url, str(row.actor_id), type),
                            "user_login": row.actor_login,
                            "tag": repo_url,
                            "owner": owner,
                            "repo": repo,
                            "type": type,
                            "grimoire_creation_date": row.created_at,
                            'metadata__enriched_on': datetime_utcnow().isoformat()
                        }
                    }
                    contributor_message.append(user_message)
                    if len(contributor_message) > MAX_BULK_UPDATE_SIZE:
                        helpers.bulk(client=self.client, actions=contributor_message)
                        contributor_message = []
                    fetch_star_count += 1
            helpers.bulk(client=self.client, actions=contributor_message)

        repo_star_count = get_repo_star_count()
        fetch_by_api()
        fetch_by_bigquery(max(bigquery_from_date, self.from_date),
                          min(datetime_utcnow().strftime('%Y-%m-%dT%H:%M:%S'), self.end_date))

    def fetch_github_fork_contributor(self, repo_url):
        """ Fetch github fork contributor data """
        type = "fork"
        owner = repo_url.split('/')[-2]
        repo = repo_url.split('/')[-1]
        contributor_message = []
        headers = {
            'user-Agent': random.choice(USER_AGENT_LIST),
            'Accept': 'application/vnd.github.star+json',
            'Authorization': 'Bearer ' + self.api_token
        }
        page = 1
        while True:
            url = f'https://api.github.com/repos/{owner}/{repo}/forks?per_page=100&page={page}'
            res = requests_with_headers(url, headers)
            time.sleep(1)
            if res is None:
                continue
            page += 1
            forks_text = json.loads(res.text)
            if (len(forks_text)) == 0:
                break
            for message in forks_text:
                if not (self.from_date <= message["created_at"] < self.end_date):
                    continue
                user_message = {
                    "_index": self.observe_index,
                    "_id": get_uuid(repo_url, str(message["owner"]["id"]), type),
                    "_source": {
                        "uuid": get_uuid(repo_url, str(message["owner"]["id"]), type),
                        "user_login": message["owner"]["login"],
                        "tag": repo_url,
                        "owner": owner,
                        "repo": repo,
                        "type": type,
                        "grimoire_creation_date": message["created_at"],
                        'metadata__enriched_on': datetime_utcnow().isoformat()
                    }
                }
                contributor_message.append(user_message)
                if len(contributor_message) > MAX_BULK_UPDATE_SIZE:
                    helpers.bulk(client=self.client, actions=contributor_message)
                    contributor_message = []
        helpers.bulk(client=self.client, actions=contributor_message)

    def fetch_gitee_star_contributor(self, repo_url):
        """ Fetch gitee star contributor data """
        type = "star"
        owner = repo_url.split('/')[-2]
        repo = repo_url.split('/')[-1]
        headers = {'user-Agent': USER_AGENT_LIST[-1]}
        contributor_message = []
        page = 1
        while True:
            url = f'https://gitee.com/api/v5/repos/{owner}/{repo}/stargazers?access_token={self.api_token}' \
                  f'&per_page=100&page={page}'
            res = requests_with_headers(url, headers)
            if res is None:
                continue
            page += 1
            stars_text = json.loads(res.text)
            if (len(stars_text)) == 0:
                break
            for message in stars_text:
                if message is None or not (self.from_date <= message["star_at"] < self.end_date):
                    continue
                user_message = {
                    "_index": self.observe_index,
                    "_id": get_uuid(repo_url, str(message["id"]), type),
                    "_source": {
                        "uuid": get_uuid(repo_url, str(message["id"]), type),
                        "user_login": message["login"],
                        "author_name": message["name"],
                        "tag": repo_url,
                        "owner": owner,
                        "repo": repo,
                        "type": type,
                        "grimoire_creation_date": message["star_at"],
                        'metadata__enriched_on': datetime_utcnow().isoformat()
                    }
                }
                contributor_message.append(user_message)
                if len(contributor_message) > MAX_BULK_UPDATE_SIZE:
                    helpers.bulk(client=self.client, actions=contributor_message)
                    contributor_message = []
        helpers.bulk(client=self.client, actions=contributor_message)

    def fetch_gitee_fork_contributor(self, repo_url):
        """ Fetch gitee fork contributor data """
        type = "fork"
        owner = repo_url.split('/')[-2]
        repo = repo_url.split('/')[-1]
        headers = {'user-Agent': USER_AGENT_LIST[-1]}
        contributor_message = []
        page = 1
        while True:
            url = f'https://gitee.com/api/v5/repos/{owner}/{repo}/forks?access_token={self.api_token}' \
                  f'&per_page=100&page={page}'
            res = requests_with_headers(url, headers)
            if res is None:
                continue
            page += 1
            forks_text = json.loads(res.text)
            if (len(forks_text)) == 0:
                break
            for message in forks_text:
                if message is None or not (self.from_date <= message["created_at"] < self.end_date):
                    continue
                user_message = {
                    "_index": self.observe_index,
                    "_id": get_uuid(repo_url, str(message["id"]), type),
                    "_source": {
                        "uuid": get_uuid(repo_url, str(message["id"]), type),
                        "user_login": message["owner"]["login"],
                        "author_name": message["owner"]["name"],
                        "tag": repo_url,
                        "owner": owner,
                        "repo": repo,
                        "type": type,
                        "grimoire_creation_date": message["created_at"],
                        'metadata__enriched_on': datetime_utcnow().isoformat()
                    }
                }
                contributor_message.append(user_message)
                if len(contributor_message) > MAX_BULK_UPDATE_SIZE:
                    helpers.bulk(client=self.client, actions=contributor_message)
                    contributor_message = []
        helpers.bulk(client=self.client, actions=contributor_message)

    def fetch_gitee_watch_contributor(self, repo_url):
        """ Fetch gitee watch contributor data """
        type = "watch"
        owner = repo_url.split('/')[-2]
        repo = repo_url.split('/')[-1]
        headers = {'user-Agent': USER_AGENT_LIST[-1]}
        contributor_message = []
        page = 1
        while True:
            url = f'https://gitee.com/api/v5/repos/{owner}/{repo}/subscribers?access_token={self.api_token}' \
                  f'&per_page=100&page={page}'
            res = requests_with_headers(url, headers)
            if res is None:
                continue
            page += 1
            watchs_text = json.loads(res.text)
            if (len(watchs_text)) == 0:
                break
            for message in watchs_text:
                if message is None or not (self.from_date <= message["watch_at"] < self.end_date):
                    continue
                user_message = {
                    "_index": self.observe_index,
                    "_id": get_uuid(repo_url, str(message["id"]), type),
                    "_source": {
                        "uuid": get_uuid(repo_url, str(message["id"]), type),
                        "user_login": message["login"],
                        "author_name": message["name"],
                        "tag": repo_url,
                        "owner": owner,
                        "repo": repo,
                        "type": type,
                        "grimoire_creation_date": message["watch_at"],
                        'metadata__enriched_on': datetime_utcnow().isoformat()
                    }
                }
                contributor_message.append(user_message)
                if len(contributor_message) > MAX_BULK_UPDATE_SIZE:
                    helpers.bulk(client=self.client, actions=contributor_message)
                    contributor_message = []
        helpers.bulk(client=self.client, actions=contributor_message)
