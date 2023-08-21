#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021-2022 Yehui Wang, Chenqi Shan
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Yehui Wang <yehui.wang.mdh@gmail.com>
#     Chenqi Shan <chenqishan337@gmail.com>

from perceval.backend import uuid
from datetime import datetime, timedelta
from urllib.parse import urlparse
import json
import yaml
import pandas as pd
import logging
from grimoire_elk.enriched.utils import get_time_diff_days
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
from grimoire_elk.elastic import ElasticSearch
from .metrics_model import MetricsModel, MAX_BULK_UPDATE_SIZE, check_times_has_overlap

from .utils import (get_uuid,
                    get_date_list,
                    get_activity_score,
                    community_support,
                    code_quality_guarantee,
                    organizations_activity,
                    community_decay,
                    activity_decay,
                    code_quality_decay)
from .utils_inside import (developer_attraction)

import os
import inspect
import sys
current_dir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
os.chdir(current_dir)
sys.path.append('../')

logger = logging.getLogger(__name__)

def get_oldest_date(date1, date2):
    return date2 if date1 >= date2 else date1


def get_latest_date(date1, date2):
    return date1 if date1 >= date2 else date2

def get_query_all(opensearch_client, index_name, from_date, to_date, org_name, inclue=None, exclusion=None):
    query = {
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "org_change_date_list.org_name.keyword": org_name
                        }
                    },

                ],
                "filter": [
                    {
                        "range": {
                            "code_commit_date_list": {
                                "gte": from_date,
                                "lte": to_date
                            }
                        }
                    }
                ]
            }
        }
    }
    if inclue is not None:
        query["query"]["bool"]["must"].append({
            "match_phrase": {
                "repo_name.keyword": inclue
            }
        })
    if exclusion is not None:
        query["query"]["bool"]["must_not"] = [
            {
                "match_phrase": {
                    "repo_name.keyword": exclusion
                }
            }
        ]
    hits = opensearch_client.search(index=index_name, body=query)["hits"]["hits"]
    data_list = [source["_source"] for source in hits]
    return data_list

def get_data(opensearch_client, index_name,from_date,to_date, org_name, inclue=None, exclusion=None):
    data_list = get_query_all(opensearch_client, index_name,from_date,to_date, org_name,inclue, exclusion)
    author_name_set = set()
    for item in data_list:
        author_name_set.update(item.get("id_git_author_name_list"))
    return author_name_set

def commit_frequency_count_same(contributor_same, from_date, to_date, contributor_list):
    commit_count = 0
    from_date = from_date.strftime("%Y-%m-%d")
    to_date = to_date.strftime("%Y-%m-%d")

    for contributor in contributor_list:
        if len(set(contributor["id_git_author_name_list"]) & contributor_same) > 0:
            for commit_date in contributor["code_commit_date_list"]:
                if from_date <= commit_date and commit_date <= to_date:
                    commit_count += 1
    return commit_count / 12.85




class CodeQualityGuaranteeMetricsModelInside(MetricsModel):
    def __init__(self, issue_index=None, pr_index=None, repo_index=None, json_file=None, git_index=None, out_index=None,
                 git_branch=None, from_date=None, end_date=None, community=None, level=None, company=None,
                 pr_comments_index=None, contributors_index=None):
        super().__init__(json_file, from_date, end_date, out_index, community, level)
        self.issue_index = issue_index
        self.repo_index = repo_index
        self.git_index = git_index
        self.git_branch = git_branch
        self.model_name = 'Code_Quality_Guarantee'
        self.pr_index = pr_index
        self.company = company
        self.pr_comments_index = pr_comments_index
        self.contributors_index = contributors_index
        self.commit_message_dict = {}

    def get_pr_message_count(self, repos_list, field, date_field="grimoire_creation_date", size=0, filter_field=None,
                             from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": size,
            "track_total_hits": True,
            "aggs": {
                "count_of_uuid": {
                    "cardinality": {
                        "field": field
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [{
                                    "simple_query_string": {
                                        "query": i,
                                        "fields": ["tag"]
                                    }} for i in repos_list],
                                "minimum_should_match": 1
                            }
                        },
                        {
                            "match_phrase": {
                                "pull_request": "true"
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range":
                                {
                                    filter_field: {
                                        "gte": 1
                                    }
                                }},
                        {
                            "range":
                                {
                                    date_field: {
                                        "gte": from_date.strftime("%Y-%m-%d"),
                                        "lt": to_date.strftime("%Y-%m-%d")
                                    }
                                }
                        }
                    ]
                }
            }
        }
        return query

    def get_pr_linked_issue_count(self, repo, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": 0,
            "track_total_hits": True,
            "aggs": {
                "count_of_uuid": {
                    "cardinality": {
                        "script": "if(doc.containsKey('pull_id')) {return doc['pull_id']} else {return doc['id']}"
                    }
                }
            },
            "query": {
                "bool": {
                    "should": [
                        {
                            "range": {
                                "linked_issues_count": {
                                    "gte": 1
                                }
                            }
                        },
                        {
                            "script": {
                                "script": "if (doc.containsKey('body') && doc['body'].size()>0 &&doc['body'].value.indexOf('" + repo + "/issue') != -1){return true}"
                            }
                        }
                    ],
                    "minimum_should_match": 1,
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {
                                        "simple_query_string": {
                                            "query": repo,
                                            "fields": [
                                                "tag"
                                            ]
                                        }
                                    }
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "grimoire_creation_date": {
                                    "gte": from_date.strftime("%Y-%m-%d"),
                                    "lt": to_date.strftime("%Y-%m-%d")
                                }
                            }
                        }
                    ]
                }
            }
        }
        return query

    def contributor_count(self, contributor_list):
        contributor_count = 0
        contributor_identity = set()

        for contributor in contributor_list:
            contributor_break_flag = False
            for identity in contributor["id_identity_list"]:
                if identity in contributor_identity:
                    contributor_break_flag = True
                    break
            if not contributor_break_flag:
                contributor_count += 1
            contributor_identity.update(contributor["id_identity_list"])
        return contributor_count

    def contributor_count_org_name(self, org_name, from_date, to_date, contributor_list):
        contributor_count = 0
        contributor_identity = set()

        from_date = from_date.strftime("%Y-%m-%d")
        to_date = to_date.strftime("%Y-%m-%d")

        for contributor in contributor_list:
            if len(contributor_identity & set(contributor["id_identity_list"])) == 0:
                for org in contributor["org_change_date_list"]:
                    if org.get("org_name") == org_name and check_times_has_overlap(org["first_date"], org["last_date"],from_date, to_date):
                        contributor_count += 1
                        break
                contributor_identity.update(contributor["id_identity_list"])
        return contributor_count

    # def commit_frequency(self, date, repos_list):
    #     query_commit_frequency = self.get_uuid_count_query(
    #         "cardinality", repos_list, "hash", "grimoire_creation_date", size=0, from_date=date - timedelta(days=90),
    #         to_date=date)
    #     commit_frequency = self.es_in.search(index=self.git_index, body=query_commit_frequency)[
    #         'aggregations']["count_of_uuid"]['value']
    #     return commit_frequency / 12.85

    def commit_frequency(self, from_date, to_date, contributor_list):
        commit_count = 0
        from_date = from_date.strftime("%Y-%m-%d")
        to_date = to_date.strftime("%Y-%m-%d")

        for contributor in contributor_list:
            for commit_date in contributor["code_commit_date_list"]:
                if from_date <= commit_date and commit_date <= to_date:
                    commit_count += 1
        return commit_count / 12.85

    def commit_frequency_org_name(self, org_name, from_date, to_date, contributor_list):
        commit_count = 0
        from_date = from_date.strftime("%Y-%m-%d")
        to_date = to_date.strftime("%Y-%m-%d")

        for contributor in contributor_list:
            for org in contributor["org_change_date_list"]:
                if org.get("org_name") == org_name and check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                    for commit_date in contributor["code_commit_date_list"]:
                        if get_latest_date(org["first_date"],from_date) <= commit_date and commit_date <= get_oldest_date(org["last_date"],to_date):
                            commit_count += 1
                    break
        return commit_count/12.85

    def is_maintained(self, date, repos_list, level):
        is_maintained_list = []
        if level == "repo":
            date_list_maintained = get_date_list(begin_date=str(
                date - timedelta(days=90)), end_date=str(date), freq='7D')
            for day in date_list_maintained:
                query_git_commit_i = self.get_uuid_count_query(
                    "cardinality", repos_list, "hash", size=0, from_date=day - timedelta(days=7), to_date=day)
                if self.git_branch is not None:
                    query_git_commit_i["query"]["bool"]["must"].append(
                        {"match_phrase": {"branches": self.git_branch}})
                commit_frequency_i = self.es_in.search(index=self.git_index, body=query_git_commit_i)[
                    'aggregations']["count_of_uuid"]['value']
                if commit_frequency_i > 0:
                    is_maintained_list.append("True")
                else:
                    is_maintained_list.append("False")

        elif level in ["project", "community"]:
            for repo in repos_list:
                query_git_commit_i = self.get_uuid_count_query("cardinality", [repo + '.git'], "hash",
                                                               from_date=date - timedelta(days=30), to_date=date)
                if self.git_branch is not None:
                    query_git_commit_i["query"]["bool"]["must"].append(
                        {"match_phrase": {"branches": self.git_branch}})
                commit_frequency_i = \
                self.es_in.search(index=self.git_index, body=query_git_commit_i)['aggregations']["count_of_uuid"][
                    'value']
                if commit_frequency_i > 0:
                    is_maintained_list.append("True")
                else:
                    is_maintained_list.append("False")
        try:
            return is_maintained_list.count("True") / len(is_maintained_list)
        except ZeroDivisionError:
            return 0

    def LOC_frequency(self, date, repos_list, field='lines_changed'):
        query_LOC_frequency = self.get_uuid_count_query(
            'sum', repos_list, field, 'grimoire_creation_date', size=0, from_date=date - timedelta(days=90),
            to_date=date)
        if self.git_branch is not None:
            query_LOC_frequency["query"]["bool"]["must"].append({"match_phrase": {"branches": self.git_branch}})
        LOC_frequency = self.es_in.search(index=self.git_index, body=query_LOC_frequency)[
            'aggregations']['count_of_uuid']['value']
        return LOC_frequency / 12.85

    def code_review_ratio(self, date, repos_list):
        if self.pr_index is None:
            return None, None
        query_pr_count = self.get_uuid_count_query(
            "cardinality", repos_list, "uuid", size=0, from_date=(date - timedelta(days=90)), to_date=date)
        pr_count = self.es_in.search(index=self.pr_index, body=query_pr_count)[
            'aggregations']["count_of_uuid"]['value']
        query_pr_body = self.get_pr_message_count(repos_list, "uuid", "grimoire_creation_date", size=0,
                                                  filter_field="num_review_comments_without_bot",
                                                  from_date=(date - timedelta(days=90)), to_date=date)
        prs = self.es_in.search(index=self.pr_index, body=query_pr_body)[
            'aggregations']["count_of_uuid"]['value']
        try:
            return prs / pr_count, pr_count
        except ZeroDivisionError:
            return None, 0

    def git_pr_linked_ratio(self, date, repos_list):
        commit_frequency = self.get_uuid_count_query("cardinality", repos_list, "hash", "grimoire_creation_date",
                                                     size=10000, from_date=date - timedelta(days=90), to_date=date)
        commits_without_merge_pr = {
            "bool": {
                "should": [{"script": {
                    "script": "if (doc.containsKey('message') && doc['message'].size()>0 &&doc['message'].value.indexOf('Merge pull request') == -1){return true}"
                }
                }],
                "minimum_should_match": 1}
        }
        commit_frequency["query"]["bool"]["must"].append(commits_without_merge_pr)
        if self.git_branch is not None:
            commit_frequency["query"]["bool"]["must"].append({"match_phrase": {"branches": self.git_branch}})
        commit_message = self.es_in.search(index=self.git_index, body=commit_frequency)
        commit_count = commit_message['aggregations']["count_of_uuid"]['value']
        commit_pr_cout = 0
        commit_all_message = [commit_message_i['_source']['hash'] for commit_message_i in
                              commit_message['hits']['hits']]

        if self.pr_index is None:
            return len(commit_all_message), None, None, None

        for commit_message_i in set(commit_all_message):
            commit_hash = commit_message_i
            if commit_hash in self.commit_message_dict:
                commit_pr_cout += self.commit_message_dict[commit_hash]
            else:
                pr_message = self.get_uuid_count_query("cardinality", repos_list, "uuid", "grimoire_creation_date",
                                                       size=0)
                commit_hash_query = {"bool": {"should": [{"match_phrase": {"commits_data": commit_hash}}],
                                              "minimum_should_match": 1
                                              }
                                     }
                pr_message["query"]["bool"]["must"].append(commit_hash_query)
                prs = self.es_in.search(index=self.pr_index, body=pr_message)
                if prs['aggregations']["count_of_uuid"]['value'] > 0:
                    self.commit_message_dict[commit_hash] = 1
                    commit_pr_cout += 1
                else:
                    self.commit_message_dict[commit_hash] = 0
        if commit_count > 0:
            return len(commit_all_message), commit_pr_cout, commit_pr_cout / len(commit_all_message)
        else:
            return 0, None, None

    def code_merge_ratio(self, date, repos_list):
        if self.pr_index is None:
            return None, None
        query_pr_body = self.get_uuid_count_query("cardinality", repos_list, "uuid", "grimoire_creation_date", size=0,
                                                  from_date=(date - timedelta(days=90)), to_date=date)
        query_pr_body["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "true"}})
        query_pr_body["query"]["bool"]["must"].append({"match_phrase": {"merged": "true"}})
        pr_merged_count = self.es_in.search(index=self.pr_index, body=query_pr_body)[
            'aggregations']["count_of_uuid"]['value']
        query_pr_body["query"]["bool"]["must"].append({
            "script": {
                "script": "if(doc['merged_by_data_name'].size() > 0 && doc['author_name'].size() > 0 && doc['merged_by_data_name'].value !=  doc['author_name'].value){return true}"
            }
        })
        prs = self.es_in.search(index=self.pr_index, body=query_pr_body)[
            'aggregations']["count_of_uuid"]['value']
        try:
            return prs / pr_merged_count, pr_merged_count
        except ZeroDivisionError:
            return None, 0

    def pr_issue_linked(self, date, repos_list):
        if self.pr_index is None or self.pr_comments_index is None:
            return None
        pr_linked_issue = 0
        for repo in repos_list:
            query_pr_linked_issue = self.get_pr_linked_issue_count(
                repo, from_date=date - timedelta(days=90), to_date=date)
            pr_linked_issue += \
            self.es_in.search(index=(self.pr_index, self.pr_comments_index), body=query_pr_linked_issue)[
                'aggregations']["count_of_uuid"]['value']
        query_pr_count = self.get_uuid_count_query(
            "cardinality", repos_list, "uuid", size=0, from_date=(date - timedelta(days=90)), to_date=date)
        query_pr_count["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "true"}})
        pr_count = self.es_in.search(index=self.pr_index,
                                     body=query_pr_count)[
            'aggregations']["count_of_uuid"]['value']
        try:
            return pr_linked_issue / pr_count
        except ZeroDivisionError:
            return None

    def metrics_model_enrich(self, repos_list, label, type=None, level=None, date_list=None):
        level = level if level != None else self.level
        date_list = date_list if date_list != None else self.date_list
        item_datas = []
        last_metrics_data = {}
        self.commit_message_dict = {}
        for date in date_list:
            logger.info(str(date) + "--" + self.model_name + "--" + label)
            created_since = self.created_since(date, repos_list)
            if created_since is None:
                continue
            from_date = date - timedelta(days=90)
            to_date = date
            commit_contributor_list = self.get_contributor_list(from_date, to_date, repos_list, "code_commit_date_list")
            pr_contributor_list = self.get_contributor_list(from_date, to_date, repos_list, "pr_creation_date_list")
            pr_comment_contributor_list = self.get_contributor_list(from_date, to_date, repos_list,
                                                                    "pr_review_date_list")

            git_pr_linked_ratio = self.git_pr_linked_ratio(date, repos_list)
            code_review_ratio, pr_count = self.code_review_ratio(date, repos_list)
            code_merge_ratio, pr_merged_count = self.code_merge_ratio(date, repos_list)
            # upstream_set = get_data(self.es_in,"openeuler_fedora_upstream_contributors_org_repo", from_date.strftime("%Y-%m-%d"),to_date.strftime("%Y-%m-%d"), self.company, exclusion="https://gitee.com/openeuler/kernel")
            upstream_set = get_data(self.es_in,"openeuler_fedora_upstream_contributors_org_repo", from_date.strftime("%Y-%m-%d"),to_date.strftime("%Y-%m-%d"), self.company, exclusion="https://github.com/torvalds/linux")
            if self.company == "Huawei":
                my_set = get_data(self.es_in, self.contributors_index, from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"), self.company,exclusion="https://gitee.com/src-openeuler/kernel")
            else:
                my_set = get_data(self.es_in, self.contributors_index, from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"), self.company,exclusion="https://src.fedoraproject.org/rpms/kernel")
            metrics_data = {
                'uuid': get_uuid(str(date), self.community, level, label, self.model_name, type),
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'contributor_count': self.contributor_count(commit_contributor_list + pr_contributor_list + pr_comment_contributor_list),
                'contributor_count_inside': None if self.company is None else self.contributor_count_org_name(self.company, from_date, to_date, (commit_contributor_list + pr_contributor_list + pr_comment_contributor_list)),
                'contributor_count_non_org': self.contributor_count_org_name(None, from_date, to_date, (commit_contributor_list + pr_contributor_list + pr_comment_contributor_list)),
                'contributor_count_same': len(upstream_set & my_set),
                'active_C2_contributor_count': self.contributor_count(commit_contributor_list),
                'active_C1_pr_create_contributor': self.contributor_count(pr_contributor_list),
                'active_C1_pr_comments_contributor': self.contributor_count(pr_comment_contributor_list),
                'commit_frequency': self.commit_frequency(from_date, to_date, commit_contributor_list),
                'commit_frequency_inside': None if self.company is None else self.commit_frequency_org_name(self.company, from_date, to_date, commit_contributor_list),
                'commit_frequency_non_org': self.commit_frequency_org_name(None, from_date, to_date, commit_contributor_list),
                'commit_frequency_count_same': commit_frequency_count_same(upstream_set & my_set, from_date, to_date, commit_contributor_list),
                'is_maintained': round(self.is_maintained(date, repos_list, level), 4),
                'LOC_frequency': self.LOC_frequency(date, repos_list),
                'lines_added_frequency': self.LOC_frequency(date, repos_list, 'lines_added'),
                'lines_removed_frequency': self.LOC_frequency(date, repos_list, 'lines_removed'),
                'pr_issue_linked_ratio': self.pr_issue_linked(date, repos_list),
                'code_review_ratio': code_review_ratio,
                'code_merge_ratio': code_merge_ratio,
                'pr_count': pr_count,
                'pr_merged_count': pr_merged_count,
                'pr_commit_count': git_pr_linked_ratio[0],
                'pr_commit_linked_count': git_pr_linked_ratio[1],
                'git_pr_linked_ratio': git_pr_linked_ratio[2],
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            self.cache_last_metrics_data(metrics_data, last_metrics_data)
            score = code_quality_guarantee(code_quality_decay(metrics_data, last_metrics_data, level), level)
            metrics_data["code_quality_guarantee"] = score
            item_datas.append(metrics_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")

    def cache_last_metrics_data(self, item, last_metrics_data):
        for i in ["code_merge_ratio", "code_review_ratio", "pr_issue_linked_ratio", "git_pr_linked_ratio"]:
            if item[i] != None:
                data = [item[i], item['grimoire_creation_date']]
                last_metrics_data[i] = data

class DeveloperAttractionMetricsModelInside(MetricsModel):
    def __init__(self, json_file, company, git_index, contributors_index, out_index, from_date, end_date, community=None, level=None):
        super().__init__(json_file, from_date, end_date, out_index, community, level)
        self.git_index = git_index
        self.contributors_index = contributors_index
        self.model_name = "Developer Attraction"
        self.company = company


    def contributor_count(self, contributor_list):
        contributor_count = 0
        contributor_identity = set()

        for contributor in contributor_list:
            contributor_break_flag = False
            for identity in contributor["id_identity_list"]:
                if identity in contributor_identity:
                    contributor_break_flag = True
                    break
            if not contributor_break_flag:
                contributor_count += 1
            contributor_identity.update(contributor["id_identity_list"])
        return contributor_count

    def d2_attraction_time(self, date, d2_contributor_list, c1_contributor_list, c0_contributor_list):
        c1_contributor_dict = {}
        c0_contributor_dict = {}
        for contributor in c1_contributor_list:
            c1_contributor_dict[contributor["uuid"]] = contributor
        for contributor in c0_contributor_list:
            c0_contributor_dict[contributor["uuid"]] = contributor

        contributor_count = 0
        contributor_identity = set()
        convertion_time = []
        for contributor in d2_contributor_list:
            contributor_break_flag = False
            for identity in contributor["id_identity_list"]:
                if identity in contributor_identity:
                    contributor_break_flag = True
                    break
            contributor_identity.update(contributor["id_identity_list"])
            if not contributor_break_flag:
                contributor_count += 1
                if contributor["uuid"] in c1_contributor_dict.keys():
                    c1_contributor = c1_contributor_dict[contributor["uuid"]]
                    max_first_date = ""
                    for first_date in ["first_issue_creation_date","first_issue_comments_date","first_pr_creation_date","first_issue_comments_date"]:
                        if c1_contributor[first_date] is not None and str_to_datetime(c1_contributor[first_date]) > (
                                date - timedelta(days=90)) and str_to_datetime(c1_contributor[first_date]) < date:
                            max_first_date = max(c1_contributor[first_date], max_first_date)
                    if max_first_date != "":
                        diff_time = get_time_diff_days(contributor["first_code_commit_date"], max_first_date)
                        convertion_time.append(diff_time if diff_time > 0 else 0.0)
                elif contributor["uuid"] in c0_contributor_dict.keys():
                    c0_contributor = c0_contributor_dict[contributor["uuid"]]
                    max_first_date = ""
                    for first_date in ["first_star_date", "first_fork_date"]:
                        if c1_contributor[first_date] is not None and str_to_datetime(c0_contributor[first_date]) > (
                                date - timedelta(days=90)) and str_to_datetime(c0_contributor[first_date]) < date:
                            max_first_date = max(c0_contributor[first_date], max_first_date)
                    if max_first_date != "":
                        diff_time = get_time_diff_days(contributor["first_code_commit_date"], max_first_date)
                        convertion_time.append(diff_time if diff_time > 0 else 0.0)

        return round(sum(convertion_time) / contributor_count, 4) if contributor_count else 0.0

    def metrics_model_enrich(self, repos_list, label, type=None, level=None, date_list=None):
        level = level if level is not None else self.level
        date_list = date_list if date_list is not None else self.date_list
        item_datas = []
        for date in date_list:
            print(str(date)+"--"+self.model_name+"--"+label)
            created_since = self.created_since(date, repos_list)
            if created_since is None:
                continue
            star_contributor_list = self.get_contributor_list(date, repos_list, "first_star_date")
            fork_contributor_list = self.get_contributor_list(date, repos_list, "first_fork_date")
            commit_contributor_list = self.get_contributor_list(date, repos_list, "first_code_commit_date")
            issue_contributor_list = self.get_contributor_list(date, repos_list, "first_issue_creation_date")
            issue_comment_contributor_list = self.get_contributor_list(date, repos_list, "first_issue_comments_date")
            pr_contributor_list = self.get_contributor_list(date, repos_list, "first_pr_creation_date")
            pr_comment_contributor_list = self.get_contributor_list(date, repos_list, "first_pr_review_date")

            metrics_data = {
                'uuid': get_uuid(str(date), self.community, level, label, self.model_name, type),
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'c0_attraction_count': self.contributor_count(star_contributor_list + fork_contributor_list),
                'c1_attraction_count': self.contributor_count(issue_contributor_list + issue_comment_contributor_list + pr_contributor_list + pr_comment_contributor_list),
                'c1_pr_create_attraction_count': self.contributor_count(pr_contributor_list),
                'c1_issue_create_attraction_count': self.contributor_count(issue_contributor_list),
                'c1_pr_comments_attraction_count': self.contributor_count(pr_comment_contributor_list),
                'c1_issue_comments_attraction_count': self.contributor_count(issue_comment_contributor_list),
                'd2_attraction_count': self.contributor_count(commit_contributor_list),
                'd2_attraction_time': self.d2_attraction_time(date, commit_contributor_list,
                                                                  (issue_contributor_list+issue_comment_contributor_list+pr_contributor_list+pr_comment_contributor_list),
                                                                  (star_contributor_list+fork_contributor_list)),
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            score = developer_attraction(metrics_data)
            metrics_data["developer_attraction_score"] = score
            item_datas.append(metrics_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")


class OrganizationsActivityMetricsModelInside(MetricsModel):
    def __init__(self, issue_index, repo_index=None, pr_index=None, json_file=None, git_index=None, out_index=None, git_branch=None, from_date=None, end_date=None, community=None, level=None, company=None, issue_comments_index=None, pr_comments_index=None, contributors_index=None):
        super().__init__(json_file, from_date, end_date, out_index, community, level)
        self.issue_index = issue_index
        self.repo_index = repo_index
        self.git_index = git_index
        self.pr_index = pr_index
        self.git_branch = git_branch
        self.issue_comments_index = issue_comments_index
        self.pr_comments_index = pr_comments_index
        self.contributors_index = contributors_index
        self.company = company
        self.model_name = 'Organizations Activity'
        self.org_name_dict = {}

    # def add_org_name(self, contributor_list):
    #     for contributor in contributor_list:
    #         for org in contributor["org_change_date_list"]:
    #             org_name = org.get("org_name") if org.get("org_name") else org.get("domain")
    #             is_org = True if org.get("org_name") else False
    #             self.org_name_dict[org_name] = is_org

    def add_org_name(self, contributor_list):
        for contributor in contributor_list:
            for org in contributor["org_change_date_list"]:
                org_name = org.get("org_name") if org.get("org_name") else "Non-org"
                is_org = True if org.get("org_name") else False
                self.org_name_dict[org_name] = is_org

    def contributor_count(self, from_date, to_date, contributor_list):
        contributor_count = 0
        contributor_identity = set()
        org_contributor_count_dict = {}  # {"org_name": count}
        org_contributor_identity_dict = {}  # {"org_name": {author_name1,author_name2}}

        from_date = from_date.strftime("%Y-%m-%d")
        to_date = to_date.strftime("%Y-%m-%d")

        for contributor in contributor_list:
            for org in contributor["org_change_date_list"]:
                if org.get("org_name") is not None and check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                    contributor_identity.add(contributor["id_git_author_name_list"][0])
                    break
            
            org_change_date_list = contributor["org_change_date_list"]
            if len(org_change_date_list) == 0:
                continue
            org = org_change_date_list[len(org_change_date_list)-1]
            if check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                org_name = org.get("org_name") if org.get("org_name") else "Non-org"
                org_contributor_identity = org_contributor_identity_dict.get(org_name, set())
                org_contributor_identity.add(contributor["id_git_author_name_list"][0])
                org_contributor_identity_dict[org_name] = org_contributor_identity
                org_contributor_count_dict[org_name] = len(org_contributor_identity)
                
                   

        return len(contributor_identity), org_contributor_count_dict
    
    def contributor_count2(self, from_date, to_date, contributor_list):
        contributor_count = 0
        contributor_identity = set()
        org_contributor_count_dict = {}  # {"org_name": count}
        org_contributor_identity_dict = {}  # {"org_name": {author_name1,author_name2}}

        from_date = from_date.strftime("%Y-%m-%d")
        to_date = to_date.strftime("%Y-%m-%d")

        for contributor in contributor_list:
            contributor_break_flag = False
            for org in contributor["org_change_date_list"]:
                if org.get("org_name") is not None and check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                    for identity in contributor["id_identity_list"]:
                        if identity in contributor_identity:
                            contributor_break_flag = True
                            break
                    if not contributor_break_flag:
                        contributor_count += 1
                    contributor_identity.update(contributor["id_identity_list"])
                    break

            org_contributor_break_flag = False
            for org in contributor["org_change_date_list"]:
                if check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                    org_name = org.get("org_name") if org.get("org_name") else "Non-org"
                    org_contributor_identity = org_contributor_identity_dict.get(org_name, set())
                    for identity in contributor["id_identity_list"]:
                        if identity in org_contributor_identity:
                            org_contributor_break_flag = True
                            break
                    if not org_contributor_break_flag:
                        org_contributor_count_dict[org_name] = org_contributor_count_dict.get(org_name, 0) + 1
                        org_contributor_identity.update(contributor["id_identity_list"])
                        org_contributor_identity_dict[org_name] = org_contributor_identity
                    continue

        return contributor_count, org_contributor_count_dict

    # def commit_frequency(self, from_date, to_date, contributor_list):
    #     total_count = 0
    #     commit_count = 0
    #     org_commit_count_dict = {}  # {"org_name": count}
    #     org_commit_percentage_dict = {}  # {"org_name": [org_count, org_percentage, percentage]}
    #
    #     from_date = from_date.strftime("%Y-%m-%d")
    #     to_date = to_date.strftime("%Y-%m-%d")
    #
    #     for contributor in contributor_list:
    #         for commit_date in contributor["code_commit_date_list"]:
    #             if from_date <= commit_date and commit_date <= to_date:
    #                 total_count += 1
    #
    #         for org in contributor["org_change_date_list"]:
    #             if org.get("org_name") is not None and check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
    #                 for commit_date in contributor["code_commit_date_list"]:
    #                     if from_date <= commit_date and commit_date <= to_date:
    #                         commit_count += 1
    #                 break
    #
    #         for org in contributor["org_change_date_list"]:
    #             if check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
    #                 org_name = org.get("org_name") if org.get("org_name") else org.get("domain")
    #                 count = org_commit_count_dict.get(org_name, 0)
    #                 for commit_date in contributor["code_commit_date_list"]:
    #                     if from_date <= commit_date and commit_date <= to_date:
    #                         count += 1
    #                 org_commit_count_dict[org_name] = count
    #
    #     if total_count == 0:
    #         return 0 ,{}
    #     for org_name, count in org_commit_count_dict.items():
    #         if self.org_name_dict[org_name]:
    #             org_commit_percentage_dict[org_name] = [count, count/total_count, 0 if commit_count == 0 else count/commit_count]
    #         else:
    #             org_commit_percentage_dict[org_name] = [count, count/total_count, 0 if (total_count - commit_count) == 0 else count/(total_count - commit_count)]
    #     return commit_count/12.85, org_commit_percentage_dict

    def commit_frequency(self, from_date, to_date, contributor_list):
        total_count = 0
        commit_count = 0
        org_commit_count_dict = {}  # {"org_name": count}
        org_commit_percentage_dict = {}  # {"org_name": [org_count, org_percentage, percentage]}

        from_date = from_date.strftime("%Y-%m-%d")
        to_date = to_date.strftime("%Y-%m-%d")

        for contributor in contributor_list:
            for commit_date in contributor["code_commit_date_list"]:
                if from_date <= commit_date and commit_date <= to_date:
                    total_count += 1

            for org in contributor["org_change_date_list"]:
                if org.get("org_name") is not None and check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                    for commit_date in contributor["code_commit_date_list"]:
                        if from_date <= commit_date and commit_date <= to_date:
                            commit_count += 1
                    break

            org_change_date_list = contributor["org_change_date_list"]
            if len(org_change_date_list) == 0:
                continue
            org = org_change_date_list[len(org_change_date_list)-1]
            if check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                org_name = org.get("org_name") if org.get("org_name") else "Non-org"
                count = org_commit_count_dict.get(org_name, 0)
                for commit_date in contributor["code_commit_date_list"]:
                    if from_date <= commit_date and commit_date <= to_date:
                        count += 1
                org_commit_count_dict[org_name] = count
                    

        if total_count == 0:
            return 0 ,{}
        for org_name, count in org_commit_count_dict.items():
            if self.org_name_dict[org_name]:
                org_commit_percentage_dict[org_name] = [count, count/total_count, 0 if commit_count == 0 else count/commit_count]
            else:
                org_commit_percentage_dict[org_name] = [count, count/total_count, 0 if (total_count - commit_count) == 0 else count/(total_count - commit_count)]
        return commit_count/12.85, org_commit_percentage_dict

    def contribution_last(self, from_date, to_date, contributor_list):
        contribution_last = 0
        contributor_dict = {} #{"repo_name":[contributor1,contributor2]}
        for contributor in contributor_list:
            repo_contributor_list = contributor_dict.get(contributor["repo_name"], [])
            repo_contributor_list.append(contributor)
            contributor_dict[contributor["repo_name"]] = repo_contributor_list

        date_list = get_date_list(begin_date=str(from_date), end_date=str(to_date), freq='7D')
        for repo, repo_contributor_list in contributor_dict.items():
            for day in date_list:
                org_name_set = set()
                from_day = (day - timedelta(days=7)).strftime("%Y-%m-%d")
                to_day = day.strftime("%Y-%m-%d")
                for contributor in repo_contributor_list:
                    for org in contributor["org_change_date_list"]:
                        if org.get("org_name") is not None and check_times_has_overlap(org["first_date"], org["last_date"], from_day, to_day):
                            for commit_date in contributor["code_commit_date_list"]:
                                if from_day <= commit_date and commit_date <= to_day:
                                    org_name_set.add(org.get("org_name"))
                                    break
                contribution_last += len(org_name_set)
        return contribution_last

    def commit_day_org(self, from_date, to_date, contributor_list):
        org_commit_day_dict = {}  # {"org_name": {day1,day2}}

        from_date = from_date.strftime("%Y-%m-%d")
        to_date = to_date.strftime("%Y-%m-%d")

        for contributor in contributor_list:
            for org in contributor["org_change_date_list"]:
                if check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                    org_name = org.get("org_name") if org.get("org_name") else "Non-org"
                    day_set = org_commit_day_dict.get(org_name, set())
                    for commit_date in contributor["code_commit_date_list"]:
                        if from_date <= commit_date and commit_date <= to_date:
                            day_set.add(commit_date.split("T")[0])
                    org_commit_day_dict[org_name] = day_set
        return org_commit_day_dict


    def metrics_model_enrich(self, repos_list, label, type=None, level=None, date_list=None):
        level = level if level != None else self.level
        date_list = date_list if date_list != None else self.date_list
        date_list = []
        for year in range(2022, 2023):
            date_list.append(str(year) + "-12-31")
        date_list = [str_to_datetime(date).replace(tzinfo=None) for date in date_list]
        item_datas = []
        self.org_name_dict = {}
        for date in date_list:
            logger.info(str(date)+"--"+self.model_name+"--"+label)
            created_since = self.created_since(date, repos_list)
            if created_since is None:
                continue
            from_date = str_to_datetime("2022-01-01").replace(tzinfo=None)
            to_date = str_to_datetime("2023-01-01").replace(tzinfo=None)
            contributor_list = self.get_contributor_list(from_date, to_date, repos_list, "code_commit_date_list")
            if len(contributor_list) == 0:
                continue
            # bot_list = ["i-robot","mindspore-ci-bot",
            #             "pytorchbot","Website Deployment Script","chronos_secgrp_pytorch_oss_ci_oncall","Pytorch Test Infra",
            #             "A. Unique TensorFlower","TensorFlower Gardener",
            #             "bot"]
            # contributor_list = [source for source in contributor_list if source["id_git_author_name_list"][0] not in bot_list]
            self.add_org_name(contributor_list)
            contributor_count, org_contributor_count_dict = self.contributor_count(from_date, to_date, contributor_list)
            commit_frequency, org_commit_percentage_dict = self.commit_frequency(from_date, to_date, contributor_list)
            org_count = self.org_count(from_date, to_date, contributor_list)
            contribution_last = self.contribution_last(from_date, to_date, contributor_list)
            commit_day_org = self.commit_day_org(from_date, to_date, contributor_list)
            for org_name in self.org_name_dict.keys():
                if org_name not in org_commit_percentage_dict.keys():
                    continue
                metrics_data = {
                    'uuid': get_uuid(str(date), org_name, self.community, level, label, self.model_name, type),
                    'level': level,
                    'type': type,
                    'label': label,
                    'model_name': self.model_name,
                    'org_name': org_name,
                    'is_org': self.org_name_dict[org_name],
                    'contributor_count': contributor_count,
                    'contributor_org_count': org_contributor_count_dict.get(org_name),
                    'commit_frequency': round(commit_frequency, 4),
                    'commit_frequency_org': round(org_commit_percentage_dict[org_name][0], 4),
                    'commit_frequency_org_percentage': round(org_commit_percentage_dict[org_name][1], 4),
                    'commit_frequency_percentage': round(org_commit_percentage_dict[org_name][2], 4),
                    'commit_day_org': len(commit_day_org.get(org_name, set())),
                    'org_count': org_count,
                    'contribution_last': contribution_last,
                    'grimoire_creation_date': date.isoformat(),
                    'metadata__enriched_on': datetime_utcnow().isoformat()
                }
                score = organizations_activity(metrics_data, level)
                metrics_data["organizations_activity"] = score
                item_datas.append(metrics_data)
                if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                    self.es_out.bulk_upload(item_datas, "uuid")
                    item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")