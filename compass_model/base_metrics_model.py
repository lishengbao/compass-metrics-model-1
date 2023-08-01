import json
import logging
import hashlib
import math
import pendulum
import pandas as pd

from elasticsearch import helpers
from compass_common.opensearch_client_utils import get_elasticsearch_client
from compass_common.datetime import (get_date_list,
                                     datetime_utcnow)
from compass_common.uuid_utils import get_uuid
from compass_metrics.db_dsl import get_release_index_mapping, get_repo_message_query
from compass_metrics.git_metrics import created_since, updated_since
from compass_metrics.repo_metrics import recent_releases_count
from typing import Dict, Any

logger = logging.getLogger(__name__)

MAX_BULK_UPDATE_SIZE = 5000

SOFTWARE_ARTIFACT = "software-artifact"
GOVERNANCE = "governance"

DECAY_COEFFICIENT = 0.0027
INCREMENT_DECAY_METRICS = ["issue_first_reponse_avg",
                           "issue_first_reponse_mid",
                           "bug_issue_open_time_avg",
                           "bug_issue_open_time_mid",
                           "pr_open_time_avg",
                           "pr_open_time_mid"]
DECREASE_DECAY_METRICS = ["comment_frequency",
                          "code_review_count",
                          "code_merge_ratio",
                          "code_review_ratio",
                          "pr_issue_linked_ratio",
                          "git_pr_linked_ratio"]


def get_dict_hash(dictionary: Dict[str, Any]) -> str:
    """MD5 hash of a dictionary."""
    dhash = hashlib.md5()
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()


def get_repo_list(json_file, source):
    """ Get list of repositories """
    all_repo = []
    all_repo_json = json.load(open(json_file))
    for project in all_repo_json:
        origin_software_artifact = source + "-" + SOFTWARE_ARTIFACT
        origin_governance = source + "-" + GOVERNANCE
        for key in all_repo_json[project].keys():
            if key == origin_software_artifact or key == origin_governance or key == source:
                for repo in all_repo_json[project].get(key):
                    all_repo.append(repo)
    return all_repo


def get_community_repo_list(json_file, source):
    """ Get community repositories, distinguish between software artifact and governance repositories. """
    software_artifact_repo = []
    governance_repo = []
    all_repo_json = json.load(open(json_file))
    for project in all_repo_json:
        origin_software_artifact = source + "-" + SOFTWARE_ARTIFACT
        origin_governance = source + "-" + GOVERNANCE
        for key in all_repo_json[project].keys():
            if key == origin_software_artifact:
                for j in all_repo_json[project].get(key):
                    software_artifact_repo.append(j)
            if key == origin_governance:
                for j in all_repo_json[project].get(key):
                    governance_repo.append(j)
    return software_artifact_repo, governance_repo


def create_release_index(es_client, all_repo, repo_index, release_index):
    es_exist = es_client.indices.exists(index=release_index)
    if not es_exist:
        es_client.indices.create(index=release_index, body=get_release_index_mapping())
    for repo_url in all_repo:
        query = get_repo_message_query(repo_url)
        query_hits = es_client.search(index=repo_index, body=query)["hits"]["hits"]
        if len(query_hits) > 0 and query_hits[0]["_source"].get("releases"):
            items = query_hits[0]["_source"]["releases"]
            add_release_message(es_client, release_index, repo_url, items)


def add_release_message(es_client, release_index, repo_url, releases):
    item_datas = []
    for item in releases:
        release_data = {
            "_index": release_index,
            "_id": get_uuid(str(item["id"])),
            "_source": {
                "uuid": get_uuid(str(item["id"])),
                "id": item["id"],
                "tag": repo_url,
                "tag_name": item["tag_name"],
                "target_commitish": item["target_commitish"],
                "prerelease": item["prerelease"],
                "name": item["name"],
                "author_login": item["author"]["login"],
                "author_name": item["author"]["name"],
                "grimoire_creation_date": item["created_at"],
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
        }
        item_datas.append(release_data)
        if len(item_datas) > MAX_BULK_UPDATE_SIZE:
            helpers.bulk(client=es_client, actions=item_datas)
            item_datas = []
    helpers.bulk(client=es_client, actions=item_datas)


def cache_last_metrics_data(item, last_metrics_data):
    cache_metrics = INCREMENT_DECAY_METRICS + DECREASE_DECAY_METRICS
    for metrics in cache_metrics:
        if metrics in item:
            data = [item[metrics], item['grimoire_creation_date']]
            last_metrics_data[metrics] = data


def get_score_ahp(metrics_data, metrics_weights_thresholds):
    total_weight = 0
    total_score = 0
    for metrics, weights_thresholds in metrics_weights_thresholds.items():
        total_weight += weights_thresholds["weight"]
        param_data = metrics_data[metrics]
        if param_data is None:
            if weights_thresholds["weight"] >= 0:
                param_data = 0
            else:
                param_data = weights_thresholds["threshold"]
        total_score += get_param_score(param_data, weights_thresholds["threshold"], weights_thresholds["weight"])
    try:
        return round(total_score / total_weight, 5)
    except ZeroDivisionError:
        return 0.0


def get_param_score(param, max_value, weight=1):
    """Return paramater score given its current value, max value and
    parameter weight."""
    return (math.log(1 + param) / math.log(1 + max(param, max_value))) * weight


def increment_decay(last_data, threshold, days):
    return min(last_data + DECAY_COEFFICIENT * threshold * days, threshold)


def decrease_decay(last_data, threshold, days):
    return max(last_data - DECAY_COEFFICIENT * threshold * days, 0)


def normalize(score, min_score, max_score):
    return (score - min_score) / (max_score - min_score)


class BaseMetricsModel:
    def __init__(self, repo_index, git_index, issue_index, pr_index, issue_comments_index, pr_comments_index,
                 contributors_index, release_index, out_index, from_date, end_date, level, community, source,
                 json_file, model_name, metrics_weights_thresholds, algorithm="AHP", custom_fields=None):
        """ Metrics Model is designed for the integration of multiple CHAOSS metrics.
        :param repo_index: repo index
        :param git_index: git index
        :param issue_index: Issue index
        :param pr_index: pr index
        :param issue_comments_index: issue comment index
        :param pr_comments_index: pr comment index
        :param contributors_index: contributor index
        :param release_index: release index
        :param out_index: target index for Metrics Model.
        :param from_date: the beginning of time for metric model
        :param end_date: the end of time for metric model,
        :param level: str representation of the metrics, choose from repo, project, community.
        :param community: used to mark the repo belongs to which community.
        :param source: Is the repo data source gitee or github
        :param json_file: the path of json file containing repository message.
        :param model_name: the model name
        :param metrics_weights_thresholds: dict representation of metrics, the dict values include weights and thresholds.
        :param algorithm: The algorithm chosen by the model,include AHP.
        :param custom_fields: custom_fields
        """
        self.repo_index = repo_index
        self.git_index = git_index
        self.issue_index = issue_index
        self.pr_index = pr_index
        self.issue_comments_index = issue_comments_index
        self.pr_comments_index = pr_comments_index
        self.contributors_index = contributors_index
        self.release_index = release_index
        self.out_index = out_index
        self.from_date = from_date
        self.end_date = end_date
        self.level = level
        self.community = community
        self.source = source
        self.json_file = json_file
        self.model_name = model_name
        self.algorithm = algorithm
        self.client = None

        if type(metrics_weights_thresholds) == dict:
            self.metrics_weights_thresholds = metrics_weights_thresholds
            self.metrics_weights_thresholds_hash = get_dict_hash(metrics_weights_thresholds)
        else:
            raise Exception("Invalid metrics param.")

        if type(custom_fields) == dict:
            self.custom_fields = custom_fields
            self.custom_fields_hash = get_dict_hash(custom_fields)
        else:
            self.custom_fields = {}
            self.custom_fields_hash = None

    def metrics_model_metrics(self, elastic_url):
        self.client = get_elasticsearch_client(elastic_url)
        if self.level == "repo":
            repo_list = get_repo_list(self.json_file, self.source)
            if len(repo_list) > 0:
                for repo in repo_list:
                    self.metrics_model_enrich([repo], repo, self.level)
        if self.level == "community":
            software_artifact_repo_list, governance_repo_list = get_community_repo_list(self.json_file, self.source)
            repo_list = software_artifact_repo_list + governance_repo_list
            if len(repo_list) > 0:
                for repo in repo_list:
                    self.metrics_model_enrich([repo], repo, "repo")
            if len(software_artifact_repo_list) > 0:
                self.metrics_model_enrich(software_artifact_repo_list, self.community, self.level, "software-artifact")
            if len(governance_repo_list) > 0:
                self.metrics_model_enrich(governance_repo_list, self.community, self.level, "governance")

    def metrics_model_enrich(self, repo_list, label, level, type=None):
        last_metrics_data = {}
        create_release_index(self.client, repo_list, self.repo_index, self.release_index)
        date_list = get_date_list(self.from_date, self.end_date)
        item_datas = []
        for date in date_list:
            logger.info(f"{str(date)}--{self.model_name}--{label}")
            created_since_metric = created_since(self.client, self.git_index, date, repo_list)
            if created_since_metric is None:
                continue
            metrics = self.get_metrics(date, repo_list)
            metrics_uuid = get_uuid(str(date), self.community, level, label, self.model_name, type,
                                    self.custom_fields_hash)
            metrics_data = {
                'uuid': metrics_uuid,
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                **metrics,
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat(),
                **self.custom_fields
            }
            cache_last_metrics_data(metrics_data, last_metrics_data)
            metrics_data["score"] = self.get_metrics_score(self.metrics_decay(metrics_data, last_metrics_data))
            item_data = {
                "_index": self.out_index,
                "_id": metrics_uuid,
                "_source": metrics_data
            }
            item_datas.append(item_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                helpers.bulk(client=self.client, actions=item_datas)
                item_datas = []
        helpers.bulk(client=self.client, actions=item_datas)

    def get_metrics(self, date, repo_list):
        metrics = {}
        for metric_field in self.metrics_weights_thresholds.keys():
            if metric_field == "created_since":
                metrics.update(created_since(self.client, self.git_index, date, repo_list))
            elif metric_field == "updated_since":
                metrics.update(updated_since(self.client, self.git_index, date, repo_list))
            elif metric_field == "recent_releases_count":
                metrics.update(recent_releases_count(self.client, self.release_index, date, repo_list))
        return metrics

    def get_metrics_score(self, metrics_data):
        new_metrics_weights_thresholds = {}
        for metrics, weights_thresholds in self.metrics_weights_thresholds.items():
            if metrics in ["issue_first_reponse", "bug_issue_open_time", "pr_open_time"]:
                weights_thresholds["weight"] = weights_thresholds["weight"] * 0.5
                new_metrics_weights_thresholds[metrics + "avg"] = weights_thresholds
                new_metrics_weights_thresholds[metrics + "min"] = weights_thresholds
            else:
                new_metrics_weights_thresholds[metrics] = weights_thresholds
        if self.algorithm == "AHP":
            score = get_score_ahp(metrics_data, new_metrics_weights_thresholds)
            min_metrics_data = {key: None for key in new_metrics_weights_thresholds.keys()}
            min_score = round(get_score_ahp(min_metrics_data, new_metrics_weights_thresholds), 5)
            return normalize(score, min_score, 1 - min_score)
        else:
            raise Exception("Invalid algorithm param.")

    def metrics_decay(self, metrics_data, last_data):
        if last_data is None:
            return metrics_data

        new_metrics_weights_thresholds = {}
        for metrics, weights_thresholds in self.metrics_weights_thresholds.items():
            if metrics in ["issue_first_reponse", "bug_issue_open_time", "pr_open_time"]:
                weights_thresholds["weight"] = weights_thresholds["weight"] * 0.5
                new_metrics_weights_thresholds[metrics + "avg"] = weights_thresholds
                new_metrics_weights_thresholds[metrics + "min"] = weights_thresholds
            else:
                new_metrics_weights_thresholds[metrics] = weights_thresholds

        decay_metrics_data = metrics_data.copy()
        increment_decay_dict = {}
        decrease_decay_dict = {}
        for metrics, weights_thresholds in new_metrics_weights_thresholds.items():
            threshold = weights_thresholds["threshold"]
            if metrics in INCREMENT_DECAY_METRICS:
                increment_decay_dict[metrics] = threshold
            if metrics in DECREASE_DECAY_METRICS:
                decrease_decay_dict[metrics] = threshold

        for key, value in increment_decay_dict.items():
            if metrics_data[key] is None and last_data.get(key) is not None:
                days = pendulum.parse(metrics_data['grimoire_creation_date']).diff(
                    pendulum.parse(last_data[key][1])).days
                decay_metrics_data[key] = round(increment_decay(last_data[key][0], value, days), 4)
        for key, value in decrease_decay_dict.items():
            if metrics_data[key] is None and last_data.get(key) is not None:
                days = pendulum.parse(metrics_data['grimoire_creation_date']).diff(
                    pendulum.parse(last_data[key][1])).days
                decay_metrics_data[key] = round(decrease_decay(last_data[key][0], value, days), 4)
        return decay_metrics_data