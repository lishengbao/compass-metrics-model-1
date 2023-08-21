from perceval.backend import uuid
import json
import yaml
import re
import logging
from datetime import datetime, timedelta
import urllib3
from elasticsearch import helpers
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime,
                                          datetime_to_utc)
from compass_common.utils.uuid_utils import get_uuid
from compass_common.utils.opensearch_client_utils import get_elasticsearch_client
from compass_common.utils.utils import get_all_repo
from compass_common.utils.str_utils import str_is_not_empty

logger = logging.getLogger(__name__)
urllib3.disable_warnings()
page_size = 1000
MAX_BULK_UPDATE_SIZE = 500

exclude_field_list = ["unknown", "-- undefined --"]

def exclude_special_str(str):
    """ Exclude special characters """
    regEx = "[`~!#$%^&*()+=|{}':;',\\[\\]<>/?~！#￥%……&*（）——+|{}【】‘；：”“’\"\"。 ，、？_-]"
    return re.sub(regEx, "",str)

def get_organizations_info(file_path):
    organizations_dict = {}
    organizations_config = json.load(open(file_path))
    for org_name in organizations_config["organizations"].keys():
        for domain in organizations_config["organizations"][org_name]:
            organizations_dict[domain["domain"]] = org_name
    return organizations_dict


def get_identities_info(file_path):
    identities_dict = {}
    identities_config = yaml.safe_load(open(file_path))
    for identities in identities_config:
        for email in identities["email"]:
            enrollments = identities.get("enrollments")
            if enrollments is not None:
                identities_dict[email] = enrollments[0]["organization"]
    return identities_dict

def get_bots_info(file_path):
    bots_config = json.load(open(file_path))
    common = []
    community_dict = {}
    repo_dict = {}
    if bots_config.get("common") and bots_config["common"].get("pattern") and len(bots_config["common"].get("pattern")):
        common = bots_config["common"]["pattern"]
    for community, community_values in bots_config["community"].items():
        if community_values.get("author_name") and len(community_values.get("author_name")) > 0:
            community_dict[community] = community_values["author_name"]
        if community_values.get("repo"):
            for repo, repo_values in community_values["repo"].items():
                if repo_values.get("author_name") and len(repo_values.get("author_name")) > 0:
                    repo_dict[repo] = repo_values["author_name"]

    bots_dict = {
        "common": common,
        "community": community_dict,
        "repo": repo_dict
    }
    return bots_dict

def is_bot_by_author_name(bots_dict, repo, author_name_list):
    for author_name in author_name_list:
        common_list = bots_dict["common"]
        if len(common_list) > 0:
            for common in common_list:
                pattern = f"^{common.replace('*', '.*')}$"
                regex = re.compile(pattern)
                if regex.match(author_name):
                    return True
        community_dict = bots_dict["community"]
        if len(community_dict) > 0:
            for community, community_values in community_dict.items():
                if community in repo and author_name in community_values:
                    return True
        repo_dict = bots_dict["repo"]
        if len(repo_dict) > 0:
            if repo_dict.get(repo) and author_name in repo_dict.get(repo):
                return True
    return False

def list_of_groups(list_info, per_list_len):
    list_of_group = zip(*(iter(list_info),) * per_list_len)
    end_list = [list(i) for i in list_of_group]
    count = len(list_info) % per_list_len
    end_list.append(list_info[-count:]) if count != 0 else end_list
    return end_list


def get_email_prefix_domain(email):
    email_prefix = None
    domain = None
    try:
        email_prefix = email.split("@")[0]
        domain = email.split("@")[1]
    except (IndexError, AttributeError):
        return email_prefix, domain
    return email_prefix, domain


def get_oldest_date(date1, date2):
    return date2 if date1 >= date2 else date1


def get_latest_date(date1, date2):
    return date1 if date1 >= date2 else date2


class ContributorDevOrgRepo:
    def __init__(self, json_file, identities_config_file, organizations_config_file, bots_config_file, issue_index,
                 pr_index, issue_comments_index, pr_comments_index, git_index, contributors_index, from_date, end_date,
                 repo_index, observe_index=None, company=None, event_index=None):
        self.issue_index = issue_index
        self.pr_index = pr_index
        self.issue_comments_index = issue_comments_index
        self.pr_comments_index = pr_comments_index
        self.git_index = git_index
        self.repo_index = repo_index
        self.observe_index = observe_index
        self.contributors_index = contributors_index
        self.from_date = from_date
        self.end_date = end_date
        self.organizations_dict = get_organizations_info(organizations_config_file)
        self.identities_dict = get_identities_info(identities_config_file)
        self.bots_dict = get_bots_info(bots_config_file)
        self.company = None if company or company == 'None' else company
        self.event_index = event_index
        self.client = None
        self.all_repo = get_all_repo(json_file, 'gitee' if 'gitee' in issue_index else 'github')
        self.platform_item_id_dict = {}
        self.platform_item_identity_dict = {}
        self.git_item_id_dict = {}
        self.git_item_identity_dict = {}
        self.date_field_list = []

    def run(self, elastic_url):
        self.client = get_elasticsearch_client(elastic_url)
        exist = self.client.indices.exists(index=self.contributors_index)
        if not exist:
            self.client.indices.create(index=self.contributors_index, body=self.get_contributor_index_mapping())
        for repo in self.all_repo:
            self.processing_data(repo)

    def processing_data(self, repo):
        logger.info(repo + " start")
        start_time = datetime.now()
        self.platform_item_id_dict = {}
        self.platform_item_identity_dict = {}
        self.git_item_id_dict = {}
        self.git_item_identity_dict = {}
        self.date_field_list = []
        platform_index_type_dict = {
            "pr_LabeledEvent": {"index": self.event_index, "date_field": "pr_label_date_list"},
            "pr_ClosedEvent": {"index": self.event_index, "date_field": "pr_close_date_list"},
            "pr_AssignedEvent": {"index": self.event_index, "date_field": "pr_assign_date_list"},
            "issue_LabeledEvent": {"index": self.event_index, "date_field": "issue_label_date_list"},
            "issue": {"index": self.issue_index, "date_field": "issue_creation_date_list"},
            "pr": {"index": self.pr_index, "date_field": "pr_creation_date_list"},
            "issue_comments": {"index": self.issue_comments_index, "date_field": "issue_comments_date_list"},
            "pr_comments": {"index": self.pr_comments_index, "date_field": "pr_comments_date_list"},
            "fork": {"index": self.observe_index, "date_field": "fork_date_list"},
            "star": {"index": self.observe_index, "date_field": "star_date_list"},
            "watch": {"index": self.observe_index, "date_field": "watch_date_list"},
            "issue_ClosedEvent": {"index": self.event_index, "date_field": "issue_close_date_list"},
            "issue_ReopenedEvent": {"index": self.event_index, "date_field": "issue_reopen_date_list"},
            "issue_AssignedEvent": {"index": self.event_index, "date_field": "issue_assign_date_list"},
            "issue_MilestonedEvent": {"index": self.event_index, "date_field": "issue_milestone_date_list"},
            "issue_MarkedAsDuplicateEvent": {"index": self.event_index, "date_field": "issue_mark_as_duplicate_date_list"},
            "issue_TransferredEvent": {"index": self.event_index, "date_field": "issue_transfer_date_list"},
            "issue_LockedEvent": {"index": self.event_index, "date_field": "issue_lock_date_list"},
            "pr_ReopenedEvent": {"index": self.event_index, "date_field": "pr_reopen_date_list"},
            "pr_MilestonedEvent": {"index": self.event_index, "date_field": "pr_milestone_date_list"},
            "pr_MarkedAsDuplicateEvent": {"index": self.event_index, "date_field": "pr_mark_as_duplicate_date_list"},
            "pr_TransferredEvent": {"index": self.event_index, "date_field": "pr_transfer_date_list"},
            "pr_LockedEvent": {"index": self.event_index, "date_field": "pr_lock_date_list"},
            "pr_MergedEvent": {"index": self.event_index, "date_field": "pr_merge_date_list"},
            "pr_PullRequestReview": {"index": self.event_index, "date_field": "pr_review_date_list"},
        }
        for index_key, index_values in platform_index_type_dict.items():
            self.date_field_list.append(index_values["date_field"])
            self.processing_platform_data(index_values["index"], repo, self.from_date, self.end_date, index_values["date_field"], type=index_key)
        if self.git_index is not None:
            self.date_field_list.append("code_commit_date_list")
            self.date_field_list.append("code_direct_commit_date_list")
            self.processing_commit_data(self.git_index, repo, self.from_date, self.end_date)
        if len(self.platform_item_id_dict) == 0 and len(self.git_item_id_dict) == 0:
            logger.info(repo + " finish count:" + str(0) + " " + str(datetime.now() - start_time))
            return

        all_items_dict = self.get_merge_platform_git_contributor_data(repo, self.git_item_id_dict, self.platform_item_id_dict)
        # old_items_dict = self.query_contributors_org_dict(self.contributors_index, repo)
        # all_items_dict, merge_item_id_set = self.get_merge_old_new_contributor_data(old_items_dict, new_items_dict)
        logger.info(repo + "  save data...")
        # if len(merge_item_id_set) > 0:
        #     merge_item_id_list = list_of_groups(list(merge_item_id_set), 100)
        #     for merge_id in merge_item_id_list:
        #         query = self.get_contributors_dsl(repo, "uuid", merge_id)
        #         self.client.delete_by_query(index=self.contributors_index, body=query)
        all_bulk_data = []
        community =repo.split("/")[-2]
        platform_type = repo.split("/")[-3].split(".")[0]
        # leader_set = set(self.get_repo_leader_list(repo))
        # print("leader_set:"+str(len(leader_set)))
        for item in all_items_dict.values():
            contribution_date_field_dict = {}
            for date_field in self.date_field_list:
                contribution_date_list = list(item.get(date_field, []))
                contribution_date_list.sort()
                contribution_first_date = contribution_date_list[0] if len(contribution_date_list) > 0 else None
                contribution_date_field_dict[date_field] = contribution_date_list
                contribution_date_field_dict["first_" + date_field.replace("_list", "")] = contribution_first_date

            org_change_date_list = list(item.get("org_change_date_list", []))
            if len(org_change_date_list) > 0:
                sorted(org_change_date_list, key=lambda x: x["first_date"])
            is_bot = self.is_bot_by_author_name(repo, list(item.get("id_git_author_name_list", []))
                                                + list(item.get("id_platform_login_name_list", []))
                                                + list(item.get("id_platform_author_name_list", [])))

            contributor_data = {
                "_index": self.contributors_index,
                "_id": item.get("uuid"),
                "_source": {
                    "uuid": item.get("uuid"),
                    "id_git_author_name_list": list(item.get("id_git_author_name_list", [])),
                    "id_git_author_email_list": list(item.get("id_git_author_email_list", [])),
                    "id_platform_login_author_name_list": list(item.get("id_platform_login_author_name_list", [])),
                    "id_platform_login_name_list": list(item.get("id_platform_login_name_list", [])),
                    "id_platform_author_name_list": list(item.get("id_platform_author_name_list", [])),
                    "id_platform_author_email_list": list(item.get("id_platform_author_email_list", [])),
                    "id_identity_list": list(item.get("id_identity_list", [])),
                    **contribution_date_field_dict,
                    "last_contributor_date": item["last_contributor_date"],
                    "org_change_date_list": org_change_date_list,
                    "platform_type": platform_type,
                    "domain": org_change_date_list[len(org_change_date_list)-1]["domain"] if len(org_change_date_list) > 0 else None,
                    "org_name": org_change_date_list[len(org_change_date_list)-1]["org_name"] if len(org_change_date_list) > 0 else None,
                    "community": community,
                    "repo_name": repo,
                    "is_bot": is_bot,
                    "update_at_date": datetime_utcnow().isoformat()
                }
            }
            all_bulk_data.append(contributor_data)
            if len(all_bulk_data) > MAX_BULK_UPDATE_SIZE:
                helpers.bulk(client=self.client, actions=all_bulk_data, request_timeout=100)
                all_bulk_data = []
        helpers.bulk(client=self.client, actions=all_bulk_data, request_timeout=100)
        logger.info(repo + " finish count:" + str(len(all_items_dict)) + " " + str(datetime.now() - start_time))

    def processing_platform_data(self, index, repo, from_date, to_date, date_field, type="issue"):
        logger.info(f"{repo} {index}  {type} processing...")
        search_after = []
        count = 0
        start_time = datetime.now()
        while True:
            results = []
            if type == "issue":
                results = self.get_issue_enrich_data(index, repo, from_date, to_date, page_size, search_after)
            elif type == "pr":
                results = self.get_pr_enrich_data(index, repo, from_date, to_date, page_size, search_after)
            elif type == "issue_comments":
                results = self.get_issue_comment_enrich_data(index, repo, from_date, to_date, page_size, search_after)
            elif type == "pr_comments":
                results = self.get_pr_comment_enrich_data(index, repo, from_date, to_date, page_size, search_after)
            elif type in ["fork", "star", "watch"]:
                results = self.get_observe_enrich_data(index, repo, from_date, to_date, page_size, search_after, type)
            elif type in ["issue_LabeledEvent", "issue_ClosedEvent", "issue_ReopenedEvent", "issue_AssignedEvent",
                          "issue_MilestonedEvent", "issue_MarkedAsDuplicateEvent", "issue_TransferredEvent",
                          "issue_LockedEvent"]:
                results = self.get_issue_event_enrich_data(index, repo, from_date, to_date, page_size, search_after, type.replace("issue_", ""))
            elif type in ["pr_LabeledEvent", "pr_ClosedEvent", "pr_ReopenedEvent", "pr_AssignedEvent",
                          "pr_MilestonedEvent", "pr_MarkedAsDuplicateEvent", "pr_TransferredEvent",
                          "pr_LockedEvent", "pr_MergedEvent", "pr_PullRequestReview"]:
                results = self.get_pr_event_enrich_data(index, repo, from_date, to_date, page_size, search_after, type.replace("pr_", ""))

            count = count + len(results)
            if len(results) == 0:
                break
            for result in results:
                search_after = result["sort"]
                source = result["_source"]
                grimoire_creation_date = datetime_to_utc(
                    str_to_datetime(source["grimoire_creation_date"]).replace(tzinfo=None) + timedelta(microseconds=int(source["uuid"], 16) % 100000)).isoformat()
                user_login = source.get("user_login") if source.get("user_login") else source.get("actor_username")
                if user_login is None:
                    continue
                id_identity_list = [
                    user_login,
                    source.get("author_name"),
                    source.get("user_email")
                ]
                id_identity_list = set(
                    [exclude_special_str(x.lower()) for x in id_identity_list if str_is_not_empty(x) and x.lower() not in exclude_field_list and str_is_not_empty(exclude_special_str(x)) ])
                org_change_date_list = []
                if source.get("user_email") is not None:
                    domain = get_email_prefix_domain(source.get("user_email"))[1]
                    if domain is not None:
                        org_name = self.get_org_name_by_email(source.get("user_email"))
                        org_date = {
                            "domain": domain,
                            "org_name": org_name,
                            "first_date": grimoire_creation_date,
                            "last_date": grimoire_creation_date
                        }
                        org_change_date_list.append(org_date)


                login_author_name = (user_login if user_login else "") + " &&& " + (source.get("author_name") if source.get("author_name") else "")
                item = {
                    "uuid": get_uuid(repo, "platform", user_login, source.get("author_name"), source.get("user_email"), grimoire_creation_date),
                    "id_platform_login_author_name_list": set([login_author_name]),
                    "id_platform_login_name_list": set([user_login] if user_login else []),
                    "id_platform_author_name_list": set([source.get("author_name")] if source.get("author_name") else []),
                    "id_platform_author_email_list": set([source.get("user_email")] if source.get("user_email") else []),
                    "id_identity_list": id_identity_list,
                    date_field: {grimoire_creation_date},
                    "last_contributor_date": grimoire_creation_date,
                    "org_change_date_list": org_change_date_list
                }

                old_item_dict = {}
                for identity in id_identity_list:
                    if identity in self.platform_item_identity_dict.keys() and self.platform_item_identity_dict[identity] in self.platform_item_id_dict.keys():
                        old_item = self.platform_item_id_dict.pop(self.platform_item_identity_dict[identity])
                        old_item_dict[old_item["uuid"]] = old_item
                if len(old_item_dict) > 0:
                    item = self.get_merge_old_new_contributor_data(old_item_dict, {item["uuid"]: item})[0][item["uuid"]]

                self.platform_item_id_dict[item["uuid"]] = item
                for identity in item["id_identity_list"]:
                    self.platform_item_identity_dict[identity] = item["uuid"]
        logger.info(f"{repo} {index}  {type} finish count:{str(count)} {str(datetime.now() - start_time)}")
        # logger.info(repo + " " + index + " finish count:" + str(count) + " " + str(datetime.now() - start_time))

    def processing_commit_data(self, index, repo, from_date, to_date):
        logger.info(repo + " " + index + " processing...")
        created_at = self.get_repo_created(repo)
        search_after = []
        count = 0
        start_time = datetime.now()
        while True:
            results = self.get_commit_enrich_data(index, repo, from_date, to_date, page_size, search_after)
            count = count + len(results)
            if len(results) == 0:
                break
            search_after = results[len(results) - 1]["sort"]
            hash_list = [result["_source"]["hash"] for result in results]
            pr_hits = self.get_pr_list_by_commit_hash(repo, hash_list)
            pr_data_dict = {}
            for pr_hit in pr_hits:
                pr_data_dict[pr_hit["_source"]["merge_commit_sha"]] = pr_hit["_source"]
                for pr_commit_hash in pr_hit["_source"]["commits_data"]:
                    pr_data_dict[pr_commit_hash] = pr_hit["_source"]
            for result in results:
                source = result["_source"]
                if source.get("author_name") is None:
                    continue
                grimoire_creation_date = datetime_to_utc(
                    str_to_datetime(source["grimoire_creation_date"]).replace(tzinfo=None) + timedelta(microseconds=int(source["uuid"], 16) % 100000)).isoformat()
                id_identity_list = [
                    source.get("author_name"),
                    source.get("author_email", None)
                ]
                id_identity_list = set(
                    [exclude_special_str(x.lower()) for x in id_identity_list if str_is_not_empty(x) and x.lower() not in exclude_field_list and str_is_not_empty(exclude_special_str(x)) ])
                org_change_date_list = []
                if source.get("author_email") is not None:
                    domain = get_email_prefix_domain(source.get("author_email"))[1]
                    if domain is not None:
                        org_name = self.get_org_name_by_email(source.get("author_email"))
                        org_date = {
                            "domain": domain,
                            "org_name": org_name,
                            "first_date": grimoire_creation_date,
                            "last_date": grimoire_creation_date
                        }
                        org_change_date_list.append(org_date)
                code_direct_commit_date = None
                if grimoire_creation_date >= created_at and source["hash"] not in pr_data_dict \
                        and ((source["committer_name"] in "GitHub" and source["committer_email"] in "noreply@github.com")
                             or (source["committer_name"] == source["author_name"] and source["committer_email"] == source["author_email"])):
                    code_direct_commit_date = grimoire_creation_date

                item = {
                    "uuid": get_uuid(repo, "git", source["author_name"], source.get("author_email"), grimoire_creation_date),
                    "id_git_author_name_list": set([source.get("author_name")] if source.get("author_name") else []),
                    "id_git_author_email_list": set([source.get("author_email")] if source.get("author_email") else []),
                    "id_identity_list": id_identity_list,
                    "code_commit_date_list": {grimoire_creation_date},
                    "code_direct_commit_date_list": {code_direct_commit_date} if code_direct_commit_date else set(),
                    "last_contributor_date": grimoire_creation_date,
                    "org_change_date_list": org_change_date_list
                }

                old_item_dict = {}
                for identity in id_identity_list:
                    if identity in self.git_item_identity_dict.keys() and self.git_item_identity_dict[identity] in self.git_item_id_dict.keys():
                        old_item = self.git_item_id_dict.pop(self.git_item_identity_dict[identity])
                        old_item_dict[old_item["uuid"]] = old_item
                if len(old_item_dict) > 0:
                    item = self.get_merge_old_new_contributor_data(old_item_dict, {item["uuid"]: item})[0][item["uuid"]]

                self.git_item_id_dict[item["uuid"]] = item
                for identity in item["id_identity_list"]:
                    self.git_item_identity_dict[identity] = item["uuid"]
        logger.info(repo + " " + index + " finish count:" + str(count) + " " + str(datetime.now() - start_time))

    def get_merge_org_change_date(self, old_data_list, new_data_list):
        result_data_list = []
        old_data_dict = {}
        for old_data in old_data_list:
            old_key = old_data["domain"]+":"+(old_data["org_name"] if old_data["org_name"] else "")
            old_data_dict[old_key] = old_data
        for new_data in new_data_list:
            new_key = new_data["domain"]+":"+(new_data["org_name"] if new_data["org_name"] else "")
            if new_key in old_data_dict.keys():
                old_data = old_data_dict.pop(new_key)
                data_dict = {
                    "domain": new_data["domain"],
                    "org_name": new_data["org_name"],
                    "first_date": get_oldest_date(new_data["first_date"], old_data["first_date"]),
                    "last_date": get_latest_date(new_data["last_date"], old_data["last_date"])
                }
                result_data_list.append(data_dict)
                continue
            result_data_list.append(new_data)
        if len(old_data_dict) > 0:
            for old_data in old_data_dict.values():
                result_data_list.append(old_data)
        return result_data_list

    def get_merge_platform_git_contributor_data_backup20230723(self, repo, git_data_dict, platform_data_dict):
        search_after = []
        login_author_name_dict = {}
        new_git_data_dict = git_data_dict.copy()
        new_platform_data_dict = {}
        while True:
            # 1: 获取全部PR , 并提取commit hash list
            pr_list = self.get_pr_enrich_data(self.pr_index, repo, self.from_date, self.end_date, page_size, search_after)
            if len(pr_list) == 0:
                break
            search_after = pr_list[len(pr_list) - 1]["sort"]
            commit_hash_list = [commit_hash for pr in pr_list for commit_hash in pr["_source"]["commits_data"]]
            # 2: 根据commit hash list获取对应 commit数据
            git_commit_list = self.get_git_list_by_hash_list(repo, commit_hash_list)
            git_commit_dict = {git_message["_source"]["hash"]: git_message for git_message in git_commit_list}
            # 3: 提取 merge commit hash list ,再获取对应 commit 数据, 最终得到 commit list
            git_squash_rebase_hash_list = [pr["_source"]["merge_commit_sha"] for pr in pr_list if pr["_source"]["commits_data"][0] not in git_commit_dict]
            git_merge_hash_list = [pr["_source"]["merge_commit_sha"] for pr in pr_list if pr["_source"]["commits_data"][0] in git_commit_dict]
            git_squash_rebase_commit_dict = {}
            git_merge_commit_dict = {}
            if len(git_merge_hash_list) > 0:
                git_squash_rebase_commit_list = self.get_git_list_by_hash_list(repo, git_squash_rebase_hash_list)
                git_squash_rebase_commit_dict = {git_message["_source"]["hash"]: git_message for git_message in git_squash_rebase_commit_list}
            if len(git_merge_hash_list) > 0:
                git_merge_commit_list = self.get_git_list_by_hash_list(repo, git_merge_hash_list)
                git_merge_commit_dict = {git_message["_source"]["hash"]: git_message for git_message in git_merge_commit_list}
            # 4: 遍历 PR , 得到login 和 author 对应关系 login_author_mapping
            for pr in pr_list:
                pr_data = pr["_source"]
                merge_login = pr_data["merge_author_login"]
                create_login = pr_data["user_login"]
                if pr_data["commits_data"][0] in git_commit_dict:
                    git_message = git_commit_dict[pr_data["commits_data"][0]]
                    commit_author_name = git_message["_source"]["author_name"]
                    login_author_name_dict[merge_login] = login_author_name_dict.get(merge_login, set()).add(commit_author_name)
                if pr_data["merge_commit_sha"] in git_squash_rebase_commit_dict:
                    git_message = git_squash_rebase_commit_dict[pr_data["merge_commit_sha"]]
                    squash_rebase_commit_author_name = git_message["_source"]["author_name"]
                    login_author_name_dict[merge_login] = login_author_name_dict.get(merge_login, set()).add(
                        squash_rebase_commit_author_name)
                if pr_data["merge_commit_sha"] in git_merge_commit_dict:
                    git_message = git_merge_commit_dict[pr_data["merge_commit_sha"]]
                    merge_commit_author_name = git_message["_source"]["author_name"]
                    login_author_name_dict[create_login] = login_author_name_dict.get(create_login, set()).add(
                        merge_commit_author_name)
        # 5: 将 git_data_dict 转成 git_author_uuid_dict
        git_author_uuid_dict = {author_name: git_data["uuid"] for git_data in git_data_dict.values()
                                for author_name in git_data["id_git_author_name_list"]}
        # 6: 遍历 platform_data_dict , 匹配login_author_mapping 对应 git_author_uuid_dict
        for platform_data in platform_data_dict:
            for platform_login_name in platform_data["id_platform_login_name_list"]:
                if platform_login_name in login_author_name_dict:
                    for author_name in login_author_name_dict[platform_login_name]:
                        git_data = new_git_data_dict.pop(git_author_uuid_dict[author_name], None)
                        if git_data:
                            platform_data = self.get_merge_contributor_data(platform_data, git_data)
            new_platform_data_dict[platform_data["uuid"]] = platform_data

        result_item_dict, merge_id_set = self.get_merge_old_new_contributor_data(new_git_data_dict, new_platform_data_dict)
        for commit_data in new_git_data_dict.values():
            if commit_data["uuid"] in merge_id_set:
                continue
            result_item_dict[commit_data["uuid"]] = commit_data
        return result_item_dict

    def get_merge_platform_git_contributor_data(self, repo, git_data_dict, platform_data_dict):
        new_git_data_dict = git_data_dict.copy()
        new_platform_data_dict = {}
        login_author_name_dict = self.get_platform_login_git_author_dict(repo)

        git_author_uuid_dict = {author_name: git_data["uuid"] for git_data in git_data_dict.values()
                                for author_name in git_data["id_git_author_name_list"]}
        for platform_data in platform_data_dict.values():
            for platform_login_name in platform_data["id_platform_login_name_list"]:
                if platform_login_name in login_author_name_dict:
                    for author_name in login_author_name_dict[platform_login_name]:
                        git_data = new_git_data_dict.pop(git_author_uuid_dict[author_name], None)
                        if git_data:
                            platform_data = self.get_merge_contributor_data(platform_data, git_data)
            new_platform_data_dict[platform_data["uuid"]] = platform_data

        result_item_dict, merge_id_set = self.get_merge_old_new_contributor_data(new_git_data_dict, new_platform_data_dict)
        for commit_data in new_git_data_dict.values():
            if commit_data["uuid"] in merge_id_set:
                continue
            result_item_dict[commit_data["uuid"]] = commit_data
        return result_item_dict

    def get_platform_login_git_author_dict(self, repo):
        created_at = self.get_repo_created(repo)
        search_after = []
        login_author_name_dict = {}
        while True:
            query_dsl = self.get_enrich_dsl("tag", repo + ".git", self.from_date, self.end_date, page_size, search_after)
            query_dsl["query"]["bool"]["filter"].append({"range": {"grimoire_creation_date": {"gte": created_at}}})
            results = self.client.search(index=self.git_index, body=query_dsl)["hits"]["hits"]
            if len(results) == 0:
                break
            search_after = results[len(results) - 1]["sort"]
            hash_list = [result["_source"]["hash"] for result in results]
            hash_set = set(hash_list)
            pr_hits = self.get_pr_list_by_commit_hash(repo, hash_list)
            pr_data_dict = {}
            for pr_hit in pr_hits:
                pr_data_dict[pr_hit["_source"]["merge_commit_sha"]] = pr_hit["_source"]
                for pr_commit_hash in pr_hit["_source"]["commits_data"]:
                    pr_data_dict[pr_commit_hash] = pr_hit["_source"]
            for hit in results:
                data = hit["_source"]
                commit_author_name = data["author_name"]
                commit_committer_name = data["committer_name"]
                if data["hash"] in pr_data_dict:
                    pr_data = pr_data_dict[data["hash"]]
                    merge_login = pr_data["merge_author_login"]
                    create_login = pr_data["user_login"]
                    if len(set(pr_data["commits_data"]) & hash_set) > 0 or len(data["parents"]) > 1:
                        # merge
                        if data["hash"] in pr_data["merge_commit_sha"] and data["committer_name"] in "GitHub" and data["committer_email"] in "noreply@github.com":
                            author_set = login_author_name_dict.get(merge_login, set())
                            author_set.add(commit_author_name)
                            login_author_name_dict[merge_login] = author_set
                        elif data["hash"] in pr_data["commits_data"] and commit_author_name == commit_committer_name:
                            author_set = login_author_name_dict.get(create_login, set())
                            author_set.add(commit_author_name)
                            login_author_name_dict[create_login] = author_set
                    else:
                        if data["committer_name"] in "GitHub" and data["committer_email"] in "noreply@github.com":
                            # squash
                            author_set = login_author_name_dict.get(create_login, set())
                            author_set.add(commit_author_name)
                            login_author_name_dict[create_login] = author_set
                        else:
                            # rebase
                            author_set = login_author_name_dict.get(create_login, set())
                            author_set.add(commit_author_name)
                            login_author_name_dict[create_login] = author_set

                            committer_set = login_author_name_dict.get(merge_login, set())
                            committer_set.add(commit_committer_name)
                            login_author_name_dict[merge_login] = committer_set
        return login_author_name_dict

    def get_git_list_by_hash_list(self, repo, hash_list):
        git_query_dsl = self.get_enrich_dsl("tag", repo + ".git", "1970-01-01", "2099-01-01", page_size, [])
        git_query_dsl["query"]["bool"]["must"].append({"terms": {"hash": hash_list}})
        git_list = self.client.search(index=self.git_index, body=git_query_dsl)["hits"]["hits"]
        return git_list

    def get_merge_old_new_contributor_data(self, old_data_dict, new_data_dict):
        result_item_dict = {}
        identity_dict = {}
        for item in old_data_dict.values():
            for identity in item["id_identity_list"]:
                identity_dict[identity] = item
        result_identity_uuid_dict = {}
        merge_id_set = set()
        for uuid, item in new_data_dict.items():
            old_data_list_dict = {}
            for identity in item["id_identity_list"]:
                if identity in identity_dict.keys():
                    old_data_list_dict[identity_dict[identity]["uuid"]] = identity_dict[identity]

            if len(old_data_list_dict) == 0:
                result_item_dict[uuid] = item
                continue
            for old_data in old_data_list_dict.values():
                if old_data["uuid"] in merge_id_set:
                    for identity in item["id_identity_list"]:
                        if identity in result_identity_uuid_dict.keys() and result_identity_uuid_dict[identity] in result_item_dict.keys():
                            old_data = result_item_dict.pop(result_identity_uuid_dict[identity])
                            break
                else:
                    merge_id_set.add(old_data["uuid"])
                item = self.get_merge_contributor_data(item, old_data)
            result_item_dict[item["uuid"]] = item
            for identity_list in item["id_identity_list"]:
                result_identity_uuid_dict[identity_list] = item["uuid"]
        return result_item_dict, merge_id_set

    def get_merge_contributor_data(self, contributor1, contributor2):
        id_platform_login_name_list = contributor1.get("id_platform_login_name_list", set())
        id_platform_login_author_name_list = contributor1.get("id_platform_login_author_name_list", set())
        id_platform_author_name_list = contributor1.get("id_platform_author_name_list", set())
        id_platform_author_email_list = contributor1.get("id_platform_author_email_list", set())
        id_git_author_name_list = contributor1.get("id_git_author_name_list", set())
        id_git_author_email_list = contributor1.get("id_git_author_email_list", set())
        identity_list = contributor1.get("id_identity_list", set())
        org_change_date_list = contributor1.get("org_change_date_list", [])


        id_platform_login_name_list.update(
            set(contributor2["id_platform_login_name_list"] if contributor2.get("id_platform_login_name_list") else []))
        id_platform_login_author_name_list.update(
            set(contributor2["id_platform_login_author_name_list"] if contributor2.get("id_platform_login_author_name_list") else []))
        id_platform_author_name_list.update(
            set(contributor2["id_platform_author_name_list"] if contributor2.get("id_platform_author_name_list") else []))
        id_platform_author_email_list.update(
            set(contributor2["id_platform_author_email_list"] if contributor2.get("id_platform_author_email_list") else []))
        id_git_author_name_list.update(
            set(contributor2["id_git_author_name_list"] if contributor2.get("id_git_author_name_list") else []))
        id_git_author_email_list.update(
            set(contributor2["id_git_author_email_list"] if contributor2.get("id_git_author_email_list") else []))
        identity_list.update(set(contributor2["id_identity_list"] if contributor2.get("id_identity_list") else []))
        if contributor2.get("org_change_date_list") is not None:
            org_change_date_list = self.get_merge_org_change_date(contributor2.get("org_change_date_list"),
                                                                  org_change_date_list)

        contributor1["id_platform_login_name_list"] = id_platform_login_name_list
        contributor1["id_platform_login_author_name_list"] = id_platform_login_author_name_list
        contributor1["id_platform_author_name_list"] = id_platform_author_name_list
        contributor1["id_platform_author_email_list"] = id_platform_author_email_list
        contributor1["id_git_author_name_list"] = id_git_author_name_list
        contributor1["id_git_author_email_list"] = id_git_author_email_list
        contributor1["id_identity_list"] = identity_list

        for data_field in self.date_field_list:
            contribution_data_list = contributor1.get(data_field, set())
            contribution_data_list.update(
                set(contributor2[data_field] if contributor2.get(data_field) else []))
            contributor1[data_field] = contribution_data_list
        contributor1["last_contributor_date"] = get_latest_date(contributor1["last_contributor_date"],
                                                        contributor2["last_contributor_date"])
        contributor1["org_change_date_list"] = org_change_date_list
        return contributor1

    def get_enrich_dsl(self, repo_field, repo, from_date, to_date, page_size=100, search_after=[]):
        query = {
            "size": page_size,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                repo_field: repo
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "grimoire_creation_date": {
                                    "gte": from_date,
                                    "lte": to_date
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "grimoire_creation_date": {
                        "order": "asc"
                    }
                },
                {
                    "_id": {
                        "order": "asc"
                    }
                }
            ]
        }
        if len(search_after) > 0:
            query['search_after'] = search_after
        return query

    def get_issue_enrich_data(self, index, repo, from_date, to_date, page_size=100, search_after=[]):
        query_dsl = self.get_enrich_dsl("tag", repo, from_date, to_date, page_size, search_after)
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "false"}})
        results = self.client.search(index=index, body=query_dsl)["hits"]["hits"]
        return results

    def get_pr_enrich_data(self, index, repo, from_date, to_date, page_size=100, search_after=[]):
        query_dsl = self.get_enrich_dsl("tag", repo, from_date, to_date, page_size, search_after)
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "true"}})
        results = self.client.search(index=index, body=query_dsl)["hits"]["hits"]
        return results

    def get_issue_comment_enrich_data(self, index, repo, from_date, to_date, page_size=100, search_after=[]):
        query_dsl = self.get_enrich_dsl("tag", repo, from_date, to_date, page_size, search_after)
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"issue_pull_request": "false"}})
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"item_type": "comment"}})
        results = self.client.search(index=index, body=query_dsl)["hits"]["hits"]
        return results

    def get_pr_comment_enrich_data(self, index, repo, from_date, to_date, page_size=100, search_after=[]):
        query_dsl = self.get_enrich_dsl("tag", repo, from_date, to_date, page_size, search_after)
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"item_type": "comment"}})
        results = self.client.search(index=index, body=query_dsl)["hits"]["hits"]
        return results

    def get_observe_enrich_data(self, index, repo, from_date, to_date, page_size=100, search_after=[], type="fork"):
        query_dsl = self.get_enrich_dsl("tag", repo, from_date, to_date, page_size, search_after)
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"type": type}})
        results = self.client.search(index=index, body=query_dsl)["hits"]["hits"]
        return results

    def get_issue_event_enrich_data(self, index, repo, from_date, to_date, page_size=100, search_after=[], type="LabeledEvent"):
        query_dsl = self.get_enrich_dsl("tag", repo, from_date, to_date, page_size, search_after)
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "false"}})
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"event_type": type}})
        if type in ["ClosedEvent", "ReopenedEvent"]:
            query_dsl["query"]["bool"]["must"].append({
                "script": {
                    "script": "doc['actor_username'].size() > 0 && doc['reporter_user_name'].size() > 0 &&  doc['actor_username'].value != doc['reporter_user_name'].value"
                }
            })
        results = self.client.search(index=index, body=query_dsl)["hits"]["hits"]
        return results

    def get_pr_event_enrich_data(self, index, repo, from_date, to_date, page_size=100, search_after=[], type="LabeledEvent"):
        query_dsl = self.get_enrich_dsl("tag", repo, from_date, to_date, page_size, search_after)
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "true"}})
        query_dsl["query"]["bool"]["must"].append({"match_phrase": {"event_type": type}})
        if type in ["ClosedEvent", "ReopenedEvent"]:
            query_dsl["query"]["bool"]["must"].append({
                "script": {
                    "script": "doc['actor_username'].size() > 0 && doc['reporter_user_name'].size() > 0 &&  doc['actor_username'].value != doc['reporter_user_name'].value"
                }
            })
        if type in "PullRequestReview":
            query_dsl["query"]["bool"]["must"].append({
                "terms": {
                    "merge_state": [
                        "APPROVED",
                        "CHANGES_REQUESTED",
                        "DISMISSED"
                    ]
                }
            })
        results = self.client.search(index=index, body=query_dsl)["hits"]["hits"]
        return results


    def get_commit_enrich_data(self, index, repo, from_date, to_date, page_size=100, search_after=[]):
        query_dsl = self.get_enrich_dsl("tag", repo + ".git", from_date, to_date, page_size, search_after)
        results = self.client.search(index=index, body=query_dsl)["hits"]["hits"]
        return results

    def get_contributor_index_mapping(self):
        mapping = {
            "mappings" : {
                "properties" : {
                "code_commit_date_list" : {
                    "type" : "date"
                },
                "community" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "domain" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "first_code_commit_date" : {
                    "type" : "date"
                },
                "first_issue_comments_date" : {
                    "type" : "date"
                },
                "first_issue_creation_date" : {
                    "type" : "date"
                },
                "first_pr_creation_date" : {
                    "type" : "date"
                },
                "first_pr_review_date" : {
                    "type" : "date"
                },
                "id_git_author_email_list" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "id_git_author_name_list" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "id_identity_list" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "id_platform_author_email_list" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "id_platform_author_name_list" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "id_platform_login_name_list" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "issue_comments_date_list" : {
                    "type" : "date"
                },
                "issue_creation_date_list" : {
                    "type" : "date"
                },
                "last_contributor_date" : {
                    "type" : "date"
                },
                "org_change_date_list" : {
                    "properties" : {
                    "domain" : {
                        "type" : "text",
                        "fields" : {
                        "keyword" : {
                            "type" : "keyword",
                            "ignore_above" : 256
                        }
                        }
                    },
                    "first_date" : {
                        "type" : "date"
                    },
                    "last_date" : {
                        "type" : "date"
                    },
                    "org_name" : {
                        "type" : "text",
                        "fields" : {
                        "keyword" : {
                            "type" : "keyword",
                            "ignore_above" : 256
                        }
                        }
                    }
                    }
                },
                "org_name" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "platform_type" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "pr_creation_date_list" : {
                    "type" : "date"
                },
                "pr_review_date_list" : {
                    "type" : "date"
                },
                "repo_name" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                },
                "update_at_date" : {
                    "type" : "date"
                },
                "uuid" : {
                    "type" : "text",
                    "fields" : {
                    "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                    }
                    }
                }
                }
            }
        }
        return mapping

    def get_contributors_dsl(self, repo, field, field_value_list):
        query = {
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                field + ".keyword": field_value_list
                            }
                        },
                        {
                            "match_phrase": {
                                "repo_name.keyword": repo
                            }
                        }
                    ]
                }
            }
        }
        return query

    def query_contributors_org_dict(self, index, repo):
        result_list = []
        all_identity_set = set()
        all_identity_set.update(self.platform_item_identity_dict.keys())
        all_identity_set.update(self.git_item_identity_dict.keys())
        for identity_list in list_of_groups(list(all_identity_set), page_size):
            query = self.get_contributors_dsl(repo, "id_identity_list", identity_list)
            contributors_list = self.client.search(index=index, body=query)["hits"]["hits"]
            if len(contributors_list) > 0:
                result_list = result_list + [contributor["_source"] for contributor in contributors_list]
        return dict(zip([item["uuid"] for item in result_list], result_list))

    def get_org_name_by_email(self, email):
        domain = get_email_prefix_domain(email)[1]
        if domain is None:
            return None
        org_name = self.identities_dict[email] if self.identities_dict.get(email) else self.organizations_dict.get(domain)
        if "facebook.com" in domain:
            org_name = "Facebook"
        if ("noreply.gitee.com" in domain or "noreply.github.com" in domain) and self.company is not None:
            org_name = self.company
        return org_name

    def is_bot_by_author_name(self, repo, author_name_list):
        for author_name in author_name_list:
            common_list = self.bots_dict["common"]
            if len(common_list) > 0:
                for common in common_list:
                    pattern = f"^{common.replace('*', '.*')}$"
                    regex = re.compile(pattern)
                    if regex.match(author_name):
                        return True
            community_dict = self.bots_dict["community"]
            if len(community_dict) > 0:
                for community, community_values in community_dict.items():
                    if community in repo and author_name in community_values:
                        return True
            repo_dict = self.bots_dict["repo"]
            if len(repo_dict) > 0:
                if repo_dict.get(repo) and author_name in repo_dict.get(repo):
                    return True
        return False

    def get_repo_leader_list(self, repo):
        leader_list_by_event = self.get_repo_leader_list_by_event(repo)
        leader_list_by_by_direct_push = self.get_repo_leader_list_by_direct_push(repo)
        return leader_list_by_event | leader_list_by_by_direct_push

    def get_repo_leader_list_by_event(self, repo):
        query = {
            "size": 0,
            "aggs": {
                "terms_actor_username": {
                    "terms": {
                        "field": "actor_username",
                        "size": 10000
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "origin": repo
                            }
                        }
                    ],
                    "should": [
                        {
                            "terms": {
                                "event_type": [
                                    "LabeledEvent",
                                    "UnlabeledEvent",
                                    "MergedEvent",
                                    "AssignedEvent",
                                    "LockedEvent",
                                    "MilestonedEvent",
                                    "MarkedAsDuplicateEvent",
                                    "TransferredEvent"
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                        "terms": {
                                            "event_type": [
                                                "ClosedEvent",
                                                "ReopenedEvent"
                                            ]
                                        }
                                    },
                                    {
                                        "script": {
                                            "script": "doc['actor_username'].size() > 0 && doc['reporter_user_name'].size() > 0 &&  doc['actor_username'].value != doc['reporter_user_name'].value"
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                      "match_phrase": {
                                        "event_type": "PullRequestReview"
                                      }
                                    },
                                    {
                                        "terms": {
                                            "merge_state": [
                                              "APPROVED",
                                              "CHANGES_REQUESTED",
                                              "DISMISSED"
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        buckets = self.client.search(index=self.event_index, body=query)["aggregations"]["terms_actor_username"]["buckets"]
        leader_list = {bucket["key"] for bucket in buckets}
        return leader_list

    def get_repo_leader_list_by_direct_push(self, repo):
        created_at = self.get_repo_created(repo)
        search_after = []
        leader_list = set()
        while True:
            query_dsl = self.get_enrich_dsl("tag", repo + ".git", self.from_date, self.end_date, page_size, search_after)
            query_dsl["query"]["bool"]["filter"].append({"range": {"grimoire_creation_date": {"gte": created_at}}})
            results = self.client.search(index=self.git_index, body=query_dsl)["hits"]["hits"]
            if len(results) == 0:
                break
            search_after = results[len(results) - 1]["sort"]
            hash_list = [result["_source"]["hash"] for result in results]
            pr_hits = self.get_pr_list_by_commit_hash(repo, hash_list)
            pr_data_dict = {}
            for pr_hit in pr_hits:
                pr_data_dict[pr_hit["_source"]["merge_commit_sha"]] = pr_hit["_source"]
                for pr_commit_hash in pr_hit["_source"]["commits_data"]:
                    pr_data_dict[pr_commit_hash] = pr_hit["_source"]
            for result in results:
                data = result["_source"]
                if data["hash"] not in pr_data_dict \
                        and ((data["committer_name"] in "GitHub" and data["committer_email"] in "noreply@github.com")
                             or (data["committer_name"] == data["author_name"] and data["committer_email"] == data["author_email"])):
                    leader_list.add(data["author_name"])
        return leader_list

    def get_repo_created(self, repo):
        repo_query = {
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "origin": repo
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "metadata__enriched_on": {
                        "order": "desc"
                    }
                }
            ]
        }
        hits = self.client.search(index=self.repo_index, body=repo_query)["hits"]["hits"]
        created_at = hits[0]["_source"]["created_at"]
        return created_at.replace("Z", "")

    def get_pr_list_by_commit_hash(self, repo, hash_list):
        pr_query = {
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "origin": repo
                            }
                        },
                        {
                            "match_phrase": {
                                "merged": "true"
                            }
                        }
                    ],
                    "should": [
                        {
                            "terms": {
                                "merge_commit_sha": hash_list
                            }
                        },
                        {
                            "terms": {
                                "commits_data": hash_list
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        pr_hits = self.client.search(index=self.pr_index, body=pr_query)[
            "hits"]["hits"]
        return pr_hits
        
