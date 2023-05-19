import logging
import json
from datetime import timedelta
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime,
                                          datetime_to_utc)
from compass_metrics_model.metrics_model import (MetricsModel,
                                                 MAX_BULK_UPDATE_SIZE)
from compass_metrics_model.utils import (get_score_ahp,
                                         get_date_list,
                                         get_uuid)
from compass_common.utils.datetime_utils import str_to_offset                                         

logger = logging.getLogger(__name__)

MODEL_NAME = "Developer Retention"
WEIGHTS_FILE = "developer_metrics_model/resources/developer_retention_weights.yaml"


class DeveloperRetentionMetricsModel(MetricsModel):
    def __init__(self, json_file=None, from_date=None, end_date=None, out_index=None, community=None, level=None,
                 weights=None, custom_fields=None, git_index=None, contributors_index=None, issue_index=None):
        """ DeveloperAttractionMetricsModel
        Args:
            json_file: the path of json file containing repository message.
            from_date: the beginning of time for Metrics Model.
            end_date: the end of time for Metrics Model
            out_index: target index for Metrics Model.
            community: used to mark the repo belongs to which community.
            level: str representation of the metrics, choose from repo, project, community.
            weights: dict representation of the weights of metrics.
            custom_fields: dict representation of the custom fields of metrics.
            git_index: git enriched index.
            contributors_index: contributors index.
            issue_index: issue index.
        """
        super().__init__(json_file, from_date, end_date, out_index, community, level, weights, custom_fields,
                         WEIGHTS_FILE)
        self.model_name = MODEL_NAME
        self.git_index = git_index
        self.contributors_index = contributors_index
        self.issue_index = issue_index
        self.period_dict = {
            "week": {"freq": "W-MON", "offset": "1w"},
            "month": {"freq": "MS", "offset": "1m"},
            "seasonal": {"freq": "QS-JAN", "offset": "3m"},
            "year": {"freq": "AS", "offset": "1y"},
        }
        self.period_first_contribution_dict = {}

    def get_freq_contributor_retention_dict(self, from_date, to_date, date_field_contributor_dict, is_bot=False,
                                            period="week"):
        def get_last_period_retention_dict():
            """
                Contributors who were active in the previous cycle, the same batch of contributors who are still active
                in this cycle (regardless of role switching)
            """
            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, date_field_contributor_dict, is_bot, period)

            last_period_from_date = from_date - str_to_offset(self.period_dict[period]["offset"])
            last_period_to_date = from_date

            last_period_activity_total_set, \
            last_period_activity_casual_set, \
            last_period_activity_regular_set, \
            last_period_activity_core_set = self.get_freq_contributor_activity_set(
                last_period_from_date, last_period_to_date, date_field_contributor_dict, is_bot, period)
            retention_total_dict = {
                "count": len(current_period_activity_total_set & last_period_activity_total_set),
                "ratio": round(len(current_period_activity_total_set & last_period_activity_total_set) /
                               len(last_period_activity_total_set), 4) if len(last_period_activity_total_set) > 0 else 0
            }
            retention_casual_dict = {
                "count": len(current_period_activity_total_set & last_period_activity_casual_set),
                "ratio": round(len(current_period_activity_total_set & last_period_activity_casual_set) /
                               len(last_period_activity_casual_set), 4) if len(
                    last_period_activity_casual_set) > 0 else 0
            }
            retention_regular_dict = {
                "count": len(current_period_activity_total_set & last_period_activity_regular_set),
                "ratio": round(len(current_period_activity_total_set & last_period_activity_regular_set) /
                               len(last_period_activity_regular_set), 4) if len(
                    last_period_activity_regular_set) > 0 else 0
            }
            retention_core_dict = {
                "count": len(current_period_activity_total_set & last_period_activity_core_set),
                "ratio": round(len(current_period_activity_total_set & last_period_activity_core_set) /
                               len(last_period_activity_core_set), 4) if len(last_period_activity_core_set) > 0 else 0
            }
            return retention_total_dict, retention_casual_dict, retention_regular_dict, retention_core_dict

        def get_same_period_retention_list():
            """
                Cohort contributors who became contributors in the same period and are still active
                in the current cycle (regardless of role switching)
            """
            retention_total_list = []
            retention_casual_list = []
            retention_regular_list = []
            retention_core_list = []
            current_date_key = from_date.strftime("%Y-%m-%d")
            period_dict = self.period_first_contribution_dict.get(period, {})
            if len(period_dict) > 0:
                current_period_activity_total_set, \
                current_period_activity_casual_set, \
                current_period_activity_regular_set, \
                current_period_activity_core_set = self.get_freq_contributor_activity_set(
                    from_date, to_date, date_field_contributor_dict, is_bot, period)
                sorted_period_items = sorted(period_dict.items(), key=lambda x: x[0], reverse=True)
                for date_key, date_value in sorted_period_items:
                    if len(retention_total_list) > 48:
                        break
                    end_date = (datetime_to_utc(str_to_datetime(date_key)) + str_to_offset(
                        self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
                    period_num = len(get_date_list(date_key, current_date_key, self.period_dict[period]["freq"])) - 1
                    retention_total_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(date_value["attraction_total_set"] & current_period_activity_total_set),
                        "ratio": round(
                            len(date_value["attraction_total_set"] & current_period_activity_total_set) / len(
                                date_value[
                                    "attraction_total_set"]), 4) if len(date_value["attraction_total_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    retention_casual_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(date_value["attraction_casual_set"] & current_period_activity_total_set),
                        "ratio": round(
                            len(date_value["attraction_casual_set"] & current_period_activity_total_set) / len(
                                date_value[
                                    "attraction_casual_set"]), 4) if len(
                            date_value["attraction_casual_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    retention_regular_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(date_value["attraction_regular_set"] & current_period_activity_total_set),
                        "ratio": round(
                            len(date_value["attraction_regular_set"] & current_period_activity_total_set) / len(
                                date_value[
                                    "attraction_regular_set"]), 4) if len(
                            date_value["attraction_regular_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    retention_core_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(date_value["attraction_core_set"] & current_period_activity_total_set),
                        "ratio": round(
                            len(date_value["attraction_core_set"] & current_period_activity_total_set) / len(date_value[
                                                                                                                 "attraction_core_set"]),
                            4) if len(date_value["attraction_core_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })

            attraction_total_set, \
            attraction_casual_set, \
            attraction_regular_set, \
            attraction_core_set = self.get_freq_contributor_attraction_set(from_date, to_date,
                                                                           date_field_contributor_dict,
                                                                           is_bot,
                                                                           period)

            period_dict[current_date_key] = {
                "attraction_total_set": attraction_total_set,
                "attraction_casual_set": attraction_casual_set,
                "attraction_regular_set": attraction_regular_set,
                "attraction_core_set": attraction_core_set,
            }
            self.period_first_contribution_dict[period] = period_dict
            current_end_date = (datetime_to_utc(str_to_datetime(current_date_key)) + str_to_offset(
                self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
            retention_total_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(attraction_total_set),
                "ratio": 1 if len(attraction_total_set) > 0 else 0,
                f"{period}_num": 0
            })
            retention_casual_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(attraction_casual_set),
                "ratio": 1 if len(attraction_casual_set) > 0 else 0,
                f"{period}_num": 0
            })
            retention_regular_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(attraction_regular_set),
                "ratio": 1 if len(attraction_regular_set) > 0 else 0,
                f"{period}_num": 0
            })
            retention_core_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(attraction_core_set),
                "ratio": 1 if len(attraction_core_set) > 0 else 0,
                f"{period}_num": 0
            })
            retention_total_list = sorted(retention_total_list, key=lambda x: x["start_date"])
            retention_casual_list = sorted(retention_casual_list, key=lambda x: x["start_date"])
            retention_regular_list = sorted(retention_regular_list, key=lambda x: x["start_date"])
            retention_core_list = sorted(retention_core_list, key=lambda x: x["start_date"])
            return retention_total_list, retention_casual_list, retention_regular_list, retention_core_list

        retention_total_dict, \
        retention_casual_dict, \
        retention_regular_dict, \
        retention_core_dict = get_last_period_retention_dict()
        same_period_retention_total_list, \
        same_period_retention_casual_list, \
        same_period_retention_regular_list, \
        same_period_retention_core_list = get_same_period_retention_list()

        retention_total_dict["same_period"] = json.dumps(same_period_retention_total_list)
        retention_casual_dict["same_period"] = json.dumps(same_period_retention_casual_list)
        retention_regular_dict["same_period"] = json.dumps(same_period_retention_regular_list)
        retention_core_dict["same_period"] = json.dumps(same_period_retention_core_list)
        return retention_total_dict, retention_casual_dict, retention_regular_dict, retention_core_dict

    def get_type_contributor_retention_dict(self, from_date, to_date, date_field_contributor_dict, is_bot=False,
                                            period="week"):
        """Get the number of retention contributors by contribution type"""
        current_period_activity_set = self.get_type_contributor_activity_set(
            from_date, to_date, date_field_contributor_dict, is_bot)

        last_period_from_date = from_date - str_to_offset(self.period_dict[period]["offset"])
        last_period_to_date = from_date
        last_period_activity_set = self.get_type_contributor_activity_set(
            last_period_from_date, last_period_to_date, date_field_contributor_dict, is_bot)
        retention_dict = {
            "count": len(current_period_activity_set & last_period_activity_set),
            "ratio": round(len(current_period_activity_set & last_period_activity_set) /
                           len(last_period_activity_set), 4) if len(last_period_activity_set) > 0 else 0
        }
        return retention_dict

    def get_score(self, item, level="repo"):
        """ Get metrics model score """
        param_dict = {}
        if level == "community" or level == "project":
            param_dict = {
                "freq_casual_count": [self.weights['FREQ_CASUAL_COUNT_WEIGHT'],
                                      self.weights['FREQ_CASUAL_COUNT_MULTIPLE_THRESHOLD']],
                "freq_regular_count": [self.weights['FREQ_REGULAR_COUNT_WEIGHT'],
                                       self.weights['FREQ_REGULAR_COUNT_MULTIPLE_THRESHOLD']],
                "freq_core_count": [self.weights['FREQ_CORE_COUNT_WEIGHT'],
                                    self.weights['FREQ_CORE_COUNT_MULTIPLE_THRESHOLD']],
                "eco_leader_org_count": [self.weights['ECO_LEADER_ORG_COUNT_WEIGHT'],
                                         self.weights['ECO_LEADER_ORG_COUNT_MULTIPLE_THRESHOLD']],
                "eco_leader_person_count": [self.weights['ECO_LEADER_PERSON_COUNT_WEIGHT'],
                                            self.weights['ECO_LEADER_PERSON_COUNT_MULTIPLE_THRESHOLD']],
                "eco_participant_org_count": [self.weights['ECO_PARTICIPANT_ORG_COUNT_WEIGHT'],
                                              self.weights['ECO_PARTICIPANT_ORG_COUNT_MULTIPLE_THRESHOLD']],
                "eco_participant_person_count": [self.weights['ECO_PARTICIPANT_PERSON_COUNT_WEIGHT'],
                                                 self.weights['ECO_PARTICIPANT_PERSON_COUNT_MULTIPLE_THRESHOLD']],
                "type_observe_count": [self.weights['TYPE_OBSERVE_COUNT_WEIGHT'],
                                       self.weights['TYPE_OBSERVE_COUNT_MULTIPLE_THRESHOLD']],
                "type_issue_count": [self.weights['TYPE_ISSUE_COUNT_WEIGHT'],
                                     self.weights['TYPE_ISSUE_COUNT_MULTIPLE_THRESHOLD']],
                "type_code_count": [self.weights['TYPE_CODE_COUNT_WEIGHT'],
                                    self.weights['TYPE_CODE_COUNT_MULTIPLE_THRESHOLD']],
                "type_forum_count": [self.weights['TYPE_FORUM_COUNT_WEIGHT'],
                                     self.weights['TYPE_FORUM_COUNT_MULTIPLE_THRESHOLD']],
                "type_chat_count": [self.weights['TYPE_CHAT_COUNT_WEIGHT'],
                                    self.weights['TYPE_CHAT_COUNT_MULTIPLE_THRESHOLD']],
                "type_media_count": [self.weights['TYPE_MEDIA_COUNT_WEIGHT'],
                                     self.weights['TYPE_MEDIA_COUNT_MULTIPLE_THRESHOLD']],
                "type_event_count": [self.weights['TYPE_EVENT_COUNT_WEIGHT'],
                                     self.weights['TYPE_EVENT_COUNT_MULTIPLE_THRESHOLD']]
            }
        if level == "repo":
            param_dict = {
                "freq_casual_count": [self.weights['FREQ_CASUAL_COUNT_WEIGHT'],
                                      self.weights['FREQ_CASUAL_COUNT_THRESHOLD']],
                "freq_regular_count": [self.weights['FREQ_REGULAR_COUNT_WEIGHT'],
                                       self.weights['FREQ_REGULAR_COUNT_THRESHOLD']],
                "freq_core_count": [self.weights['FREQ_CORE_COUNT_WEIGHT'],
                                    self.weights['FREQ_CORE_COUNT_THRESHOLD']],
                "eco_leader_org_count": [self.weights['ECO_LEADER_ORG_COUNT_WEIGHT'],
                                         self.weights['ECO_LEADER_ORG_COUNT_THRESHOLD']],
                "eco_leader_person_count": [self.weights['ECO_LEADER_PERSON_COUNT_WEIGHT'],
                                            self.weights['ECO_LEADER_PERSON_COUNT_THRESHOLD']],
                "eco_participant_org_count": [self.weights['ECO_PARTICIPANT_ORG_COUNT_WEIGHT'],
                                              self.weights['ECO_PARTICIPANT_ORG_COUNT_THRESHOLD']],
                "eco_participant_person_count": [self.weights['ECO_PARTICIPANT_PERSON_COUNT_WEIGHT'],
                                                 self.weights['ECO_PARTICIPANT_PERSON_COUNT_THRESHOLD']],
                "type_observe_count": [self.weights['TYPE_OBSERVE_COUNT_WEIGHT'],
                                       self.weights['TYPE_OBSERVE_COUNT_THRESHOLD']],
                "type_issue_count": [self.weights['TYPE_ISSUE_COUNT_WEIGHT'],
                                     self.weights['TYPE_ISSUE_COUNT_THRESHOLD']],
                "type_code_count": [self.weights['TYPE_CODE_COUNT_WEIGHT'], self.weights['TYPE_CODE_COUNT_THRESHOLD']],
                "type_forum_count": [self.weights['TYPE_FORUM_COUNT_WEIGHT'],
                                     self.weights['TYPE_FORUM_COUNT_THRESHOLD']],
                "type_chat_count": [self.weights['TYPE_CHAT_COUNT_WEIGHT'], self.weights['TYPE_CHAT_COUNT_THRESHOLD']],
                "type_media_count": [self.weights['TYPE_MEDIA_COUNT_WEIGHT'],
                                     self.weights['TYPE_MEDIA_COUNT_THRESHOLD']],
                "type_event_count": [self.weights['TYPE_EVENT_COUNT_WEIGHT'],
                                     self.weights['TYPE_EVENT_COUNT_THRESHOLD']]
            }
        score = get_score_ahp(item, param_dict)
        return score

    def metrics_model_enrich(self, repos_list, label, type=None, level=None, date_list=None):
        """ Calculate metric model data and save it """
        level = level if level is not None else self.level
        for period_key, period_value in self.period_dict.items():
            item_datas = []
            date_list = get_date_list(self.from_date, self.end_date, period_value["freq"])
            for date in date_list:
                logger.info(f"{str(date)}--{self.model_name}--{label}--{period_key}")
                to_date = date + str_to_offset(period_value["offset"])
                # created_since = self.created_since(to_date, repos_list)
                # if created_since is None:
                #     continue
                last_period_from_date = date - str_to_offset(period_value["offset"])
                issue_creation_contributor_list = self.get_contributor_list(last_period_from_date,
                                                                            to_date, repos_list,
                                                                            "issue_creation_date_list")
                pr_creation_contributor_list = self.get_contributor_list(last_period_from_date, to_date,
                                                                         repos_list,
                                                                         "pr_creation_date_list")
                issue_comments_contributor_list = self.get_contributor_list(last_period_from_date,
                                                                            to_date, repos_list,
                                                                            "issue_comments_date_list")
                pr_review_contributor_list = self.get_contributor_list(last_period_from_date, to_date,
                                                                       repos_list,
                                                                       "pr_review_date_list")
                code_commit_contributor_list = self.get_contributor_list(last_period_from_date, to_date,
                                                                         repos_list,
                                                                         "code_commit_date_list")
                star_contributor_list = self.get_contributor_list(last_period_from_date, to_date,
                                                                  repos_list, "star_date_list")
                fork_contributor_list = self.get_contributor_list(last_period_from_date, to_date,
                                                                  repos_list, "fork_date_list")
                watch_contributor_list = self.get_contributor_list(last_period_from_date, to_date,
                                                                   repos_list, "watch_date_list")

                total_retention_dict, freq_casual_dict, freq_regular_dict, freq_core_dict = self.get_freq_contributor_retention_dict(
                    date,
                    to_date, {
                        "issue_creation_date_list": issue_creation_contributor_list,
                        "pr_creation_date_list": pr_creation_contributor_list,
                        "issue_comments_date_list": issue_comments_contributor_list,
                        "pr_review_date_list": pr_review_contributor_list,
                        "code_commit_date_list": code_commit_contributor_list,
                        "star_date_list": star_contributor_list,
                        "fork_date_list": fork_contributor_list,
                        "watch_date_list": watch_contributor_list
                    }, period=period_key)
                type_observe_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "star_date_list": star_contributor_list,
                    "fork_date_list": fork_contributor_list,
                    "watch_date_list": watch_contributor_list}, period=period_key)
                type_observe_star_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "star_date_list": star_contributor_list}, period=period_key)
                type_observe_fork_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "fork_date_list": fork_contributor_list}, period=period_key)
                type_observe_watch_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "watch_date_list": watch_contributor_list}, period=period_key)
                type_issue_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "issue_creation_date_list": issue_creation_contributor_list,
                    "issue_comments_date_list": issue_comments_contributor_list}, period=period_key)
                type_issue_creator_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "issue_creation_date_list": issue_creation_contributor_list}, period=period_key)
                type_issue_commenter_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "issue_comments_date_list": issue_comments_contributor_list}, period=period_key)
                type_code_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "pr_creation_date_list": pr_creation_contributor_list,
                    "pr_review_date_list": pr_review_contributor_list,
                    "code_commit_date_list": code_commit_contributor_list}, period=period_key)
                type_code_author_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "code_commit_date_list": code_commit_contributor_list}, period=period_key)
                type_code_pr_creator_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "pr_creation_date_list": pr_creation_contributor_list}, period=period_key)
                type_code_pr_reviewer_dict = self.get_type_contributor_retention_dict(date, to_date, {
                    "pr_review_date_list": pr_review_contributor_list}, period=period_key)
                uuid_value = get_uuid(str(date), self.community, level, label, self.model_name, type, period_key,
                                      self.weights_hash, self.custom_fields_hash)
                metrics_data = {
                    'uuid': uuid_value,
                    'level': level,
                    'type': type,
                    'label': label,
                    'model_name': self.model_name,
                    'period': period_key,
                    'freq_count': total_retention_dict["count"],
                    'freq_ratio': total_retention_dict["ratio"],
                    'freq_same_period': total_retention_dict["same_period"],
                    'freq_casual_count': freq_casual_dict["count"],
                    'freq_casual_ratio': freq_casual_dict["ratio"],
                    'freq_casual_same_period': freq_casual_dict["same_period"],
                    'freq_regular_count': freq_regular_dict["count"],
                    'freq_regular_ratio': freq_regular_dict["ratio"],
                    'freq_regular_same_period': freq_regular_dict["same_period"],
                    'freq_core_count': freq_core_dict["count"],
                    'freq_core_ratio': freq_core_dict["ratio"],
                    'freq_core_same_period': freq_core_dict["same_period"],
                    'eco_count': total_retention_dict["count"],
                    'eco_ratio': total_retention_dict["ratio"],
                    'eco_leader_org_count': None,
                    'eco_leader_org_ratio': None,
                    'eco_leader_person_count': None,
                    'eco_leader_person_ratio': None,
                    'eco_participant_org_count': None,
                    'eco_participant_org_ratio': None,
                    'eco_participant_person_count': None,
                    'eco_participant_person_ratio': None,
                    'type_count': total_retention_dict["count"],
                    'type_ratio': total_retention_dict["ratio"],
                    'type_observe_count': type_observe_dict["count"],
                    'type_observe_ratio': type_observe_dict["ratio"],
                    'type_observe_star_count': type_observe_star_dict["count"],
                    'type_observe_star_ratio': type_observe_star_dict["ratio"],
                    'type_observe_fork_count': type_observe_fork_dict["count"],
                    'type_observe_fork_ratio': type_observe_fork_dict["ratio"],
                    'type_observe_watch_count': type_observe_watch_dict["count"],
                    'type_observe_watch_ratio': type_observe_watch_dict["ratio"],
                    'type_issue_count': type_issue_dict["count"],
                    'type_issue_ratio': type_issue_dict["ratio"],
                    'type_issue_creator_count': type_issue_creator_dict["count"],
                    'type_issue_creator_ratio': type_issue_creator_dict["ratio"],
                    'type_issue_commenter_count': type_issue_commenter_dict["count"],
                    'type_issue_commenter_ratio': type_issue_commenter_dict["ratio"],
                    'type_code_count': type_code_dict["count"],
                    'type_code_ratio': type_code_dict["ratio"],
                    'type_code_author_count': type_code_author_dict["count"],
                    'type_code_author_ratio': type_code_author_dict["ratio"],
                    'type_code_pr_creator_count': type_code_pr_creator_dict["count"],
                    'type_code_pr_creator_ratio': type_code_pr_creator_dict["ratio"],
                    'type_code_pr_reviewer_count': type_code_pr_reviewer_dict["count"],
                    'type_code_pr_reviewer_ratio': type_code_pr_reviewer_dict["ratio"],
                    'type_forum_count': None,
                    'type_forum_ratio': None,
                    'type_chat_count': None,
                    'type_chat_ratio': None,
                    'type_media_count': None,
                    'type_media_ratio': None,
                    'type_event_count': None,
                    'type_event_ratio': None,
                    'grimoire_creation_date': date.isoformat(),
                    'metadata__enriched_on': datetime_utcnow().isoformat(),
                    **self.custom_fields
                }
                score = self.get_score(metrics_data, level)
                metrics_data["score"] = score
                item_datas.append(metrics_data)
                if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                    self.es_out.bulk_upload(item_datas, "uuid")
                    item_datas = []
            self.es_out.bulk_upload(item_datas, "uuid")
