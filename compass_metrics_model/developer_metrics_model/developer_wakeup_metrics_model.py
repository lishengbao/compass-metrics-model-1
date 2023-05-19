import logging
import json
from datetime import timedelta, datetime
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime,
                                          datetime_to_utc)
from compass_metrics_model.metrics_model import (MetricsModel,
                                                 MAX_BULK_UPDATE_SIZE)
from compass_metrics_model.utils import (get_score_ahp,
                                         get_date_list,
                                         get_uuid)
from compass_common.utils.list_utils import list_sub
from compass_common.utils.datetime_utils import (str_to_offset,
                                                 datetime_range)

logger = logging.getLogger(__name__)

MODEL_NAME = "Developer Wakeup"
WEIGHTS_FILE = "developer_metrics_model/resources/developer_wakeup_weights.yaml"


class DeveloperWakeupMetricsModel(MetricsModel):
    def __init__(self, json_file=None, from_date=None, end_date=None, out_index=None, community=None, level=None,
                 weights=None, custom_fields=None, git_index=None, contributors_index=None, issue_index=None):
        """ DeveloperConversionMetricsModel
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
            # "week": {"freq": "W-MON", "offset": "1w"},
            "month": {"freq": "MS", "offset": "1m"},
            # "seasonal": {"freq": "QS-JAN", "offset": "3m"},
            # "year": {"freq": "AS", "offset": "1y"},
        }
        self.period_attrition_contribution_dict = {}

    def get_wakeup_dict(self, from_date, to_date, date_field_contributor_dict,
                        date_field_contributor_last_period_silence_dict, is_bot=False,
                        period="week"):
        """
           Contributors who entered wakeup in this period
        """
        total_wakeup_set = set()
        total_silence_set = set()
        current_activity_total_set = self.get_type_contributor_activity_set(from_date, to_date,
                                                                            date_field_contributor_dict, is_bot)
        last_period_silence_set = set()
        last_period_to_date = to_date + str_to_offset("-1y")
        silence_contributor_date_dict = self.get_contribution_activity_date_dict(date_field_contributor_last_period_silence_dict,
                                                                         is_bot)
        for contributor_name, contribution_date_list in silence_contributor_date_dict.items():
            if len(list_sub(contribution_date_list, (last_period_to_date + str_to_offset("-90d")).isoformat(),
                            last_period_to_date.isoformat())) == 0:
                last_period_silence_set.add(contributor_name)

        if period in ["week", "month", "seasonal", "year"]:
            current_total_conversion_to_silence_set, \
            current_casual_conversion_to_silence_set , \
            current_regular_conversion_to_silence_set , \
            current_core_conversion_to_silence_set = self.get_conversion_to_silence_set(
                from_date, to_date, date_field_contributor_dict, is_bot, period)
            total_silence_set = last_period_silence_set | current_total_conversion_to_silence_set
            total_wakeup_set = total_silence_set & current_activity_total_set
        if period in "year":
            current_conversion_to_silence_set = set()
            current_silence_to_wakeup_set = set()
            last_90_day_from_date_str = (from_date + str_to_offset("-90d")).isoformat()
            last_90_day_to_date_str = (to_date + str_to_offset("-90d")).isoformat()
            contributor_date_dict = self.get_contribution_activity_date_dict(date_field_contributor_dict, is_bot)
            for contributor_name, contribution_date_list in contributor_date_dict.items():
                last_90_day_contribution_date_list = list_sub(contribution_date_list, last_90_day_from_date_str,
                                                              last_90_day_to_date_str)
                if len(last_90_day_contribution_date_list) > 0:
                    date_list_tmp = [last_90_day_from_date_str]
                    date_list_tmp.extend(last_90_day_contribution_date_list)
                    date_list_tmp.append(last_90_day_to_date_str)
                    for i in range(1, len(date_list_tmp)):
                        date1 = str_to_datetime(date_list_tmp[i - 1])
                        date2 = str_to_datetime(date_list_tmp[i])
                        delta = date2 - date1
                        if delta.days > 90:
                            current_conversion_to_silence_set.add(contributor_name)
                            if i < len(date_list_tmp) - 1:
                                current_silence_to_wakeup_set.add(contributor_name)
                            break
            total_silence_set = last_period_silence_set | current_conversion_to_silence_set
            total_wakeup_set = (last_period_silence_set & current_activity_total_set) | current_silence_to_wakeup_set

        total_wakeup_dict = {
            "set": total_wakeup_set,
            "count": len(total_wakeup_set),
            "ratio": round(len(total_wakeup_set) / len(total_silence_set), 4) if len(
                total_silence_set) > 0 else 0
        }
        return total_wakeup_dict

    def get_same_period_wakeup_list(self, from_date, to_date, date_field_contributor_dict,
                                    current_wakeup_total_set,
                                    is_bot=False,
                                    period="week"):
        """
            Contributor who became a casual/regular/core in the same cycle and became a wakeup contributor
            in the current cycle
        """
        wakeup_total_list = []

        current_date_key = from_date.strftime("%Y-%m-%d")
        period_dict = self.period_attrition_contribution_dict.get(period, {})

        current_period_attrition_total_set, \
        current_period_attrition_casual_set, \
        current_period_attrition_regular_set, \
        current_period_attrition_core_set = self.get_conversion_to_silence_set(
            from_date, to_date, date_field_contributor_dict, is_bot, period)

        if len(period_dict) > 0:
            sorted_period_items = sorted(period_dict.items(), key=lambda x: x[0], reverse=True)
            for date_key, date_value in sorted_period_items:
                if len(wakeup_total_list) > 48:
                    break
                end_date = (datetime_to_utc(str_to_datetime(date_key)) + str_to_offset(
                    self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
                period_num = len(get_date_list(date_key, current_date_key, self.period_dict[period]["freq"])) - 1
                # total
                wakeup_total_set = date_value["attrition_total_set"] & current_wakeup_total_set
                wakeup_total_list.append({
                    "start_date": date_key,
                    "end_date": end_date,
                    "count": len(wakeup_total_set),
                    "ratio": round(len(wakeup_total_set) / len(date_value["attrition_total_set"]), 4) if len(
                        date_value["attrition_total_set"]) > 0 else 0,
                    f"{period}_num": period_num
                })

        period_dict[current_date_key] = {
            "attrition_total_set": current_period_attrition_total_set
        }
        self.period_attrition_contribution_dict[period] = period_dict
        current_end_date = (datetime_to_utc(str_to_datetime(current_date_key)) + str_to_offset(
            self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
        # waiting wakeup
        wakeup_total_list.append({
            "start_date": current_date_key,
            "end_date": current_end_date,
            "count": len(current_period_attrition_total_set),
            "ratio": 0,
            f"{period}_num": 0
        })
        wakeup_total_list = sorted(wakeup_total_list, key=lambda x: x["start_date"])
        return wakeup_total_list

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
                created_since = self.created_since(to_date, repos_list)
                if created_since is None:
                    continue

                last_90_day_from_date = datetime_range(date + str_to_offset("-90d"), period_key)[0]
                issue_creation_contributor_list = self.get_contributor_list(last_90_day_from_date,
                                                                            to_date, repos_list,
                                                                            "issue_creation_date_list")
                pr_creation_contributor_list = self.get_contributor_list(last_90_day_from_date, to_date,
                                                                         repos_list,
                                                                         "pr_creation_date_list")
                issue_comments_contributor_list = self.get_contributor_list(last_90_day_from_date,
                                                                            to_date, repos_list,
                                                                            "issue_comments_date_list")
                pr_review_contributor_list = self.get_contributor_list(last_90_day_from_date, to_date,
                                                                       repos_list,
                                                                       "pr_review_date_list")
                code_commit_contributor_list = self.get_contributor_list(last_90_day_from_date, to_date,
                                                                         repos_list,
                                                                         "code_commit_date_list")
                star_contributor_list = self.get_contributor_list(last_90_day_from_date, to_date,
                                                                  repos_list, "star_date_list")
                fork_contributor_list = self.get_contributor_list(last_90_day_from_date, to_date,
                                                                  repos_list, "fork_date_list")
                watch_contributor_list = self.get_contributor_list(last_90_day_from_date, to_date,
                                                                   repos_list, "watch_date_list")

                last_period_to_date = to_date + str_to_offset("-"+period_value["offset"])
                last_period_90_day_to_date = last_period_to_date + str_to_offset("-90d")

                issue_creation_contributor_silence_list = self.get_contributor_silence_list(last_period_90_day_to_date,
                                                                                            last_period_to_date, repos_list,
                                                                                            "issue_creation_date_list",
                                                                                            "first_issue_creation_date")
                pr_creation_contributor_silence_list = self.get_contributor_silence_list(last_period_90_day_to_date,
                                                                                         last_period_to_date,
                                                                                         repos_list,
                                                                                         "pr_creation_date_list",
                                                                                         "first_pr_creation_date")
                issue_comments_contributor_silence_list = self.get_contributor_silence_list(last_period_90_day_to_date,
                                                                                            last_period_to_date, repos_list,
                                                                                            "issue_comments_date_list",
                                                                                            "first_issue_comments_date")
                pr_review_contributor_silence_list = self.get_contributor_silence_list(last_period_90_day_to_date,
                                                                                       last_period_to_date,
                                                                                       repos_list,
                                                                                       "pr_review_date_list",
                                                                                       "first_pr_review_date")
                code_commit_contributor_silence_list = self.get_contributor_silence_list(last_period_90_day_to_date,
                                                                                         last_period_to_date,
                                                                                         repos_list,
                                                                                         "code_commit_date_list",
                                                                                         "first_code_commit_date")
                star_contributor_silence_list = self.get_contributor_silence_list(last_period_90_day_to_date,
                                                                                  last_period_to_date,
                                                                                  repos_list, "star_date_list",
                                                                                  "first_fork_date")
                fork_contributor_silence_list = self.get_contributor_silence_list(last_period_90_day_to_date,
                                                                                  last_period_to_date,
                                                                                  repos_list, "fork_date_list",
                                                                                  "first_star_date")
                watch_contributor_silence_list = self.get_contributor_silence_list(last_period_90_day_to_date,
                                                                                   last_period_to_date,
                                                                                   repos_list, "watch_date_list",
                                                                                   "first_watch_date")

                total_wakeup_dict = self.get_wakeup_dict(date, to_date, {
                    "issue_creation_date_list": issue_creation_contributor_list,
                    "pr_creation_date_list": pr_creation_contributor_list,
                    "issue_comments_date_list": issue_comments_contributor_list,
                    "pr_review_date_list": pr_review_contributor_list,
                    "code_commit_date_list": code_commit_contributor_list,
                    "star_date_list": star_contributor_list,
                    "fork_date_list": fork_contributor_list,
                    "watch_date_list": watch_contributor_list
                }, {
                     "issue_creation_date_list": issue_creation_contributor_silence_list,
                     "pr_creation_date_list": pr_creation_contributor_silence_list,
                     "issue_comments_date_list": issue_comments_contributor_silence_list,
                     "pr_review_date_list": pr_review_contributor_silence_list,
                     "code_commit_date_list": code_commit_contributor_silence_list,
                     "star_date_list": star_contributor_silence_list,
                     "fork_date_list": fork_contributor_silence_list,
                     "watch_date_list": watch_contributor_silence_list
                 }, period=period_key)
                same_period_wakeup_list = self.get_same_period_wakeup_list(date, to_date, {
                    "issue_creation_date_list": issue_creation_contributor_list,
                    "pr_creation_date_list": pr_creation_contributor_list,
                    "issue_comments_date_list": issue_comments_contributor_list,
                    "pr_review_date_list": pr_review_contributor_list,
                    "code_commit_date_list": code_commit_contributor_list,
                    "star_date_list": star_contributor_list,
                    "fork_date_list": fork_contributor_list,
                    "watch_date_list": watch_contributor_list
                }, total_wakeup_dict["set"], period=period_key)
                type_observe_dict = self.get_wakeup_dict(date, to_date, {
                    "star_date_list": star_contributor_list,
                    "fork_date_list": fork_contributor_list,
                    "watch_date_list": watch_contributor_list}, {
                     "star_date_list": star_contributor_silence_list,
                     "fork_date_list": fork_contributor_silence_list,
                     "watch_date_list": watch_contributor_silence_list}, period=period_key)
                type_observe_star_dict = self.get_wakeup_dict(date, to_date, {
                    "star_date_list": star_contributor_list}, {
                    "star_date_list": star_contributor_silence_list}, period=period_key)
                type_observe_fork_dict = self.get_wakeup_dict(date, to_date, {
                    "fork_date_list": fork_contributor_list}, {
                    "fork_date_list": fork_contributor_silence_list}, period=period_key)
                type_observe_watch_dict = self.get_wakeup_dict(date, to_date, {
                    "watch_date_list": watch_contributor_list}, {
                    "watch_date_list": watch_contributor_silence_list}, period=period_key)
                type_issue_dict = self.get_wakeup_dict(date, to_date, {
                    "issue_creation_date_list": issue_creation_contributor_list,
                    "issue_comments_date_list": issue_comments_contributor_list}, {
                    "issue_creation_date_list": issue_creation_contributor_silence_list,
                    "issue_comments_date_list": issue_comments_contributor_silence_list}, period=period_key)
                type_issue_creator_dict = self.get_wakeup_dict(date, to_date, {
                    "issue_creation_date_list": issue_creation_contributor_list}, {
                    "issue_creation_date_list": issue_creation_contributor_silence_list}, period=period_key)
                type_issue_commenter_dict = self.get_wakeup_dict(date, to_date, {
                    "issue_comments_date_list": issue_comments_contributor_list}, {
                    "issue_comments_date_list": issue_comments_contributor_silence_list}, period=period_key)
                type_code_dict = self.get_wakeup_dict(date, to_date, {
                    "pr_creation_date_list": pr_creation_contributor_list,
                    "pr_review_date_list": pr_review_contributor_list,
                    "code_commit_date_list": code_commit_contributor_list}, {
                    "pr_creation_date_list": pr_creation_contributor_silence_list,
                    "pr_review_date_list": pr_review_contributor_silence_list,
                    "code_commit_date_list": code_commit_contributor_silence_list}, period=period_key)
                type_code_author_dict = self.get_wakeup_dict(date, to_date, {
                    "code_commit_date_list": code_commit_contributor_list}, {
                    "code_commit_date_list": code_commit_contributor_silence_list}, period=period_key)
                type_code_pr_creator_dict = self.get_wakeup_dict(date, to_date, {
                    "pr_creation_date_list": pr_creation_contributor_list}, {
                    "pr_creation_date_list": pr_creation_contributor_silence_list}, period=period_key)
                type_code_pr_reviewer_dict = self.get_wakeup_dict(date, to_date, {
                    "pr_review_date_list": pr_review_contributor_list}, {
                    "pr_review_date_list": pr_review_contributor_silence_list}, period=period_key)
                uuid_value = get_uuid(str(date), self.community, level, label, self.model_name, type, period_key,
                                      self.weights_hash, self.custom_fields_hash)
                metrics_data = {
                    'uuid': uuid_value,
                    'level': level,
                    'type': type,
                    'label': label,
                    'model_name': self.model_name,
                    'period': period_key,

                    'freq_wakeup_to_casual_count': total_wakeup_dict["count"],
                    'freq_wakeup_to_casual_ratio': total_wakeup_dict["ratio"],
                    'freq_wakeup_to_casual_same_period': json.dumps(same_period_wakeup_list),

                    'eco_count': None,
                    'eco_ratio': None,
                    'eco_leader_org_count': None,
                    'eco_leader_org_ratio': None,
                    'eco_leader_person_count': None,
                    'eco_leader_person_ratio': None,
                    'eco_participant_org_count': None,
                    'eco_participant_org_ratio': None,
                    'eco_participant_person_count': None,
                    'eco_participant_person_ratio': None,

                    'type_count': total_wakeup_dict["count"],
                    'type_ratio': total_wakeup_dict["ratio"],
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
                # score = self.get_score(metrics_data, level)
                # metrics_data["score"] = score
                item_datas.append(metrics_data)
                if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                    self.es_out.bulk_upload(item_datas, "uuid")
                    item_datas = []
            self.es_out.bulk_upload(item_datas, "uuid")
