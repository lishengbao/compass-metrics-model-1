import logging
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime,
                                          datetime_to_utc)
from compass_metrics_model.metrics_model import (MetricsModel,
                                                 MAX_BULK_UPDATE_SIZE)
from compass_metrics_model.utils import (get_score_ahp,
                                         get_date_list,
                                         get_uuid)
from compass_common.utils.datetime_utils import str_to_offset
from compass_common.utils.list_utils import list_sub
from compass_common.utils.datetime_utils import (str_to_offset,
                                                 datetime_range)

logger = logging.getLogger(__name__)

MODEL_NAME = "Developer Person"


class DeveloperPersonMetricsModel(MetricsModel):
    def __init__(self, json_file=None, from_date=None, end_date=None, out_index=None, community=None, level=None,
                 weights=None, custom_fields=None, git_index=None, contributors_index=None, issue_index=None):
        """ DeveloperPersonMetricsModel
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
        super().__init__(json_file, from_date, end_date, out_index, community, level, weights, custom_fields)
        self.model_name = MODEL_NAME
        self.git_index = git_index
        self.contributors_index = contributors_index
        self.issue_index = issue_index
        self.period_dict = {
            # "week": {"freq": "W-MON", "offset": "1w"},
            # "month": {"freq": "MS", "offset": "1m"},
            # "seasonal": {"freq": "QS-JAN", "offset": "3m"},
            "year": {"freq": "AS", "offset": "1y"},
        }
        self.contributor_track_dict = {}
        self.contributor_info_dict = {}

    def get_contributor_person_info(self, date_field_contributor_dict, is_bot=False):
        contributor_list = [item for sublist in date_field_contributor_dict.values() for item in sublist]
        for contributor in list({item["uuid"]: item for item in contributor_list}.values()):
            if is_bot is None or contributor["is_bot"] == is_bot:
                contributor_name = None
                if contributor.get("id_platform_login_name_list") and len(
                        contributor.get("id_platform_login_name_list")) > 0:
                    contributor_name = contributor["id_platform_login_name_list"][0]
                elif contributor.get("id_git_author_name_list") and len(
                        contributor.get("id_git_author_name_list")) > 0:
                    contributor_name = contributor["id_git_author_name_list"][0]
                # if contributor_name in ["vscodebot[bot]", "alexbarten", "kiranshila", "michaelvanstraten", "xiangpengzhao", "Alexendoo", "jnicklas" ,"Box-Of-Hats"]:
                #     continue
                contributor_info = self.contributor_info_dict.get(contributor_name, {})
                org_info = contributor["org_change_date_list"]
                if contributor_info.get("org_info", None):
                    org_info.append(contributor_info.get("org_info", None))
                contributor_info["org_info"] = max(org_info, key=lambda x: x["last_date"]) if org_info else {}
                self.contributor_info_dict[contributor_name] = contributor_info


    def get_contributor_person_state(self, from_date, to_date, date_field_contributor_dict,
                                            date_field_contributor_silence_dict, is_bot=False,
                                            period="year"):

        silence_set = self.get_contributor_silence_set(from_date, to_date, date_field_contributor_silence_dict, is_bot)
        activity_total_set, \
        activity_casual_set, \
        activity_regular_set, \
        activity_core_set = self.get_freq_contributor_activity_set(from_date, to_date, date_field_contributor_dict,
                                                                   is_bot,
                                                                   period)

        casual_dict = {key: "casual" for key in activity_casual_set}
        regular_dict = {key: "regular" for key in activity_regular_set}
        core_dict = {key: "core" for key in activity_core_set}
        silence_dict = {key: "silence" for key in silence_set}

        total_dict = {**casual_dict, **regular_dict, **core_dict, **silence_dict}
        current_date = from_date.strftime("%Y-%m-%d")
        for key, value in total_dict.items():
            contributor = self.contributor_track_dict.get(key, {})
            mileage_list = contributor.get(f"{period}_mileage_list", [])
            mileage_list.append({
                "date": current_date,
                "state": value
            })
            contributor["name"] = key
            contributor[f"{period}_mileage_list"] = mileage_list
            self.contributor_track_dict[key] = contributor

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
                issue_creation_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                            "issue_creation_date_list")
                pr_creation_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                         "pr_creation_date_list")
                issue_comments_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                            "issue_comments_date_list")
                pr_review_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                       "pr_review_date_list")
                code_commit_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                         "code_commit_date_list")
                star_contributor_list = self.get_contributor_list(date, to_date, repos_list, "star_date_list")
                fork_contributor_list = self.get_contributor_list(date, to_date, repos_list, "fork_date_list")
                watch_contributor_list = self.get_contributor_list(date, to_date, repos_list, "watch_date_list")

                last_90_day_to_date = to_date + str_to_offset("-90d")
                issue_creation_contributor_silence_list = self.get_contributor_silence_list(last_90_day_to_date,
                                                                                            to_date, repos_list,
                                                                                            "issue_creation_date_list",
                                                                                            "first_issue_creation_date")
                pr_creation_contributor_silence_list = self.get_contributor_silence_list(last_90_day_to_date, to_date,
                                                                                         repos_list,
                                                                                         "pr_creation_date_list",
                                                                                         "first_pr_creation_date")
                issue_comments_contributor_silence_list = self.get_contributor_silence_list(last_90_day_to_date,
                                                                                            to_date, repos_list,
                                                                                            "issue_comments_date_list",
                                                                                            "first_issue_comments_date")
                pr_review_contributor_silence_list = self.get_contributor_silence_list(last_90_day_to_date, to_date,
                                                                                       repos_list,
                                                                                       "pr_review_date_list",
                                                                                       "first_pr_review_date")
                code_commit_contributor_silence_list = self.get_contributor_silence_list(last_90_day_to_date, to_date,
                                                                                         repos_list,
                                                                                         "code_commit_date_list",
                                                                                         "first_code_commit_date")
                star_contributor_silence_list = self.get_contributor_silence_list(last_90_day_to_date, to_date,
                                                                                  repos_list, "star_date_list",
                                                                                  "first_fork_date")
                fork_contributor_silence_list = self.get_contributor_silence_list(last_90_day_to_date, to_date,
                                                                                  repos_list, "fork_date_list",
                                                                                  "first_star_date")
                watch_contributor_silence_list = self.get_contributor_silence_list(last_90_day_to_date, to_date,
                                                                                   repos_list, "watch_date_list",
                                                                                   "first_watch_date")
                date_field_contributor_dict = {
                    "issue_creation_date_list": issue_creation_contributor_list,
                    "pr_creation_date_list": pr_creation_contributor_list,
                    "issue_comments_date_list": issue_comments_contributor_list,
                    "pr_review_date_list": pr_review_contributor_list,
                    "code_commit_date_list": code_commit_contributor_list,
                    "star_date_list": star_contributor_list,
                    "fork_date_list": fork_contributor_list,
                    "watch_date_list": watch_contributor_list
                }
                self.get_contributor_person_state(
                    date, to_date, date_field_contributor_dict, {
                        "issue_creation_date_list": issue_creation_contributor_silence_list,
                        "pr_creation_date_list": pr_creation_contributor_silence_list,
                        "issue_comments_date_list": issue_comments_contributor_silence_list,
                        "pr_review_date_list": pr_review_contributor_silence_list,
                        "code_commit_date_list": code_commit_contributor_silence_list,
                        "star_date_list": star_contributor_silence_list,
                        "fork_date_list": fork_contributor_silence_list,
                        "watch_date_list": watch_contributor_silence_list
                    }, period=period_key)

                self.get_contributor_person_info(date_field_contributor_dict)

        for key, value in self.contributor_track_dict.items():
            uuid_value = get_uuid(key, self.community, level, label, self.model_name, type,
                                  self.weights_hash, self.custom_fields_hash)
            metrics_data = {
                'uuid': uuid_value,
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'org_name': self.contributor_info_dict.get(key, {}).get("org_info", {}).get("org_name"),
                **value,
                'metadata__enriched_on': datetime_utcnow().isoformat(),
                **self.custom_fields
            }
            item_datas.append(metrics_data)
            if len(item_datas) > 10000:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")
