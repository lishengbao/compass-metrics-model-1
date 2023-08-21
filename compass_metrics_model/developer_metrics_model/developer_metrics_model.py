import logging
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime,
                                          datetime_to_utc)
from compass_metrics_model.metrics_model import (MetricsModel,
                                                 MAX_BULK_UPDATE_SIZE,
                                                 check_times_has_overlap)
from compass_metrics_model.utils import (get_score_ahp,
                                         get_date_list,
                                         get_uuid)
from compass_common.utils.datetime_utils import str_to_offset
from compass_common.utils.list_utils import list_sub
from compass_common.utils.datetime_utils import (str_to_offset,
                                                 datetime_range)
import json                                                

logger = logging.getLogger(__name__)

MODEL_NAME = "Developer"

observe_date_field = ["star_date_list", "fork_date_list", "watch_date_list"]
issue_date_field = ["issue_creation_date_list", "issue_comments_date_list"]
code_date_field = ["pr_creation_date_list", "pr_comments_date_list", "code_commit_date_list"]
issue_admin_date_field = ["issue_label_date_list","issue_close_date_list","issue_reopen_date_list",
                            "issue_assign_date_list","issue_milestone_date_list","issue_mark_as_duplicate_date_list",
                            "issue_transfer_date_list","issue_lock_date_list"]
code_admin_date_field = ["pr_label_date_list", "pr_close_date_list", "pr_reopen_date_list", "pr_assign_date_list", 
                            "pr_milestone_date_list", "pr_mark_as_duplicate_date_list", "pr_transfer_date_list",
                            "pr_lock_date_list", "pr_merge_date_list", "pr_review_date_list", "code_direct_commit_date_list"]


def get_contributor_type(contributor_name_set, eco_contributor_dict, type_contributor_dict):
    result_list = []
    for contributor_name in contributor_name_set:
        result = {
            "name": contributor_name,
            "eco": eco_contributor_dict.get(contributor_name, None),
            "type": type_contributor_dict.get(contributor_name, [])
        }
        result_list.append(result)
    return json.dumps(result_list)

class DeveloperMetricsModel(MetricsModel):
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
        self.period_last_contributor_dict = {}

    def get_eco_contributor_dict(self, from_date, to_date, date_field_contributor_dict, is_bot=False):

        def get_first_contribution_date(contributor, date_field_list):
            """ Get first contribution time for contributor """
            date_list = []
            for date_field in date_field_list:
                contribution_date_list = contributor.get(date_field)
                if contribution_date_list:
                    date_list.append(contribution_date_list[0])
            return min(date_list) if len(date_list) > 0 else None

        from_date = from_date.isoformat()
        to_date = to_date.isoformat()
        eco_contributor_dict = {}
        date_field_list = ["issue_label_date_list",
            "issue_close_date_list",
            "issue_reopen_date_list",
            "issue_assign_date_list",
            "issue_milestone_date_list",
            "issue_mark_as_duplicate_date_list",
            "issue_transfer_date_list",
            "issue_lock_date_list",
            "pr_label_date_list",
            "pr_close_date_list",
            "pr_reopen_date_list",
            "pr_assign_date_list",
            "pr_milestone_date_list",
            "pr_mark_as_duplicate_date_list",
            "pr_transfer_date_list",
            "pr_lock_date_list",
            "pr_merge_date_list",
            "pr_review_date_list"]
        contributor_list = [item for sublist in date_field_contributor_dict.values() for item in sublist]
        for contributor in list({item["uuid"]: item for item in contributor_list}.values()):
            if (is_bot is None or contributor["is_bot"] == is_bot):
                is_leader = False
                min_contribution_date = get_first_contribution_date(contributor, date_field_list)
                if min_contribution_date and from_date >= min_contribution_date:
                    is_leader = True
                is_org = False
                org_name = ""
                for org in contributor["org_change_date_list"]:
                    if check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                        if org.get("org_name") is not None:
                            is_org = True
                            org_name = org.get("org_name")
                        # else:
                        #     org_name = org.get("domain")
                        break
                contributor_name = None
                if contributor.get("id_platform_login_name_list") and len(
                        contributor.get("id_platform_login_name_list")) > 0:
                    contributor_name = contributor["id_platform_login_name_list"][0]
                elif contributor.get("id_git_author_name_list") and len(contributor.get("id_git_author_name_list")) > 0:
                    contributor_name = contributor["id_git_author_name_list"][0]

                if is_leader and is_org:
                    eco_contributor_dict[contributor_name] = {
                        "eco_name": "eco_leader_org",
                        "org_name": org_name
                    }
                elif is_leader and not is_org:
                    eco_contributor_dict[contributor_name] = {
                        "eco_name": "eco_leader_person",
                        "org_name": org_name
                    }
                elif not is_leader and is_org:
                    eco_contributor_dict[contributor_name] = {
                        "eco_name": "eco_participant_org",
                        "org_name": org_name
                    }
                elif not is_leader and not is_org:
                    eco_contributor_dict[contributor_name] = {
                        "eco_name": "eco_participant_person",
                        "org_name": org_name
                    }
        return eco_contributor_dict

    def get_type_contributor_dict(self, from_date, to_date, date_field_contributor_dict, is_bot=False):
        from_date_str = from_date.isoformat()
        to_date_str = to_date.isoformat()
        type_contributor_dict = {}


        observe_contributor_uuid_dict = {}
        issue_contributor_uuid_dict = {}
        code_contributor_uuid_dict = {}
        issue_admin_contributor_uuid_dict = {}
        code_admin_contributor_uuid_dict= {}
        for date_field in observe_date_field:
            observe_contributor_uuid_dict[date_field] = {contributor["uuid"] for contributor in date_field_contributor_dict[date_field]}
        for date_field in issue_date_field:
            issue_contributor_uuid_dict[date_field] = {contributor["uuid"] for contributor in date_field_contributor_dict[date_field]}
        for date_field in code_date_field:
            code_contributor_uuid_dict[date_field] = {contributor["uuid"] for contributor in date_field_contributor_dict[date_field]}
        for date_field in issue_admin_date_field:
            issue_admin_contributor_uuid_dict[date_field] = {contributor["uuid"] for contributor in date_field_contributor_dict[date_field]}
        for date_field in code_admin_date_field:
            code_admin_contributor_uuid_dict[date_field] = {contributor["uuid"] for contributor in date_field_contributor_dict[date_field]}
            
        type_contributor_uuid_dict = {
            "observe": observe_contributor_uuid_dict,
            "issue": issue_contributor_uuid_dict,
            "code": code_contributor_uuid_dict,
            "issue_admin": issue_admin_contributor_uuid_dict,
            "code_admin": code_admin_contributor_uuid_dict,
        }

        contributor_list = [item for sublist in date_field_contributor_dict.values() for item in sublist]
        for contributor in list({item["uuid"]: item for item in contributor_list}.values()):
            if (is_bot is None or contributor["is_bot"] == is_bot):
                contributor_name = None
                type_list = []
                if contributor.get("id_platform_login_name_list") and len(
                        contributor.get("id_platform_login_name_list")) > 0:
                    contributor_name = contributor["id_platform_login_name_list"][0]
                elif contributor.get("id_git_author_name_list") and len(contributor.get("id_git_author_name_list")) > 0:
                    contributor_name = contributor["id_git_author_name_list"][0]

                for type, contributor_uuid_dict in type_contributor_uuid_dict.items():
                    for date_field, contributor_uuid_set in contributor_uuid_dict.items():
                        if contributor["uuid"] in contributor_uuid_set:
                            date_field_replace = date_field.replace("_date_list", "")
                            if type == "code":
                                date_field_replace = date_field_replace.replace("code_", "")
                            if type in ["issue_admin", "issue"]:
                                date_field_replace = date_field_replace.replace("issue_", "")
                            if type == "code_admin":
                                date_field_replace = date_field_replace.replace("pr_", "")
                            type_name = type + "_" + date_field_replace
                            type_list.append({
                                "type_name": type_name,
                                "contribution_count": len(list_sub(contributor[date_field], from_date_str, to_date_str))
                            })

                type_contributor_dict[contributor_name] = type_list
        return type_contributor_dict


    def get_contributor_attraction_retention_set(self, from_date, to_date, date_field_contributor_dict, is_bot=False):
        from_date_str = from_date.isoformat()
        to_date_str = to_date.isoformat()
        attraction_set = set()
        retention_set = set()
        contributor_date_dict = self.get_contribution_activity_date_dict(date_field_contributor_dict, is_bot)
        for contributor_name, date_list in contributor_date_dict.items():
            if len(list_sub(date_list, from_date_str, to_date_str)) > 0:
                if from_date_str <= min(date_list) <= to_date_str:
                    attraction_set.add(contributor_name)
                else:
                    retention_set.add(contributor_name)
        return attraction_set, retention_set


    def get_contributor_state(self, from_date, to_date, date_field_contributor_dict,
                                            date_field_contributor_silence_dict, is_bot=False,
                                            period="year"):
        last_contributor_dict = self.period_last_contributor_dict.get(period, {})
        # 1:获取留存用户, 吸引用户, 静默用户
        attraction_set, retention_set = self.get_contributor_attraction_retention_set(
            from_date, to_date, date_field_contributor_dict, is_bot)
        silence_set = self.get_contributor_silence_set(
            from_date, to_date, date_field_contributor_silence_dict, is_bot)

        # 2:根据活跃用户划分出访客, 常客, 核心用户
        activity_total_set, \
        activity_casual_set, \
        activity_regular_set, \
        activity_core_set = self.get_freq_contributor_activity_set(from_date, to_date, date_field_contributor_dict,
                                                                   is_bot,
                                                                   period)
        eco_contributor_activity_dict = self.get_eco_contributor_dict(from_date, to_date, date_field_contributor_dict, is_bot)
        eco_contributor_silence_dict = self.get_eco_contributor_dict(from_date, to_date, date_field_contributor_silence_dict, is_bot)
        eco_contributor_dict = {**eco_contributor_activity_dict, **eco_contributor_silence_dict}
        type_contributor_dict = self.get_type_contributor_dict(from_date, to_date, date_field_contributor_dict, is_bot)

        # 3:吸引访客 = 吸引用户 & 访客
        attraction_casual_set = attraction_set & activity_casual_set
        # 4:吸引常客 = 吸引用户 & 常客
        attraction_regular_set = attraction_set & activity_regular_set
        # 5:吸引核心 = 吸引用户 & 核心
        attraction_core_set = attraction_set & activity_core_set
        # 6:留存访客 = 留存用户 & 访客
        retention_casual_set = retention_set & activity_casual_set
        # 6.1 留存访客转访客 = 历史访客 & 留存访客
        retention_casual_to_casual = last_contributor_dict.get("retention_casual_set", set()) & retention_casual_set
        # 6.2 留存常客转访客 = 历史常客 & 留存访客
        retention_regular_to_casual = last_contributor_dict.get("retention_regular_set", set()) & retention_casual_set
        # 6.3 留存核心转访客 = 历史核心 & 留存访客
        retention_core_to_casual = last_contributor_dict.get("retention_core_set", set()) & retention_casual_set
        # 6.4 静默转访客 = 历史静默 & 留存访客
        silence_to_casual = last_contributor_dict.get("silence_set", set()) & retention_casual_set
        # 6.5 留存吸引访客转访客 = 历史吸引访客 & 留存访客
        attraction_casual_to_casual = last_contributor_dict.get("attraction_casual_set", set()) & retention_casual_set
        # 6.6 留存吸引常客转访客 = 历史吸引常客 & 留存访客
        attraction_regular_to_casual = last_contributor_dict.get("attraction_regular_set", set()) & retention_casual_set
        # 6.7 留存吸引核心转访客 = 历史吸引核心 & 留存访客
        attraction_core_to_casual = last_contributor_dict.get("attraction_core_set", set()) & retention_casual_set
        retention_casual_dict = {
            "count": len(retention_casual_set),
            "list": "",
            "from_casual": {
                "count": len(retention_casual_to_casual),
                "list": ""
            },
            "from_regular": {
                "count": len(retention_regular_to_casual),
                "list": get_contributor_type(retention_regular_to_casual, eco_contributor_dict, type_contributor_dict)
            },
            "from_core": {
                "count": len(retention_core_to_casual),
                "list": get_contributor_type(retention_core_to_casual, eco_contributor_dict, type_contributor_dict)
            },
            "from_silence": {
                "count": len(silence_to_casual),
                "list": ""
            },
            "from_attraction_casual": {
                "count": len(attraction_casual_to_casual),
                "list": ""
            },
            "from_attraction_regular": {
                "count": len(attraction_regular_to_casual),
                "list": get_contributor_type(attraction_regular_to_casual, eco_contributor_dict, type_contributor_dict)
            },
            "from_attraction_core": {
                "count": len(attraction_core_to_casual),
                "list": get_contributor_type(attraction_core_to_casual, eco_contributor_dict, type_contributor_dict)
            }
        }

        # 7:留存常客 = 留存用户 & 常客
        retention_regular_set = retention_set & activity_regular_set
        # 7.1 留存访客转常客 = 历史访客 & 留存常客
        retention_casual_to_regular = last_contributor_dict.get("retention_casual_set", set()) & retention_regular_set
        # 7.2 留存常客转常客 = 历史常客 & 留存常客
        retention_regular_to_regular = last_contributor_dict.get("retention_regular_set", set()) & retention_regular_set
        # 7.3 留存核心转常客 = 历史核心 & 留存常客
        retention_core_to_regular = last_contributor_dict.get("retention_core_set", set()) & retention_regular_set
        # 7.4 静默转常客 = 历史静默 & 留存常客
        silence_to_regular = last_contributor_dict.get("silence_set", set()) & retention_regular_set
        # 7.5 留存吸引访客转常客 = 历史吸引访客 & 留存常客
        attraction_casual_to_regular = last_contributor_dict.get("attraction_casual_set", set()) & retention_regular_set
        # 7.6 留存吸引常客转常客 = 历史吸引常客 & 留存常客
        attraction_regular_to_regular = last_contributor_dict.get("attraction_regular_set",
                                                                  set()) & retention_regular_set
        # 7.7 留存吸引核心转常客 = 历史吸引核心 & 留存常客
        attraction_core_to_regular = last_contributor_dict.get("attraction_core_set", set()) & retention_regular_set
        retention_regular_dict = {
            "count": len(retention_regular_set),
            "list": get_contributor_type(retention_regular_set, eco_contributor_dict, type_contributor_dict),
            "from_casual": {
                "count": len(retention_casual_to_regular),
                "list": get_contributor_type(retention_casual_to_regular, eco_contributor_dict, type_contributor_dict)
            },
            "from_regular": {
                "count": len(retention_regular_to_regular),
                "list": get_contributor_type(retention_regular_to_regular, eco_contributor_dict, type_contributor_dict)
            },
            "from_core": {
                "count": len(retention_core_to_regular),
                "list": get_contributor_type(retention_core_to_regular, eco_contributor_dict, type_contributor_dict)
            },
            "from_silence": {
                "count": len(silence_to_regular),
                "list": get_contributor_type(silence_to_regular, eco_contributor_dict, type_contributor_dict)
            },
            "from_attraction_casual": {
                "count": len(attraction_casual_to_regular),
                "list": get_contributor_type(attraction_casual_to_regular, eco_contributor_dict, type_contributor_dict)
            },
            "from_attraction_regular": {
                "count": len(attraction_regular_to_regular),
                "list": get_contributor_type(attraction_regular_to_regular, eco_contributor_dict, type_contributor_dict)
            },
            "from_attraction_core": {
                "count": len(attraction_core_to_regular),
                "list": get_contributor_type(attraction_core_to_regular, eco_contributor_dict, type_contributor_dict)
            }
        }

        # 8:留存核心 = 留存用户 & 核心
        retention_core_set = retention_set & activity_core_set
        # 8.1 留存访客转核心 = 历史访客 & 留存核心
        retention_casual_to_core = last_contributor_dict.get("retention_casual_set", set()) & retention_core_set
        # 8.2 留存常客转核心 = 历史常客 & 留存核心
        retention_regular_to_core = last_contributor_dict.get("retention_regular_set", set()) & retention_core_set
        # 8.3 留存核心转核心 = 历史核心 & 留存核心
        retention_core_to_core = last_contributor_dict.get("retention_core_set", set()) & retention_core_set
        # 8.4 静默转核心 = 历史静默 & 留存核心
        silence_to_core = last_contributor_dict.get("silence_set", set()) & retention_core_set
        # 8.5 留存吸引访客转核心 = 历史吸引访客 & 留存核心
        attraction_casual_to_core = last_contributor_dict.get("attraction_casual_set", set()) & retention_core_set
        # 8.6 留存吸引常客转核心 = 历史吸引常客 & 留存核心
        attraction_regular_to_core = last_contributor_dict.get("attraction_regular_set", set()) & retention_core_set
        # 8.8 留存吸引核心转核心 = 历史吸引核心 & 留存核心
        attraction_core_to_core = last_contributor_dict.get("attraction_core_set", set()) & retention_core_set
        retention_core_dict = {
            "count": len(retention_core_set),
            "list": get_contributor_type(retention_core_set, eco_contributor_dict, type_contributor_dict),
            "from_casual": {
                "count": len(retention_casual_to_core),
                "list": get_contributor_type(retention_casual_to_core, eco_contributor_dict, type_contributor_dict)
            },
            "from_regular": {
                "count": len(retention_regular_to_core),
                "list": get_contributor_type(retention_regular_to_core, eco_contributor_dict, type_contributor_dict)
            },
            "from_core": {
                "count": len(retention_core_to_core),
                "list": get_contributor_type(retention_core_to_core, eco_contributor_dict, type_contributor_dict)
            },
            "from_silence": {
                "count": len(silence_to_core),
                "list": get_contributor_type(silence_to_core, eco_contributor_dict, type_contributor_dict)
            },
            "from_attraction_casual": {
                "count": len(attraction_casual_to_core),
                "list": get_contributor_type(attraction_casual_to_core, eco_contributor_dict, type_contributor_dict)
            },
            "from_attraction_regular": {
                "count": len(attraction_regular_to_core),
                "list": get_contributor_type(attraction_regular_to_core, eco_contributor_dict, type_contributor_dict)
            },
            "from_attraction_core": {
                "count": len(attraction_core_to_core),
                "list": get_contributor_type(attraction_core_to_core, eco_contributor_dict, type_contributor_dict)
            }
        }

        # 9: 静默
        # 9.1 访客转静默 = 历史访客 & 静默
        casual_to_silence = last_contributor_dict.get("retention_casual_set", set()) & silence_set
        # 9.2 常客转静默 = 历史常客 & 静默
        regular_to_silence = last_contributor_dict.get("retention_regular_set", set()) & silence_set
        # 9.3 核心转静默 = 历史核心 & 静默
        core_to_silence = last_contributor_dict.get("retention_core_set", set()) & silence_set
        # 9.4 静默转静默 = 历史静默 & 静默
        silence_to_silence = last_contributor_dict.get("silence_set", set()) & silence_set
        # 9.5 吸引访客转静默 = 历史吸引访客 & 静默
        attraction_casual_to_silence = last_contributor_dict.get("attraction_casual_set", set()) & silence_set
        # 9.6 吸引常客转静默 = 历史吸引常客 & 静默
        attraction_regular_to_silence = last_contributor_dict.get("attraction_regular_set", set()) & silence_set
        # 9.7 吸引核心转静默 = 历史吸引核心 & 静默
        attraction_core_to_silence = last_contributor_dict.get("attraction_core_set", set()) & silence_set
        silence_dict = {
            "count": len(silence_set),
            "list": "",
            "from_casual": {
                "count": len(casual_to_silence),
                "list": ""
            },
            "from_regular": {
                "count": len(regular_to_silence),
                "list": get_contributor_type(regular_to_silence, eco_contributor_dict, type_contributor_dict)
            },
            "from_core": {
                "count": len(core_to_silence),
                "list": get_contributor_type(core_to_silence, eco_contributor_dict, type_contributor_dict)
            },
            "from_silence": {
                "count": len(silence_to_silence),
                "list": ""
            },
            "from_attraction_casual": {
                "count": len(attraction_casual_to_silence),
                "list": ""
            },
            "from_attraction_regular": {
                "count": len(attraction_regular_to_silence),
                "list": get_contributor_type(attraction_regular_to_silence, eco_contributor_dict, type_contributor_dict)
            },
            "from_attraction_core": {
                "count": len(attraction_core_to_silence),
                "list": get_contributor_type(attraction_core_to_silence, eco_contributor_dict, type_contributor_dict)
            }
        }

        # 9:保存当前周期数据
        current_contributor_dict = {
            "retention_casual_set": retention_casual_set,
            "retention_regular_set": retention_regular_set,
            "retention_core_set": retention_core_set,
            "silence_set": silence_set,
            "attraction_casual_set": attraction_casual_set,
            "attraction_regular_set": attraction_regular_set,
            "attraction_core_set": attraction_core_set
        }
        self.period_last_contributor_dict[period] = current_contributor_dict

        result_dict = {
            "retention_casual": retention_casual_dict,
            "retention_regular": retention_regular_dict,
            "retention_core": retention_core_dict,
            "silence": silence_dict,
            "attraction_casual": {
                "count": len(attraction_casual_set),
                "list": ""
            },
            "attraction_regular": {
                "count": len(attraction_regular_set),
                "list": get_contributor_type(attraction_regular_set, eco_contributor_dict, type_contributor_dict)
            },
            "attraction_core": {
                "count": len(attraction_core_set),
                "list": get_contributor_type(attraction_core_set, eco_contributor_dict, type_contributor_dict)
            }
        }
        return result_dict

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

                all_date_field = observe_date_field + issue_date_field + code_date_field + issue_admin_date_field + code_admin_date_field
                date_field_contributor_dict = {}
                date_field_contributor_silence_dict = {}

                last_90_day_to_date = to_date + str_to_offset("-90d")
                for date_field in all_date_field:
                    date_field_contributor_dict[date_field] = self.get_contributor_list(date, to_date, repos_list, date_field)
                    first_date_field = "first_" + date_field.replace("_list", "")
                    date_field_contributor_silence_dict[date_field] = self.get_contributor_silence_list(last_90_day_to_date,
                                                                                            to_date, repos_list,
                                                                                            date_field,
                                                                                            first_date_field)

                contributor_state = self.get_contributor_state(
                    date, to_date, date_field_contributor_dict, date_field_contributor_silence_dict, period=period_key)

                uuid_value = get_uuid(str(date), self.community, level, label, self.model_name, type, period_key,
                                      self.weights_hash, self.custom_fields_hash)
                metrics_data = {
                    'uuid': uuid_value,
                    'level': level,
                    'type': type,
                    'label': label,
                    'model_name': self.model_name,
                    'period': period_key,
                    'retention_casual': contributor_state["retention_casual"],
                    'retention_regular': contributor_state["retention_regular"],
                    'retention_core': contributor_state["retention_core"],
                    'silence': contributor_state["silence"],
                    'attraction_casual': contributor_state["attraction_casual"],
                    'attraction_regular': contributor_state["attraction_regular"],
                    'attraction_core': contributor_state["attraction_core"],
                    'grimoire_creation_date': date.isoformat(),
                    'metadata__enriched_on': datetime_utcnow().isoformat(),
                    **self.custom_fields
                }
                # score = self.get_score(metrics_data, level)
                # metrics_data["score"] = score
                item_datas.append(metrics_data)
                if len(item_datas) > 0:
                    self.es_out.bulk_upload(item_datas, "uuid")
                    item_datas = []
            self.es_out.bulk_upload(item_datas, "uuid")
