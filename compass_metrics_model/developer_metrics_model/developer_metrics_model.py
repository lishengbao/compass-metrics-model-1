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

MODEL_NAME = "Developer"


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
            "list": [],
            "from_casual": {
                "count": len(retention_casual_to_casual),
                "list": []
            },
            "from_regular": {
                "count": len(retention_regular_to_casual),
                "list": sorted(list(retention_regular_to_casual))
            },
            "from_core": {
                "count": len(retention_core_to_casual),
                "list": sorted(list(retention_core_to_casual))
            },
            "from_silence": {
                "count": len(silence_to_casual),
                "list": []
            },
            "from_attraction_casual": {
                "count": len(attraction_casual_to_casual),
                "list": []
            },
            "from_attraction_regular": {
                "count": len(attraction_regular_to_casual),
                "list": sorted(list(attraction_regular_to_casual))
            },
            "from_attraction_core": {
                "count": len(attraction_core_to_casual),
                "list": sorted(list(attraction_core_to_casual))
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
            "list": sorted(list(retention_regular_set)),
            "from_casual": {
                "count": len(retention_casual_to_regular),
                "list": sorted(list(retention_casual_to_regular))
            },
            "from_regular": {
                "count": len(retention_regular_to_regular),
                "list": sorted(list(retention_regular_to_regular))
            },
            "from_core": {
                "count": len(retention_core_to_regular),
                "list": sorted(list(retention_core_to_regular))
            },
            "from_silence": {
                "count": len(silence_to_regular),
                "list": sorted(list(silence_to_regular))
            },
            "from_attraction_casual": {
                "count": len(attraction_casual_to_regular),
                "list": sorted(list(attraction_casual_to_regular))
            },
            "from_attraction_regular": {
                "count": len(attraction_regular_to_regular),
                "list": sorted(list(attraction_regular_to_regular))
            },
            "from_attraction_core": {
                "count": len(attraction_core_to_regular),
                "list": sorted(list(attraction_core_to_regular))
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
            "list": sorted(list(retention_core_set)),
            "from_casual": {
                "count": len(retention_casual_to_core),
                "list": sorted(list(retention_casual_to_core))
            },
            "from_regular": {
                "count": len(retention_regular_to_core),
                "list": sorted(list(retention_regular_to_core))
            },
            "from_core": {
                "count": len(retention_core_to_core),
                "list": sorted(list(retention_core_to_core))
            },
            "from_silence": {
                "count": len(silence_to_core),
                "list": sorted(list(silence_to_core))
            },
            "from_attraction_casual": {
                "count": len(attraction_casual_to_core),
                "list": sorted(list(attraction_casual_to_core))
            },
            "from_attraction_regular": {
                "count": len(attraction_regular_to_core),
                "list": sorted(list(attraction_regular_to_core))
            },
            "from_attraction_core": {
                "count": len(attraction_core_to_core),
                "list": sorted(list(attraction_core_to_core))
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
        casual_to_silence = last_contributor_dict.get("attraction_casual_set", set()) & silence_set
        # 9.6 吸引常客转静默 = 历史吸引常客 & 静默
        regular_to_silence = last_contributor_dict.get("attraction_regular_set", set()) & silence_set
        # 9.7 吸引核心转静默 = 历史吸引核心 & 静默
        core_to_silence = last_contributor_dict.get("attraction_core_set", set()) & silence_set
        silence_dict = {
            "count": len(silence_set),
            "list": [],
            "from_casual": {
                "count": len(casual_to_silence),
                "list": []
            },
            "from_regular": {
                "count": len(regular_to_silence),
                "list": sorted(list(regular_to_silence))
            },
            "from_core": {
                "count": len(core_to_silence),
                "list": sorted(list(core_to_silence))
            },
            "from_silence": {
                "count": len(silence_to_silence),
                "list": []
            },
            "from_attraction_casual": {
                "count": len(casual_to_silence),
                "list": []
            },
            "from_attraction_regular": {
                "count": len(regular_to_silence),
                "list": sorted(list(regular_to_silence))
            },
            "from_attraction_core": {
                "count": len(core_to_silence),
                "list": sorted(list(core_to_silence))
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
                "list": []
            },
            "attraction_regular": {
                "count": len(attraction_regular_set),
                "list": sorted(list(attraction_regular_set))
            },
            "attraction_core": {
                "count": len(attraction_core_set),
                "list": sorted(list(attraction_core_set))
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

                contributor_state = self.get_contributor_state(
                    date, to_date, {
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
