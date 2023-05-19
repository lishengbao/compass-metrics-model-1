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
                                         str_to_offset,
                                         get_uuid)

logger = logging.getLogger(__name__)

MODEL_NAME = "Developer Conversion"
WEIGHTS_FILE = "developer_milestone_model/resources/developer_conversion_weights.yaml"


class DeveloperConversionMetricsModel(MetricsModel):
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
            # "month": {"freq": "MS", "offset": "1m"},
            # "seasonal": {"freq": "QS-JAN", "offset": "3m"},
            "year": {"freq": "AS", "offset": "1y"},
        }
        self.up_period_activity_contribution_dict = {}
        self.up_period_contribution_last_type_dict = {}
        self.down_period_activity_contribution_dict = {}
        self.down_period_contribution_last_type_dict = {}
        self.period_type_contribution_dict = {}

    def get_freq_contributor_conversion_up_dict(self, from_date, to_date, activity_contributor_date_dict,
                                                observe_activity_contributor_date_dict,
                                                period="week"):

        def get_last_period_conversion_up_dict():
            """
                Last cycle was a casual and converted to a regular/core contributor or
                last cycle was a regular and converted to a core contributor this cycle
            """
            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, activity_contributor_date_dict, observe_activity_contributor_date_dict, period)

            last_period_from_date = from_date - str_to_offset(self.period_dict[period]["offset"])
            last_period_to_date = from_date

            last_period_activity_total_set, \
            last_period_activity_casual_set, \
            last_period_activity_regular_set, \
            last_period_activity_core_set = self.get_freq_contributor_activity_set(
                last_period_from_date, last_period_to_date, activity_contributor_date_dict,
                observe_activity_contributor_date_dict, period)
            
            # casual --> regular
            casual_up_regular_set = current_period_activity_regular_set & last_period_activity_casual_set
            casual_up_regular_dict = {
                "count": len(casual_up_regular_set),
                "ratio": round(len(casual_up_regular_set) / len(last_period_activity_casual_set), 4) if len(
                    last_period_activity_casual_set) > 0 else 0
            }
            # casual --> core
            casual_up_core_set = current_period_activity_core_set & last_period_activity_casual_set
            casual_up_core_dict = {
                "count": len(casual_up_core_set),
                "ratio": round(len(casual_up_core_set) / len(last_period_activity_casual_set), 4) if len(
                    last_period_activity_casual_set) > 0 else 0
            }
            # casual --> regular + core
            casual_up_regular_core_set = casual_up_regular_set | casual_up_core_set
            casual_up_regular_core_dict = {
                "count": len(casual_up_regular_core_set),
                "ratio": round(len(casual_up_regular_core_set) / len(last_period_activity_casual_set), 4) if len(
                    last_period_activity_casual_set) > 0 else 0
            }
            # regular --> core
            regular_up_core_set = current_period_activity_core_set & last_period_activity_regular_set
            regular_up_core_dict = {
                "count": len(regular_up_core_set),
                "ratio": round(len(regular_up_core_set) / len(last_period_activity_regular_set), 4) if len(
                    last_period_activity_regular_set) > 0 else 0
            }
            # casual + regular --> core
            casual_regular_up_core_set = casual_up_core_set | regular_up_core_set
            last_period_activity_casual_regular_set = (
                    last_period_activity_casual_set | last_period_activity_regular_set)
            casual_regular_up_core_dict = {
                "count": len(casual_regular_up_core_set),
                "ratio": round(len(casual_regular_up_core_set) / len(last_period_activity_casual_regular_set), 4) if len(
                    last_period_activity_casual_regular_set) > 0 else 0
            }
            # total
            total_up_set = casual_up_regular_core_set | casual_regular_up_core_set
            total_up_dict = {
                "count": len(total_up_set),
                "ratio": round(len(total_up_set) /
                               len(last_period_activity_casual_regular_set), 4) if len(
                    last_period_activity_casual_regular_set) > 0 else 0
            }

            result_dict = {
                "casual_up_regular_dict": casual_up_regular_dict,
                "casual_up_core_dict": casual_up_core_dict,
                "casual_up_regular_core_dict": casual_up_regular_core_dict,
                "regular_up_core_dict": regular_up_core_dict,
                "casual_regular_up_core_dict": casual_regular_up_core_dict,
                "total_up_dict": total_up_dict,
            }
            return result_dict

        def get_same_period_conversion_up_dict():
            """
                Contributor who became a casual in the same cycle and became a regular in the current cycle or
                Contributor who became a casual/regular in the same cycle and became a core contributor in the current cycle
            """
            
            casual_up_regular_list = []
            casual_up_core_list = []
            casual_up_regular_core_list = []
            regular_up_core_list = []
            casual_regular_up_core_list = []
            total_up_list = []
            
            current_date_key = from_date.strftime("%Y-%m-%d")
            period_dict = self.up_period_activity_contribution_dict.get(period, {})

            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, activity_contributor_date_dict, observe_activity_contributor_date_dict, period)

            if len(period_dict) > 0:
                sorted_period_items = sorted(period_dict.items(), key=lambda x: x[0], reverse=True)
                for date_key, date_value in sorted_period_items:
                    if len(total_up_list) > 48:
                        break
                    end_date = (datetime_to_utc(str_to_datetime(date_key)) + str_to_offset(
                        self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
                    period_num = len(get_date_list(date_key, current_date_key, self.period_dict[period]["freq"])) - 1
                    # casual --> regular
                    casual_up_regular_set = date_value["activity_casual_set"] & current_period_activity_regular_set
                    casual_up_regular_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(casual_up_regular_set),
                        "ratio": round(len(casual_up_regular_set) / len(date_value[
                                                                                "activity_casual_set"]), 4) if len(
                            date_value["activity_casual_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # casual --> core
                    casual_up_core_set = date_value["activity_casual_set"] & current_period_activity_core_set
                    casual_up_core_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(casual_up_core_set),
                        "ratio": round(len(casual_up_core_set) / len(date_value[
                                                                                "activity_casual_set"]), 4) if len(
                            date_value["activity_casual_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # casual --> regular + core
                    casual_up_regular_core_set = casual_up_regular_set | casual_up_core_set
                    casual_up_regular_core_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(casual_up_regular_core_set),
                        "ratio": round(len(casual_up_regular_core_set) / len(date_value[
                                                                                "activity_casual_set"]), 4) if len(
                            date_value["activity_casual_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # regular --> core
                    regular_up_core_set = date_value["activity_regular_set"] & current_period_activity_core_set
                    regular_up_core_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(regular_up_core_set),
                        "ratio": round(len(regular_up_core_set) / len(date_value[
                                                                                "activity_regular_set"]), 4) if len(
                            date_value["activity_regular_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # casual + regular --> core
                    casual_regular_up_core_set = casual_up_core_set | regular_up_core_set
                    last_period_activity_casual_regular_set = date_value["activity_casual_set"] | date_value[
                        "activity_regular_set"]
                    casual_regular_up_core_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(casual_regular_up_core_set),
                        "ratio": round(len(casual_regular_up_core_set) / len(last_period_activity_casual_regular_set), 4) if len(
                            last_period_activity_casual_regular_set) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # total
                    total_up_set = casual_up_regular_core_set | casual_regular_up_core_set
                    last_period_activity_casual_regular_set = date_value["activity_casual_set"] | date_value[
                        "activity_regular_set"]
                    total_up_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(total_up_set),
                        "ratio": round(len(total_up_set) / len(last_period_activity_casual_regular_set), 4) if len(
                            last_period_activity_casual_regular_set) > 0 else 0,
                        f"{period}_num": period_num
                    })

            period_dict[current_date_key] = {
                "activity_total_set": current_period_activity_total_set,
                "activity_casual_set": current_period_activity_casual_set,
                "activity_regular_set": current_period_activity_regular_set,
                "activity_core_set": current_period_activity_core_set,
            }
            self.up_period_activity_contribution_dict[period] = period_dict
            current_end_date = (datetime_to_utc(str_to_datetime(current_date_key)) + str_to_offset(
                self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
            # waiting casual --> regular
            casual_up_regular_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_casual_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting casual --> core
            casual_up_core_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_casual_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting casual --> regular + core
            casual_up_regular_core_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_casual_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting regular --> core
            regular_up_core_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_regular_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting casual + regular --> core
            casual_regular_up_core_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_casual_set | current_period_activity_regular_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting total
            total_up_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_casual_set | current_period_activity_regular_set),
                "ratio": 0,
                f"{period}_num": 0
            })

            result_dict = {
                "casual_up_regular_list": sorted(casual_up_regular_list, key=lambda x: x["start_date"]),
                "casual_up_core_list": sorted(casual_up_core_list, key=lambda x: x["start_date"]),
                "casual_up_regular_core_list": sorted(casual_up_regular_core_list, key=lambda x: x["start_date"]),
                "regular_up_core_list": sorted(regular_up_core_list, key=lambda x: x["start_date"]),
                "casual_regular_up_core_list": sorted(casual_regular_up_core_list, key=lambda x: x["start_date"]),
                "total_up_list": sorted(total_up_list, key=lambda x: x["start_date"]),
            }
            return result_dict

        def get_before_period_conversion_up_dict():
            """
                A casual before this cycle, converted to a regular contributor this cycle. Or
                a casual/regular before this cycle, converted to a core contributor this cycle
            """
            current_date_key = from_date.strftime("%Y-%m-%d")
            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, activity_contributor_date_dict, observe_activity_contributor_date_dict, period)
            period_dict = self.up_period_contribution_last_type_dict.get(period, {})
            
            before_period_casual_set = {k for k, v in period_dict.items() if v['type'] == "casual"}
            before_period_regular_set = {k for k, v in period_dict.items() if v['type'] == "regular"}
            before_period_casual_regular_set = before_period_casual_set | before_period_regular_set
            # casual --> regular
            casual_up_regular_set = current_period_activity_regular_set & before_period_casual_set
            casual_up_regular_dict = {
                "count": len(casual_up_regular_set),
                "ratio": round(len(casual_up_regular_set) / len(before_period_casual_set), 4) if len(
                    before_period_casual_set) > 0 else 0
            }
            # casual --> core
            casual_up_core_set = current_period_activity_core_set & before_period_casual_set
            casual_up_core_dict = {
                "count": len(casual_up_core_set),
                "ratio": round(len(casual_up_core_set) / len(before_period_casual_set), 4) if len(
                    before_period_casual_set) > 0 else 0
            }
            # casual --> regular + core
            casual_up_regular_core_set = casual_up_regular_set | casual_up_core_set
            casual_up_regular_core_dict = {
                "count": len(casual_up_regular_core_set),
                "ratio": round(len(casual_up_regular_core_set) / len(before_period_casual_set), 4) if len(
                    before_period_casual_set) > 0 else 0
            }
            # regular --> core
            regular_up_core_set = current_period_activity_core_set & before_period_regular_set
            regular_up_core_dict = {
                "count": len(regular_up_core_set),
                "ratio": round(len(regular_up_core_set) / len(before_period_regular_set), 4) if len(
                    before_period_regular_set) > 0 else 0
            }
            # casual + regular --> core
            casual_regular_up_core_set = casual_up_core_set | regular_up_core_set
            casual_regular_up_core_dict = {
                "count": len(casual_regular_up_core_set),
                "ratio": round(len(casual_regular_up_core_set) / len(before_period_casual_regular_set), 4) if len(
                    before_period_casual_regular_set) > 0 else 0
            }
            # total
            total_up_set = casual_up_regular_core_set | casual_regular_up_core_set
            total_up_dict = {
                "count": len(total_up_set),
                "ratio": round(len(total_up_set) / len(before_period_casual_regular_set), 4) if len(
                    before_period_casual_regular_set) > 0 else 0
            }

            period_dict.update(
                {item: {"type": "casual", "date": current_date_key} for item in current_period_activity_casual_set})
            period_dict.update(
                {item: {"type": "regular", "date": current_date_key} for item in current_period_activity_regular_set})
            period_dict.update(
                {item: {"type": "core", "date": current_date_key} for item in current_period_activity_core_set})
            self.up_period_contribution_last_type_dict[period] = period_dict

            result_dict = {
                "casual_up_regular_dict": casual_up_regular_dict,
                "casual_up_core_dict": casual_up_core_dict,
                "casual_up_regular_core_dict": casual_up_regular_core_dict,
                "regular_up_core_dict": regular_up_core_dict,
                "casual_regular_up_core_dict": casual_regular_up_core_dict,
                "total_up_dict": total_up_dict,
            }
            return result_dict

        last_period_conversion_up_dict = get_last_period_conversion_up_dict()
        same_period_conversion_up_dict = get_same_period_conversion_up_dict()
        before_period_conversion_up_dict = get_before_period_conversion_up_dict()

        return last_period_conversion_up_dict, same_period_conversion_up_dict, before_period_conversion_up_dict

    def get_freq_contributor_conversion_down_dict(self, from_date, to_date, activity_contributor_date_dict,
                                                  observe_activity_contributor_date_dict,
                                                  period="week"):

        def get_last_period_conversion_down_dict():
            """
                Last cycle was a casual and converted to a regular/core contributor or
                last cycle was a regular and converted to a core contributor this cycle
            """
            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, activity_contributor_date_dict, observe_activity_contributor_date_dict, period)

            last_period_from_date = from_date - str_to_offset(self.period_dict[period]["offset"])
            last_period_to_date = from_date

            last_period_activity_total_set, \
            last_period_activity_casual_set, \
            last_period_activity_regular_set, \
            last_period_activity_core_set = self.get_freq_contributor_activity_set(
                last_period_from_date, last_period_to_date, activity_contributor_date_dict,
                observe_activity_contributor_date_dict, period)

            # core --> regular
            core_down_regular_set = current_period_activity_regular_set & last_period_activity_core_set
            core_down_regular_dict = {
                "count": len(core_down_regular_set),
                "ratio": round(len(core_down_regular_set) / len(last_period_activity_core_set), 4) if len(
                    last_period_activity_core_set) > 0 else 0
            }
            # core --> casual
            core_down_casual_set = current_period_activity_casual_set & last_period_activity_core_set
            core_down_casual_dict = {
                "count": len(core_down_casual_set),
                "ratio": round(len(core_down_casual_set) / len(last_period_activity_core_set), 4) if len(
                    last_period_activity_core_set) > 0 else 0
            }
            # core --> casual + regular
            core_down_casual_regular_set = core_down_regular_set | core_down_casual_set
            core_down_casual_regular_dict = {
                "count": len(core_down_casual_regular_set),
                "ratio": round(len(core_down_casual_regular_set) / len(last_period_activity_core_set), 4) if len(
                    last_period_activity_core_set) > 0 else 0
            }
            # regular --> casual
            regular_down_casual_set = current_period_activity_casual_set & last_period_activity_regular_set
            regular_down_casual_dict = {
                "count": len(regular_down_casual_set),
                "ratio": round(len(regular_down_casual_set) / len(last_period_activity_regular_set), 4) if len(
                    last_period_activity_regular_set) > 0 else 0
            }
            # core + regular --> casual
            core_regular_down_casual_set = core_down_casual_set | regular_down_casual_set
            last_period_activity_core_regular_set = (
                    last_period_activity_core_set | last_period_activity_regular_set)
            core_regular_down_casual_dict = {
                "count": len(core_regular_down_casual_set),
                "ratio": round(len(core_regular_down_casual_set) / len(last_period_activity_core_regular_set), 4) if len(
                    last_period_activity_core_regular_set) > 0 else 0
            }
            # total
            total_down_set = core_down_casual_regular_set | core_regular_down_casual_set
            total_down_dict = {
                "count": len(total_down_set),
                "ratio": round(len(total_down_set) /
                               len(last_period_activity_core_regular_set), 4) if len(
                    last_period_activity_core_regular_set) > 0 else 0
            }

            result_dict = {
                "core_down_regular_dict": core_down_regular_dict,
                "core_down_casual_dict": core_down_casual_dict,
                "core_down_casual_regular_dict": core_down_casual_regular_dict,
                "regular_down_casual_dict": regular_down_casual_dict,
                "core_regular_down_casual_dict": core_regular_down_casual_dict,
                "total_down_dict": total_down_dict,
            }
            return result_dict

        def get_same_period_conversion_down_dict():
            """
                Contributor who became a casual in the same cycle and became a regular in the current cycle or
                Contributor who became a casual/regular in the same cycle and became a core contributor in the current cycle
            """
            core_down_regular_list = []
            core_down_casual_list = []
            core_down_casual_regular_list = []
            regular_down_casual_list = []
            core_regular_down_casual_list = []
            total_down_list = []

            current_date_key = from_date.strftime("%Y-%m-%d")
            period_dict = self.down_period_activity_contribution_dict.get(period, {})

            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, activity_contributor_date_dict, observe_activity_contributor_date_dict, period)

            if len(period_dict) > 0:
                sorted_period_items = sorted(period_dict.items(), key=lambda x: x[0], reverse=True)
                for date_key, date_value in sorted_period_items:
                    if len(total_down_list) > 48:
                        break
                    end_date = (datetime_to_utc(str_to_datetime(date_key)) + str_to_offset(
                        self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
                    period_num = len(get_date_list(date_key, current_date_key, self.period_dict[period]["freq"])) - 1

                    # core --> regular
                    core_down_regular_set = date_value["activity_core_set"] & current_period_activity_regular_set
                    core_down_regular_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(core_down_regular_set),
                        "ratio": round(len(core_down_regular_set) / len(date_value[
                                                                                "activity_core_set"]), 4) if len(
                            date_value["activity_core_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # core --> casual
                    core_down_casual_set = date_value["activity_core_set"] & current_period_activity_casual_set
                    core_down_casual_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(core_down_casual_set),
                        "ratio": round(len(core_down_casual_set) / len(date_value[
                                                                                "activity_core_set"]), 4) if len(
                            date_value["activity_core_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # core --> casual + regular
                    core_down_casual_regular_set = core_down_casual_set | core_down_regular_set
                    core_down_casual_regular_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(core_down_casual_regular_set),
                        "ratio": round(len(core_down_casual_regular_set) / len(date_value[
                                                                                "activity_core_set"]), 4) if len(
                            date_value["activity_core_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # regular --> casual
                    regular_down_casual_set = date_value["activity_regular_set"] & current_period_activity_casual_set
                    regular_down_casual_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(regular_down_casual_set),
                        "ratio": round(len(regular_down_casual_set) / len(date_value[
                                                                                "activity_regular_set"]), 4) if len(
                            date_value["activity_regular_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # core + regular --> casual
                    core_regular_down_casual_set = core_down_casual_regular_set | regular_down_casual_set
                    last_period_activity_core_regular_set = date_value["activity_core_set"] | date_value[
                        "activity_regular_set"]
                    core_regular_down_casual_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(core_regular_down_casual_set),
                        "ratio": round(len(core_regular_down_casual_set) / len(last_period_activity_core_regular_set), 4) if len(
                            last_period_activity_core_regular_set) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # total
                    total_down_set = core_regular_down_casual_set | core_down_casual_regular_set
                    total_down_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(total_down_set),
                        "ratio": round(len(total_down_set) / len(last_period_activity_core_regular_set), 4) if len(
                            last_period_activity_core_regular_set) > 0 else 0,
                        f"{period}_num": period_num
                    })

            period_dict[current_date_key] = {
                "activity_total_set": current_period_activity_total_set,
                "activity_casual_set": current_period_activity_casual_set,
                "activity_regular_set": current_period_activity_regular_set,
                "activity_core_set": current_period_activity_core_set,
            }
            self.down_period_activity_contribution_dict[period] = period_dict
            current_end_date = (datetime_to_utc(str_to_datetime(current_date_key)) + str_to_offset(
                self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
            
            # waiting core --> regular
            core_down_regular_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_core_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting core --> casual
            core_down_casual_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_core_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting core --> casual + regular
            core_down_casual_regular_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_core_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting regular --> casual
            regular_down_casual_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_regular_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting core + regular --> casual
            core_regular_down_casual_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_core_set | current_period_activity_regular_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting total
            total_down_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_core_set | current_period_activity_regular_set),
                "ratio": 0,
                f"{period}_num": 0
            })

            result_dict = {
                "core_down_regular_list": sorted(core_down_regular_list, key=lambda x: x["start_date"]),
                "core_down_casual_list": sorted(core_down_casual_list, key=lambda x: x["start_date"]),
                "core_down_casual_regular_list": sorted(core_down_casual_regular_list, key=lambda x: x["start_date"]),
                "regular_down_casual_list": sorted(regular_down_casual_list, key=lambda x: x["start_date"]),
                "core_regular_down_casual_list": sorted(core_regular_down_casual_list, key=lambda x: x["start_date"]),
                "total_down_list": sorted(total_down_list, key=lambda x: x["start_date"]),
            }
            return result_dict

        def get_before_period_conversion_down_dict():
            """
                A casual before this cycle, converted to a regular contributor this cycle. Or
                a casual/regular before this cycle, converted to a core contributor this cycle
            """
            current_date_key = from_date.strftime("%Y-%m-%d")
            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, activity_contributor_date_dict, observe_activity_contributor_date_dict, period)
            period_dict = self.down_period_contribution_last_type_dict.get(period, {})

            before_period_core_set = {k for k, v in period_dict.items() if v['type'] == "core"}
            before_period_regular_set = {k for k, v in period_dict.items() if v['type'] == "regular"}
            before_period_core_regular_set = before_period_core_set | before_period_regular_set
            # core --> regular
            core_down_regular_set = current_period_activity_regular_set & before_period_core_set
            core_down_regular_dict = {
                "count": len(core_down_regular_set),
                "ratio": round(len(core_down_regular_set) / len(before_period_core_set), 4) if len(
                    before_period_core_set) > 0 else 0
            }
            # core --> casual
            core_down_casual_set = current_period_activity_casual_set & before_period_core_set
            core_down_casual_dict = {
                "count": len(core_down_casual_set),
                "ratio": round(len(core_down_casual_set) / len(before_period_core_set), 4) if len(
                    before_period_core_set) > 0 else 0
            }
            # core --> casual + regular
            core_down_casual_regular_set = core_down_casual_set | core_down_regular_set
            core_down_casual_regular_dict = {
                "count": len(core_down_casual_regular_set),
                "ratio": round(len(core_down_casual_regular_set) / len(before_period_core_set), 4) if len(
                    before_period_core_set) > 0 else 0
            }
            # regular --> casual
            regular_down_casual_set = current_period_activity_casual_set & before_period_regular_set
            regular_down_casual_dict = {
                "count": len(regular_down_casual_set),
                "ratio": round(len(regular_down_casual_set) / len(before_period_regular_set), 4) if len(
                    before_period_regular_set) > 0 else 0
            }
            # core + regular --> casual
            core_regular_down_casual_set = core_down_casual_set | regular_down_casual_set
            core_regular_down_casual_dict = {
                "count": len(core_regular_down_casual_set),
                "ratio": round(len(core_regular_down_casual_set) / len(before_period_core_regular_set), 4) if len(
                    before_period_core_regular_set) > 0 else 0
            }
            # total
            total_down_set = core_regular_down_casual_set | core_down_casual_regular_set
            total_down_dict = {
                "count": len(total_down_set),
                "ratio": round(len(total_down_set) / len(before_period_core_regular_set), 4) if len(
                    before_period_core_regular_set) > 0 else 0
            }

            period_dict.update(
                {item: {"type": "casual", "date": current_date_key} for item in current_period_activity_casual_set})
            period_dict.update(
                {item: {"type": "regular", "date": current_date_key} for item in current_period_activity_regular_set})
            period_dict.update(
                {item: {"type": "core", "date": current_date_key} for item in current_period_activity_core_set})
            self.down_period_contribution_last_type_dict[period] = period_dict

            result_dict = {
                "core_down_regular_dict": core_down_regular_dict,
                "core_down_casual_dict": core_down_casual_dict,
                "core_down_casual_regular_dict": core_down_casual_regular_dict,
                "regular_down_casual_dict": regular_down_casual_dict,
                "core_regular_down_casual_dict": core_regular_down_casual_dict,
                "total_down_dict": total_down_dict,
            }
            return result_dict

        last_period_conversion_down_dict = get_last_period_conversion_down_dict()
        same_period_conversion_down_dict = get_same_period_conversion_down_dict()
        before_period_conversion_down_dict = get_before_period_conversion_down_dict()

        return last_period_conversion_down_dict, same_period_conversion_down_dict, before_period_conversion_down_dict

    def get_type_contributor_conversion_dict(self, from_date, to_date, source_activity_contributor_list,
                                             source_date_field_list,
                                             to_activity_contributor_list, to_date_field_list,
                                             is_bot=False,
                                             period="week"):
        current_period_activity_set = self.get_type_contributor_activity_set(
            from_date, to_date, to_activity_contributor_list, to_date_field_list, is_bot)

        last_period_from_date = from_date - str_to_offset(self.period_dict[period]["offset"])
        last_period_to_date = from_date
        last_period_activity_set = self.get_type_contributor_activity_set(
            last_period_from_date, last_period_to_date, source_activity_contributor_list, source_date_field_list,
            is_bot)
        retention_dict = {
            "count": len(current_period_activity_set & last_period_activity_set),
            "ratio": round(len(current_period_activity_set & last_period_activity_set) /
                           len(last_period_activity_set), 4) if len(last_period_activity_set) > 0 else 0
        }
        current_date_key = from_date.strftime("%Y-%m-%d")
        period_dict = self.period_type_contribution_dict.get(period, {})
        period_dict[current_date_key] = current_period_activity_set | period_dict.get(current_date_key, set())
        self.period_type_contribution_dict[period] = period_dict
        return retention_dict

    def get_type_total_contributor_conversion_dict(self, from_date, to_date, source_activity_contributor_list,
                                                   source_date_field_list,
                                                   is_bot=False, period="week"):
        current_date_key = from_date.strftime("%Y-%m-%d")
        period_dict = self.period_type_contribution_dict.get(period, {})
        current_period_conversion_set = period_dict.get(current_date_key, set())

        last_period_from_date = from_date - str_to_offset(self.period_dict[period]["offset"])
        last_period_to_date = from_date
        last_period_activity_set = self.get_type_contributor_activity_set(
            last_period_from_date, last_period_to_date, source_activity_contributor_list, source_date_field_list,
            is_bot)
        retention_dict = {
            "count": len(current_period_conversion_set & last_period_activity_set),
            "ratio": round(len(current_period_conversion_set & last_period_activity_set) /
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
                created_since = self.created_since(to_date, repos_list)
                if created_since is None:
                    continue
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

                contribution_activity_date_dict = self.get_contribution_activity_date_dict((
                        issue_creation_contributor_list +
                        pr_creation_contributor_list +
                        issue_comments_contributor_list +
                        pr_review_contributor_list +
                        code_commit_contributor_list),
                    ["issue_creation_date_list",
                     "pr_creation_date_list",
                     "issue_comments_date_list",
                     "pr_review_date_list",
                     "code_commit_date_list"])
                observe_contribution_activity_date_dict = self.get_contribution_activity_date_dict((
                        star_contributor_list +
                        fork_contributor_list +
                        watch_contributor_list),
                    ["star_date_list",
                     "fork_date_list",
                     "watch_date_list"])
                last_period_conversion_up_dict, \
                same_period_conversion_up_dict, \
                before_period_conversion_up_dict = self.get_freq_contributor_conversion_up_dict(
                    date, to_date, contribution_activity_date_dict, observe_contribution_activity_date_dict, period_key)
                last_period_conversion_down_dict, \
                same_period_conversion_down_dict, \
                before_period_conversion_down_dict = self.get_freq_contributor_conversion_down_dict(
                    date, to_date, contribution_activity_date_dict, observe_contribution_activity_date_dict, period_key)

                source_observe_contributor_list = (issue_creation_contributor_list +
                                                   pr_creation_contributor_list +
                                                   issue_comments_contributor_list +
                                                   pr_review_contributor_list +
                                                   code_commit_contributor_list)
                source_observe_date_field_list = ["issue_creation_date_list",
                                                  "pr_creation_date_list",
                                                  "issue_comments_date_list",
                                                  "pr_review_date_list",
                                                  "code_commit_date_list"]
                type_observe_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                              source_observe_contributor_list,
                                                                              source_observe_date_field_list,
                                                                              (star_contributor_list +
                                                                               fork_contributor_list +
                                                                               watch_contributor_list),
                                                                              ["star_date_list",
                                                                               "fork_date_list",
                                                                               "watch_date_list"], period=period_key)
                type_observe_star_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                                   source_observe_contributor_list,
                                                                                   source_observe_date_field_list,
                                                                                   star_contributor_list,
                                                                                   ["star_date_list"],
                                                                                   period=period_key)
                type_observe_fork_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                                   source_observe_contributor_list,
                                                                                   source_observe_date_field_list,
                                                                                   fork_contributor_list,
                                                                                   ["fork_date_list"],
                                                                                   period=period_key)
                type_observe_watch_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                                    source_observe_contributor_list,
                                                                                    source_observe_date_field_list,
                                                                                    watch_contributor_list,
                                                                                    ["watch_date_list"],
                                                                                    period=period_key)
                source_issue_contributor_list = (pr_creation_contributor_list +
                                                 pr_review_contributor_list +
                                                 code_commit_contributor_list +
                                                 star_contributor_list +
                                                 fork_contributor_list +
                                                 watch_contributor_list)
                source_issue_date_field_list = ["pr_creation_date_list",
                                                "pr_review_date_list",
                                                "code_commit_date_list",
                                                "star_date_list",
                                                "fork_date_list",
                                                "watch_date_list"]
                type_issue_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                            source_issue_contributor_list,
                                                                            source_issue_date_field_list,
                                                                            (issue_creation_contributor_list +
                                                                             issue_comments_contributor_list),
                                                                            ["issue_creation_date_list",
                                                                             "issue_comments_date_list"],
                                                                            period=period_key)
                type_issue_creator_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                                    source_issue_contributor_list,
                                                                                    source_issue_date_field_list,
                                                                                    issue_creation_contributor_list,
                                                                                    ["issue_creation_date_list"],
                                                                                    period=period_key)
                type_issue_commenter_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                                      source_issue_contributor_list,
                                                                                      source_issue_date_field_list,
                                                                                      issue_comments_contributor_list,
                                                                                      ["issue_comments_date_list"],
                                                                                      period=period_key)
                source_code_contributor_list = (issue_creation_contributor_list +
                                                issue_comments_contributor_list +
                                                star_contributor_list +
                                                fork_contributor_list +
                                                watch_contributor_list)
                source_code_date_field_list = ["issue_creation_date_list",
                                               "issue_comments_date_list",
                                               "star_date_list",
                                               "fork_date_list",
                                               "watch_date_list"]
                type_code_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                           source_code_contributor_list,
                                                                           source_code_date_field_list,
                                                                           (pr_creation_contributor_list +
                                                                            pr_review_contributor_list +
                                                                            code_commit_contributor_list),
                                                                           ["pr_creation_date_list",
                                                                            "pr_review_date_list",
                                                                            "code_commit_date_list"], period=period_key)
                type_code_author_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                                  source_code_contributor_list,
                                                                                  source_code_date_field_list,
                                                                                  code_commit_contributor_list,
                                                                                  ["code_commit_date_list"],
                                                                                  period=period_key)
                type_code_pr_creator_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                                      source_code_contributor_list,
                                                                                      source_code_date_field_list,
                                                                                      pr_creation_contributor_list,
                                                                                      ["pr_creation_date_list"],
                                                                                      period=period_key)
                type_code_pr_reviewer_dict = self.get_type_contributor_conversion_dict(date, to_date,
                                                                                       source_code_contributor_list,
                                                                                       source_code_date_field_list,
                                                                                       pr_review_contributor_list,
                                                                                       ["pr_review_date_list"],
                                                                                       period=period_key)
                type_total_dict = self.get_type_total_contributor_conversion_dict(date, to_date,
                                                                                  (
                                                                                          issue_creation_contributor_list +
                                                                                          pr_creation_contributor_list +
                                                                                          pr_review_contributor_list +
                                                                                          issue_comments_contributor_list +
                                                                                          code_commit_contributor_list +
                                                                                          star_contributor_list +
                                                                                          fork_contributor_list +
                                                                                          watch_contributor_list),
                                                                                  ["issue_creation_date_list",
                                                                                   "pr_creation_date_list",
                                                                                   "pr_review_date_list",
                                                                                   "issue_comments_date_list",
                                                                                   "pr_review_date_list",
                                                                                   "code_commit_date_list",
                                                                                   "star_date_list",
                                                                                   "fork_date_list",
                                                                                   "watch_date_list"],
                                                                                  period=period_key)
                uuid_value = get_uuid(str(date), self.community, level, label, self.model_name, type, period_key,
                                      self.weights_hash, self.custom_fields_hash)
                metrics_data = {
                    'uuid': uuid_value,
                    'level': level,
                    'type': type,
                    'label': label,
                    'model_name': self.model_name,
                    'period': period_key,

                    'freq_up_total_count': last_period_conversion_up_dict["total_up_dict"]["count"], 
                    'freq_up_total_ratio': last_period_conversion_up_dict["total_up_dict"]["ratio"], 
                    'freq_up_casual_to_regular_count': last_period_conversion_up_dict["casual_up_regular_dict"]["count"], 
                    'freq_up_casual_to_regular_ratio': last_period_conversion_up_dict["casual_up_regular_dict"]["ratio"], 
                    'freq_up_casual_to_core_count': last_period_conversion_up_dict["casual_up_core_dict"]["count"], 
                    'freq_up_casual_to_core_ratio': last_period_conversion_up_dict["casual_up_core_dict"]["ratio"], 
                    'freq_up_casual_to_regular_core_count': last_period_conversion_up_dict["casual_up_regular_core_dict"]["count"], 
                    'freq_up_casual_to_regular_core_ratio': last_period_conversion_up_dict["casual_up_regular_core_dict"]["ratio"], 
                    'freq_up_regular_to_core_count': last_period_conversion_up_dict["regular_up_core_dict"]["count"], 
                    'freq_up_regular_to_core_ratio': last_period_conversion_up_dict["regular_up_core_dict"]["ratio"], 
                    'freq_up_casual_regular_to_core_count': last_period_conversion_up_dict["casual_regular_up_core_dict"]["count"], 
                    'freq_up_casual_regular_to_core_ratio': last_period_conversion_up_dict["casual_regular_up_core_dict"]["ratio"],

                    'freq_up_before_period_total_count': before_period_conversion_up_dict["total_up_dict"]["count"], 
                    'freq_up_before_period_total_ratio': before_period_conversion_up_dict["total_up_dict"]["ratio"], 
                    'freq_up_before_period_casual_to_regular_count': before_period_conversion_up_dict["casual_up_regular_dict"]["count"], 
                    'freq_up_before_period_casual_to_regular_ratio': before_period_conversion_up_dict["casual_up_regular_dict"]["ratio"], 
                    'freq_up_before_period_casual_to_core_count': before_period_conversion_up_dict["casual_up_core_dict"]["count"], 
                    'freq_up_before_period_casual_to_core_ratio': before_period_conversion_up_dict["casual_up_core_dict"]["ratio"], 
                    'freq_up_before_period_casual_to_regular_core_count': before_period_conversion_up_dict["casual_up_regular_core_dict"]["count"], 
                    'freq_up_before_period_casual_to_regular_core_ratio': before_period_conversion_up_dict["casual_up_regular_core_dict"]["ratio"], 
                    'freq_up_before_period_regular_to_core_count': before_period_conversion_up_dict["regular_up_core_dict"]["count"], 
                    'freq_up_before_period_regular_to_core_ratio': before_period_conversion_up_dict["regular_up_core_dict"]["ratio"], 
                    'freq_up_before_period_casual_regular_to_core_count': before_period_conversion_up_dict["casual_regular_up_core_dict"]["count"], 
                    'freq_up_before_period_casual_regular_to_core_ratio': before_period_conversion_up_dict["casual_regular_up_core_dict"]["ratio"], 

                    'freq_up_same_period_total': same_period_conversion_up_dict["total_up_list"], 
                    'freq_up_same_period_casual_to_regular': same_period_conversion_up_dict["casual_up_regular_list"], 
                    'freq_up_same_period_casual_to_core': same_period_conversion_up_dict["casual_up_core_list"], 
                    'freq_up_same_period_casual_to_regular_core': same_period_conversion_up_dict["casual_up_regular_core_list"], 
                    'freq_up_same_period_regular_to_core': same_period_conversion_up_dict["regular_up_core_list"], 
                    'freq_up_same_period_casual_regular_to_core': same_period_conversion_up_dict["casual_regular_up_core_list"], 

                    'freq_down_total_count': last_period_conversion_down_dict["total_down_dict"]["count"], 
                    'freq_down_total_ratio': last_period_conversion_down_dict["total_down_dict"]["ratio"], 
                    'freq_down_core_to_regular_count': last_period_conversion_down_dict["core_down_regular_dict"]["count"], 
                    'freq_down_core_to_regular_ratio': last_period_conversion_down_dict["core_down_regular_dict"]["ratio"], 
                    'freq_down_core_to_casual_count': last_period_conversion_down_dict["core_down_casual_dict"]["count"], 
                    'freq_down_core_to_casual_ratio': last_period_conversion_down_dict["core_down_casual_dict"]["ratio"], 
                    'freq_down_core_to_casual_regular_count': last_period_conversion_down_dict["core_down_casual_regular_dict"]["count"], 
                    'freq_down_core_to_casual_regular_ratio': last_period_conversion_down_dict["core_down_casual_regular_dict"]["ratio"], 
                    'freq_down_regular_to_casual_count': last_period_conversion_down_dict["regular_down_casual_dict"]["count"], 
                    'freq_down_regular_to_casual_ratio': last_period_conversion_down_dict["regular_down_casual_dict"]["ratio"], 
                    'freq_down_core_regular_to_casual_count': last_period_conversion_down_dict["core_regular_down_casual_dict"]["count"], 
                    'freq_down_core_regular_to_casual_ratio': last_period_conversion_down_dict["core_regular_down_casual_dict"]["ratio"],

                    'freq_down_before_period_total_count': before_period_conversion_down_dict["total_down_dict"]["count"], 
                    'freq_down_before_period_total_ratio': before_period_conversion_down_dict["total_down_dict"]["ratio"], 
                    'freq_down_before_period_core_to_regular_count': before_period_conversion_down_dict["core_down_regular_dict"]["count"], 
                    'freq_down_before_period_core_to_regular_ratio': before_period_conversion_down_dict["core_down_regular_dict"]["ratio"], 
                    'freq_down_before_period_core_to_casual_count': before_period_conversion_down_dict["core_down_casual_dict"]["count"], 
                    'freq_down_before_period_core_to_casual_ratio': before_period_conversion_down_dict["core_down_casual_dict"]["ratio"], 
                    'freq_down_before_period_core_to_casual_regular_count': before_period_conversion_down_dict["core_down_casual_regular_dict"]["count"], 
                    'freq_down_before_period_core_to_casual_regular_ratio': before_period_conversion_down_dict["core_down_casual_regular_dict"]["ratio"], 
                    'freq_down_before_period_regular_to_casual_count': before_period_conversion_down_dict["regular_down_casual_dict"]["count"], 
                    'freq_down_before_period_regular_to_casual_ratio': before_period_conversion_down_dict["regular_down_casual_dict"]["ratio"], 
                    'freq_down_before_period_core_regular_to_casual_count': before_period_conversion_down_dict["core_regular_down_casual_dict"]["count"], 
                    'freq_down_before_period_core_regular_to_casual_ratio': before_period_conversion_down_dict["core_regular_down_casual_dict"]["ratio"],

                    'freq_down_same_period_total': same_period_conversion_down_dict["total_down_list"], 
                    'freq_down_same_period_core_to_regular': same_period_conversion_down_dict["core_down_casual_list"], 
                    'freq_down_same_period_core_to_casual': same_period_conversion_down_dict["core_down_casual_regular_list"], 
                    'freq_down_same_period_core_to_casual_regular': same_period_conversion_down_dict["regular_down_casual_list"], 
                    'freq_down_same_period_regular_to_casual': same_period_conversion_down_dict["regular_down_casual_list"], 
                    'freq_down_same_period_core_regular_to_casual': same_period_conversion_down_dict["core_regular_down_casual_list"], 

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

                    'type_count': type_total_dict["count"],
                    'type_ratio': type_total_dict["ratio"],
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
