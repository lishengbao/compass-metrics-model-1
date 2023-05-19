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
                                         str_to_offset,
                                         get_uuid)
from compass_common.utils.list_utils import list_sub                                        

logger = logging.getLogger(__name__)

MODEL_NAME = "Developer Attrition"
WEIGHTS_FILE = "developer_metrics_model/resources/developer_attrition_weights.yaml"


class DeveloperAttritionMetricsModel(MetricsModel):
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
        self.period_contribution_last_date_dict = {}
        self.period_activity_contribution_dict = {}
        self.period_type_contribution_last_date_dict = {}

    def get_freq_contributor_attrition_dict(self, from_date, to_date, date_field_contributor_dict, is_bot=False,
                                            period="week"):

        current_attrition_total_set = set()


        def get_conversion_to_silence_dict():
            """
                Contributors who entered silence in this period
            """
            start_time = datetime.now()
            nonlocal current_attrition_total_set
            attrition_total_set = set()
            attrition_casual_set = set()
            attrition_regular_set = set()
            attrition_core_set = set()

            activity_total_set = set()
            activity_casual_set = set()
            activity_regular_set = set()
            activity_core_set = set()

            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, date_field_contributor_dict, is_bot, period)
            period_dict = self.period_contribution_last_date_dict.get(period, {})

            contributor_list = [item for sublist in date_field_contributor_dict.values() for item in sublist]
            date_field_list = list(date_field_contributor_dict.keys())
            contributor_date_dict = self.get_contribution_activity_date_dict(contributor_list, date_field_list, is_bot)

            now1 = datetime.now()
            for contributor_name, contribution_date_list in contributor_date_dict.items():
                if len(list_sub(contribution_date_list, from_date.isoformat(), to_date.isoformat())) == 0:
                    continue
                day = from_date
                while True:

                    contribution_last_dict = period_dict.get(contributor_name)
                    if contribution_last_dict is not None and \
                            from_date <= (
                            str_to_datetime(contribution_last_dict["last_date"]) + str_to_offset("90d")) < to_date:
                        activity_total_set.add(contributor_name)
                        if contribution_last_dict.get("type") == "casual":
                            activity_casual_set.add(contributor_name)
                        elif contribution_last_dict.get("type") == "regular":
                            activity_regular_set.add(contributor_name)
                        elif contribution_last_dict.get("type") == "core":
                            activity_core_set.add(contributor_name)

                    last_90_day_contribution_date = list_sub(contribution_date_list,
                                                             (day + str_to_offset("-90d")).isoformat(),
                                                             day.isoformat(), start_include=False, end_include=True)
                    if len(last_90_day_contribution_date) == 0:
                        if contribution_last_dict is not None and \
                                from_date <= (
                                str_to_datetime(contribution_last_dict["last_date"]) + str_to_offset("90d")) < to_date:
                            attrition_total_set.add(contributor_name)
                            if contribution_last_dict.get("type") == "casual":
                                attrition_casual_set.add(contributor_name)
                            elif contribution_last_dict.get("type") == "regular":
                                attrition_regular_set.add(contributor_name)
                            elif contribution_last_dict.get("type") == "core":
                                attrition_core_set.add(contributor_name)
                        day_to_date_list = list_sub(contribution_date_list, day.isoformat(), to_date.isoformat(),
                                                    start_include=False,
                                                    end_include=False)
                        if len(day_to_date_list) == 0:
                            break
                        day = str_to_datetime(min(day_to_date_list))
                    else:
                        contributor_type = ""
                        if contributor_name in current_period_activity_casual_set:
                            contributor_type = "casual"
                        elif contributor_name in current_period_activity_regular_set:
                            contributor_type = "regular"
                        elif contributor_name in current_period_activity_core_set:
                            contributor_type = "core"
                        period_dict[contributor_name] = {
                            "type": contributor_type,
                            "last_date": max(last_90_day_contribution_date)
                        }
                        day = str_to_datetime(max(last_90_day_contribution_date)) + str_to_offset("90d")
                        if day >= to_date:
                            period_dict[contributor_name] = {
                                "type": contributor_type,
                                "last_date": max(contribution_date_list)
                            }
                            break

            self.period_contribution_last_date_dict[period] = period_dict
            logger.info("time:1-1 ---" + str(datetime.now() - now1))

            now2 = datetime.now()
            not_activity_contributor_date_dict = {k: v for k, v in period_dict.items() if
                                                  from_date.isoformat() > v.get("last_date")}
            for contributor_name, contributor_value in not_activity_contributor_date_dict.items():
                if from_date <= str_to_datetime(contributor_value.get("last_date")) + str_to_offset("90d") < to_date:
                    if contributor_value.get("type") == "casual":
                        attrition_casual_set.add(contributor_name)
                        activity_casual_set.add(contributor_name)
                    elif contributor_value.get("type") == "regular":
                        attrition_regular_set.add(contributor_name)
                        activity_regular_set.add(contributor_name)
                    elif contributor_value.get("type") == "core":
                        attrition_core_set.add(contributor_name)
                        activity_core_set.add(contributor_name)
                    attrition_total_set.add(contributor_name)
                    activity_total_set.add(contributor_name)

            attrition_total_dict = {
                "count": len(attrition_total_set),
                "ratio": round(len(attrition_total_set) / len(activity_total_set), 4) if len(
                    activity_total_set) > 0 else 0
            }
            attrition_casual_dict = {
                "count": len(attrition_casual_set),
                "ratio": round(len(attrition_casual_set) / len(activity_casual_set), 4) if len(
                    activity_casual_set) > 0 else 0
            }
            attrition_regular_dict = {
                "count": len(attrition_regular_set),
                "ratio": round(len(attrition_regular_set) / len(activity_regular_set), 4) if len(
                    activity_regular_set) > 0 else 0
            }
            attrition_core_dict = {
                "count": len(attrition_core_set),
                "ratio": round(len(attrition_core_set) / len(activity_core_set), 4) if len(
                    activity_core_set) > 0 else 0
            }
            current_attrition_total_set = attrition_total_set
            logger.info("time:1-2  ---" + str(datetime.now() - now2))

            logger.info("time:1---"+str(datetime.now() - start_time))
            return attrition_total_dict, attrition_casual_dict, attrition_regular_dict, attrition_core_dict

        def get_silence_dict():
            """
                Contributors who are in silence state during this period
            """
            start_time = datetime.now()

            attrition_total_set = set()
            attrition_casual_set = set()
            attrition_regular_set = set()
            attrition_core_set = set()

            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, date_field_contributor_dict, is_bot, period)
            period_dict = self.period_contribution_last_date_dict.get(period, {})

            contributor_list = [item for sublist in date_field_contributor_dict.values() for item in sublist]
            date_field_list = list(date_field_contributor_dict.keys())
            contributor_date_dict = self.get_contribution_activity_date_dict(contributor_list, date_field_list, is_bot)

            start_time1 = datetime.now()
            for contributor_name, contribution_date_list in contributor_date_dict.items():
                if len(list_sub(contribution_date_list, from_date.isoformat(), to_date.isoformat())) == 0:
                    continue
                last_90_day_contribution_date = list_sub(contribution_date_list,
                                                        (to_date + str_to_offset("-90d")).isoformat(),
                                                        to_date.isoformat())
                if len(last_90_day_contribution_date) == 0:
                    if contributor_name in current_period_activity_total_set:
                        attrition_total_set.add(contributor_name)
                    elif contributor_name in current_period_activity_casual_set:
                        attrition_casual_set.add(contributor_name)
                    elif contributor_name in current_period_activity_regular_set:
                        attrition_regular_set.add(contributor_name)
                    elif contributor_name in current_period_activity_core_set:
                        attrition_core_set.add(contributor_name)
            logger.info("time:2-1 ---" + str(datetime.now() - start_time1))

            start_time2 = datetime.now()
            not_activity_contributor_date_dict = {k: v for k, v in period_dict.items() if
                                                  from_date.isoformat() > v.get("last_date")}
            for contributor_name, contributor_value in not_activity_contributor_date_dict.items():
                if from_date <= str_to_datetime(contributor_value.get("last_date")) + str_to_offset("90d") < to_date:
                    if contributor_value.get("type") == "casual":
                        attrition_casual_set.add(contributor_name)
                    elif contributor_value.get("type") == "regular":
                        attrition_regular_set.add(contributor_name)
                    elif contributor_value.get("type") == "core":
                        attrition_core_set.add(contributor_name)
                    attrition_total_set.add(contributor_name)
            logger.info("time:2-2 ---" + str(datetime.now() - start_time2))
            logger.info("time:2---" + str(datetime.now() - start_time))
            return attrition_total_set, attrition_casual_set, attrition_regular_set, attrition_core_set

        def get_same_period_conversion_to_silence_list(current_attrition_total_set):
            """
                Contributor who became a casual/regular/core in the same cycle and became a silence contributor
                in the current cycle
            """
            start_time = datetime.now()
            attrition_total_list = []
            attrition_casual_list = []
            attrition_regular_list = []
            attrition_core_list = []

            current_date_key = from_date.strftime("%Y-%m-%d")
            period_dict = self.period_activity_contribution_dict.get(period, {})

            current_period_activity_total_set, \
            current_period_activity_casual_set, \
            current_period_activity_regular_set, \
            current_period_activity_core_set = self.get_freq_contributor_activity_set(
                from_date, to_date, date_field_contributor_dict, is_bot, period)

            if len(period_dict) > 0:
                sorted_period_items = sorted(period_dict.items(), key=lambda x: x[0], reverse=True)
                for date_key, date_value in sorted_period_items:
                    if len(attrition_casual_list) > 48:
                        break
                    end_date = (datetime_to_utc(str_to_datetime(date_key)) + str_to_offset(
                        self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
                    period_num = len(get_date_list(date_key, current_date_key, self.period_dict[period]["freq"])) - 1
                    # casual --> silence
                    attrition_casual_set = date_value["activity_casual_set"] & current_attrition_total_set
                    attrition_casual_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(attrition_casual_set),
                        "ratio": round(len(attrition_casual_set) / len(date_value[
                                                                           "activity_casual_set"]), 4) if len(
                            date_value["activity_casual_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # regular --> silence
                    attrition_regular_set = date_value["activity_regular_set"] & current_attrition_total_set
                    attrition_regular_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(attrition_regular_set),
                        "ratio": round(len(attrition_regular_set) / len(date_value[
                                                                            "activity_regular_set"]), 4) if len(
                            date_value["activity_regular_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # core --> silence
                    attrition_core_set = date_value["activity_core_set"] & current_attrition_total_set
                    attrition_core_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(attrition_core_set),
                        "ratio": round(len(attrition_core_set) / len(date_value[
                                                                         "activity_core_set"]), 4) if len(
                            date_value["activity_core_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })
                    # total
                    attrition_total_set = date_value["activity_total_set"] & current_attrition_total_set
                    attrition_total_list.append({
                        "start_date": date_key,
                        "end_date": end_date,
                        "count": len(attrition_total_set),
                        "ratio": round(len(attrition_total_set) / len(date_value["activity_total_set"]), 4) if len(
                            date_value["activity_total_set"]) > 0 else 0,
                        f"{period}_num": period_num
                    })

            period_dict[current_date_key] = {
                "activity_total_set": current_period_activity_total_set,
                "activity_casual_set": current_period_activity_casual_set,
                "activity_regular_set": current_period_activity_regular_set,
                "activity_core_set": current_period_activity_core_set,
            }
            self.period_activity_contribution_dict[period] = period_dict
            current_end_date = (datetime_to_utc(str_to_datetime(current_date_key)) + str_to_offset(
                self.period_dict[period]["offset"]) + str_to_offset("-1d")).strftime("%Y-%m-%d")
            # waiting casual --> silence
            attrition_casual_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_casual_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting regular --> silence
            attrition_regular_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_regular_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting core --> slience
            attrition_core_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_core_set),
                "ratio": 0,
                f"{period}_num": 0
            })
            # waiting total
            attrition_total_list.append({
                "start_date": current_date_key,
                "end_date": current_end_date,
                "count": len(current_period_activity_total_set),
                "ratio": 0,
                f"{period}_num": 0
            })

            attrition_casual_list = sorted(attrition_casual_list, key=lambda x: x["start_date"])
            attrition_regular_list = sorted(attrition_regular_list, key=lambda x: x["start_date"])
            attrition_core_list = sorted(attrition_core_list, key=lambda x: x["start_date"])
            attrition_total_list = sorted(attrition_total_list, key=lambda x: x["start_date"])

            logger.info("time:3---" + str(datetime.now() - start_time))
            return attrition_total_list, attrition_casual_list, attrition_regular_list, attrition_core_list

        start_time = datetime.now()
        total_conversion_to_silence_dict, \
        casual_conversion_to_silence_dict, \
        regular_conversion_to_silence_dict, \
        core_conversion_to_silence_dict = get_conversion_to_silence_dict()

        attrition_total_set, \
        attrition_casual_set, \
        attrition_regular_set, \
        attrition_core_set = get_silence_dict()

        attrition_total_list, \
        attrition_casual_list, \
        attrition_regular_list, \
        attrition_core_list = get_same_period_conversion_to_silence_list(current_attrition_total_set)

        total_conversion_to_silence_dict["silence_count"] = len(attrition_total_set)
        casual_conversion_to_silence_dict["silence_count"] = len(attrition_casual_set)
        regular_conversion_to_silence_dict["silence_count"] = len(attrition_regular_set)
        core_conversion_to_silence_dict["silence_count"] = len(attrition_core_set)

        total_conversion_to_silence_dict["same_period"] = json.dumps(attrition_total_list)
        casual_conversion_to_silence_dict["same_period"] = json.dumps(attrition_casual_list)
        regular_conversion_to_silence_dict["same_period"] = json.dumps(attrition_regular_list)
        core_conversion_to_silence_dict["same_period"] = json.dumps(attrition_core_list)
        logger.info("time:4---" + str(datetime.now() - start_time))
        return total_conversion_to_silence_dict, casual_conversion_to_silence_dict, regular_conversion_to_silence_dict, core_conversion_to_silence_dict

    def get_type_contributor_attrition_dict(self, from_date, to_date, date_field_contributor_dict, is_bot=False,
                                            period="week"):
        start_time = datetime.now()
        attrition_total_set = set()
        activity_total_set = set()

        period_type_list = sorted([k.replace("_date_list", "") for k in date_field_contributor_dict.keys()])
        period_type = "&".join(period_type_list)
        period_dict = self.period_type_contribution_last_date_dict.get(period, {})
        period_type_dict = period_dict.get(period_type, {})

        contributor_list = [item for sublist in date_field_contributor_dict.values() for item in sublist]
        date_field_list = list(date_field_contributor_dict.keys())
        contributor_date_dict = self.get_contribution_activity_date_dict(contributor_list, date_field_list, is_bot)

        for contributor_name, contribution_date_list in contributor_date_dict.items():
            if len(list_sub(contribution_date_list, from_date.isoformat(), to_date.isoformat())) == 0:
                continue
            day = from_date
            while True:
                last_90_day_contribution_date = list_sub(contribution_date_list,
                                                        (day + str_to_offset("-90d")).isoformat(),
                                                        day.isoformat(), start_include=False, end_include=True)
                contribution_last_dict = period_type_dict.get(contributor_name)
                if contribution_last_dict is not None and \
                        from_date <= (
                        str_to_datetime(contribution_last_dict["last_date"]) + str_to_offset("90d")) < to_date:
                    activity_total_set.add(contributor_name)

                if len(last_90_day_contribution_date) == 0:
                    if contribution_last_dict is not None and \
                        from_date <= (
                        str_to_datetime(contribution_last_dict["last_date"]) + str_to_offset("90d")) < to_date:
                        attrition_total_set.add(contributor_name)
                    day_to_date_list = list_sub(contribution_date_list, day.isoformat(), to_date.isoformat(),
                                                start_include=False,
                                                end_include=False)
                    if len(day_to_date_list) == 0:
                        break
                    day = str_to_datetime(min(day_to_date_list))
                else:
                    period_type_dict[contributor_name] = {
                        "last_date": max(last_90_day_contribution_date)
                    }
                    day = str_to_datetime(max(last_90_day_contribution_date)) + str_to_offset("90d")
                    if day >= to_date:
                        period_type_dict[contributor_name] = {
                            "last_date": max(contribution_date_list)
                        }
                        break

        period_dict[period_type] = period_type_dict
        self.period_type_contribution_last_date_dict[period] = period_dict

        not_activity_contributor_date_dict = {k: v for k, v in period_type_dict.items() if
                                              from_date.isoformat() > v.get("last_date")}
        for contributor_name, contributor_value in not_activity_contributor_date_dict.items():
            if from_date <= str_to_datetime(contributor_value.get("last_date")) + str_to_offset("90d") < to_date:
                attrition_total_set.add(contributor_name)
                activity_total_set.add(contributor_name)

        attrition_total_dict = {
            "count": len(attrition_total_set),
            "ratio": round(len(attrition_total_set) / len(activity_total_set), 4) if len(
                activity_total_set) > 0 else 0
        }
        logger.info("time:5---" + str(datetime.now() - start_time))
        return attrition_total_dict

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

                total_silence_dict, \
                casual_silence_dict, \
                regular_silence_dict, \
                core_silence_dict = self.get_freq_contributor_attrition_dict(date, to_date, {
                        "issue_creation_date_list": issue_creation_contributor_list,
                        "pr_creation_date_list": pr_creation_contributor_list,
                        "issue_comments_date_list": issue_comments_contributor_list,
                        "pr_review_date_list": pr_review_contributor_list,
                        "code_commit_date_list": code_commit_contributor_list,
                        "star_date_list": star_contributor_list,
                        "fork_date_list": fork_contributor_list,
                        "watch_date_list": watch_contributor_list
                    }, period=period_key)
                type_observe_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "star_date_list": star_contributor_list,
                        "fork_date_list": fork_contributor_list,
                        "watch_date_list": watch_contributor_list}, period=period_key)
                type_observe_star_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "star_date_list": star_contributor_list}, period=period_key)
                type_observe_fork_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "fork_date_list": fork_contributor_list}, period=period_key)
                type_observe_watch_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "watch_date_list": watch_contributor_list}, period=period_key)
                type_issue_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "issue_creation_date_list": issue_creation_contributor_list,
                        "issue_comments_date_list": issue_comments_contributor_list}, period=period_key)
                type_issue_creator_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "issue_creation_date_list": issue_creation_contributor_list}, period=period_key)
                type_issue_commenter_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "issue_comments_date_list": issue_comments_contributor_list}, period=period_key)
                type_code_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "pr_creation_date_list": pr_creation_contributor_list,
                        "pr_review_date_list": pr_review_contributor_list,
                        "code_commit_date_list": code_commit_contributor_list}, period=period_key)
                type_code_author_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "code_commit_date_list": code_commit_contributor_list}, period=period_key)
                type_code_pr_creator_dict = self.get_type_contributor_attrition_dict(date, to_date, {
                        "pr_creation_date_list": pr_creation_contributor_list}, period=period_key)
                type_code_pr_reviewer_dict = self.get_type_contributor_attrition_dict(date, to_date, {
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

                    'freq_total_to_silence_count': total_silence_dict["count"],
                    'freq_total_to_silence_ratio': total_silence_dict["ratio"],
                    'freq_total_silence_count': total_silence_dict["silence_count"],
                    'freq_total_same_period': total_silence_dict["same_period"],
                    'freq_casual_to_silence_count': casual_silence_dict["count"],
                    'freq_casual_to_silence_ratio': casual_silence_dict["ratio"],
                    'freq_casual_silence_count': casual_silence_dict["silence_count"],
                    'freq_casual_same_period': casual_silence_dict["same_period"],
                    'freq_regular_to_silence_count': regular_silence_dict["count"],
                    'freq_regular_to_silence_ratio': regular_silence_dict["ratio"],
                    'freq_regular_silence_count': regular_silence_dict["silence_count"],
                    'freq_regular_same_period': regular_silence_dict["same_period"],
                    'freq_core_to_silence_count': core_silence_dict["count"],
                    'freq_core_to_silence_ratio': core_silence_dict["ratio"],
                    'freq_core_silence_count': core_silence_dict["silence_count"],
                    'freq_core_same_period': core_silence_dict["same_period"],

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

                    'type_count': total_silence_dict["count"],
                    'type_ratio': total_silence_dict["ratio"],
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
