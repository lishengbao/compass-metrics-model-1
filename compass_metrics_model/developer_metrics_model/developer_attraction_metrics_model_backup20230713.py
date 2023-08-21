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
                                                                                         

logger = logging.getLogger(__name__)

MODEL_NAME = "Developer Attraction"
WEIGHTS_FILE = "developer_metrics_model/resources/developer_attraction_weights.yaml"

def check_times_has_overlap(dyna_start_time, dyna_end_time, fixed_start_time, fixed_end_time):
    return not (dyna_end_time < fixed_start_time or dyna_start_time > fixed_end_time)

class DeveloperAttractionMetricsModel(MetricsModel):
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
            # "week": {"freq": "W-MON", "offset": "1w"},
            # "month": {"freq": "MS", "offset": "1m"},
            # "seasonal": {"freq": "QS-JAN", "offset": "3m"},
            "year": {"freq": "AS", "offset": "1y"},
        }

    def get_contributor_attraction_set(self, from_date, to_date, date_field_contributor_dict, is_bot=False):
        from_date_str = from_date.isoformat()
        to_date_str = to_date.isoformat()
        attraction_set = set()
        contributor_date_dict = self.get_contribution_activity_date_dict(date_field_contributor_dict, is_bot)
        for contributor_name, date_list in contributor_date_dict.items():
            if len(list_sub(date_list, from_date_str, to_date_str)) > 0:
                if from_date_str <= min(date_list) <= to_date_str:
                    attraction_set.add(contributor_name)
        return attraction_set 
    
    # def get_freq_contributor_attraction_set(self, from_date, to_date, date_field_contributor_dict, is_bot=False):
    #     pass


    

    def get_eco_contributor_attraction_set(self, from_date, to_date, date_field_contributor_dict, is_bot=False):

        def get_first_contribution_date(contributor, date_field_list):
            """ Get first contribution time for contributor """
            date_list = []
            for date_field in date_field_list:
                contribution_date_list = contributor.get(date_field)
                if contribution_date_list:
                    date_list.append(contribution_date_list[0])
            return min(date_list)

        from_date = from_date.isoformat()
        to_date = to_date.isoformat()
        eco_leader_org_set = set()
        eco_leader_person_set = set()
        eco_participant_org_set = set()
        eco_participant_person_set = set()
        contributor_list = [item for sublist in date_field_contributor_dict.values() for item in sublist]
        date_field_list = list(date_field_contributor_dict.keys())
        for contributor in list({item["uuid"]: item for item in contributor_list}.values()):
            if (is_bot is None or contributor["is_bot"] == is_bot) and \
                    from_date <= get_first_contribution_date(contributor, date_field_list) <= to_date:
                is_leader = contributor.get("is_leader", False)
                is_org = False
                for org in contributor["org_change_date_list"]:
                    if org.get("org_name") is not None and check_times_has_overlap(org["first_date"], org["last_date"],
                                                                                   from_date, to_date):
                        is_org = True
                        break
                contributor_name = None
                if contributor.get("id_platform_login_name_list") and len(
                        contributor.get("id_platform_login_name_list")) > 0:
                    contributor_name = contributor["id_platform_login_name_list"][0]
                elif contributor.get("id_git_author_name_list") and len(contributor.get("id_git_author_name_list")) > 0:
                    contributor_name = contributor["id_git_author_name_list"][0]

                if is_leader and is_org:
                    eco_leader_org_set.add(contributor_name)
                elif is_leader and not is_org:
                    eco_leader_person_set.add(contributor_name)
                elif not is_leader and is_org:
                    eco_participant_org_set.add(contributor_name)
                elif not is_leader and not is_org:
                    eco_participant_person_set.add(contributor_name)

        return eco_leader_org_set, eco_leader_person_set, eco_participant_org_set, eco_participant_person_set


    def get_score(self, item, level="repo"):
        """ Get metrics model score """
        param_dict = {}
        if level == "community" or level == "project":
            param_dict = {
                "freq_casual_count": [self.weights['FREQ_CASUAL_COUNT_WEIGHT'],
                                      self.weights['FREQ_CASUAL_COUNT_MULTIPLE_THRESHOLD']],
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
                first_issue_creation_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                                  "first_issue_creation_date")
                first_pr_creation_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                               "first_pr_creation_date")
                first_issue_comments_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                                  "first_issue_comments_date")
                first_pr_review_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                             "first_pr_review_date")
                first_code_commit_contributor_list = self.get_contributor_list(date, to_date, repos_list,
                                                                               "first_code_commit_date")
                first_star_contributor_list = self.get_contributor_list(date, to_date, repos_list, "first_star_date")
                first_fork_contributor_list = self.get_contributor_list(date, to_date, repos_list, "first_fork_date")
                first_watch_contributor_list = self.get_contributor_list(date, to_date, repos_list, "first_watch_date")

                total_attraction_set, freq_casual_set, freq_regular_set, freq_core_set = self.get_freq_contributor_attraction_set(
                    date, to_date, {
                        "issue_creation_date_list": first_issue_creation_contributor_list,
                        "pr_creation_date_list": first_pr_creation_contributor_list,
                        "issue_comments_date_list": first_issue_comments_contributor_list,
                        "pr_review_date_list": first_pr_review_contributor_list,
                        "code_commit_date_list": first_code_commit_contributor_list,
                        "star_date_list": first_star_contributor_list,
                        "fork_date_list": first_fork_contributor_list,
                        "watch_date_list": first_watch_contributor_list
                    }, period=period_key)
                
                eco_leader_org_set, \
                eco_leader_person_set, \
                eco_participant_org_set, \
                eco_participant_person_set = self.get_eco_contributor_attraction_set(date, to_date, {
                        "issue_creation_date_list": first_issue_creation_contributor_list,
                        "pr_creation_date_list": first_pr_creation_contributor_list,
                        "issue_comments_date_list": first_issue_comments_contributor_list,
                        "pr_review_date_list": first_pr_review_contributor_list,
                        "code_commit_date_list": first_code_commit_contributor_list,
                        "star_date_list": first_star_contributor_list,
                        "fork_date_list": first_fork_contributor_list,
                        "watch_date_list": first_watch_contributor_list})

                type_observe_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "star_date_list": first_star_contributor_list,
                    "fork_date_list": first_fork_contributor_list,
                    "watch_date_list": first_watch_contributor_list})
                type_observe_star_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "star_date_list": first_star_contributor_list})
                type_observe_fork_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "fork_date_list": first_fork_contributor_list})
                type_observe_watch_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "watch_date_list": first_watch_contributor_list})
                type_issue_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "issue_creation_date_list": first_issue_creation_contributor_list,
                    "issue_comments_date_list": first_issue_comments_contributor_list})
                type_issue_creator_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "issue_creation_date_list": first_issue_creation_contributor_list})
                type_issue_commenter_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "issue_comments_date_list": first_issue_comments_contributor_list})
                type_code_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "pr_creation_date_list": first_pr_creation_contributor_list,
                    "pr_review_date_list": first_pr_review_contributor_list,
                    "code_commit_date_list": first_code_commit_contributor_list})
                type_code_author_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "code_commit_date_list": first_code_commit_contributor_list})
                type_code_pr_creator_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "pr_creation_date_list": first_pr_creation_contributor_list})
                type_code_pr_reviewer_set = self.get_type_contributor_attraction_set(date, to_date, {
                    "pr_review_date_list": first_pr_review_contributor_list})
                uuid_value = get_uuid(str(date), self.community, level, label, self.model_name, type, period_key,
                                      self.weights_hash, self.custom_fields_hash)
                metrics_data = {
                    'uuid': uuid_value,
                    'level': level,
                    'type': type,
                    'label': label,
                    'model_name': self.model_name,
                    'period': period_key,
                    'freq_count': len(total_attraction_set),
                    'freq_casual_count': len(freq_casual_set),
                    'freq_regular_count': len(freq_regular_set),
                    'freq_core_count': len(freq_core_set),
                    'eco_count': len(total_attraction_set),
                    'eco_leader_org_count': len(eco_leader_org_set),
                    'eco_leader_person_count': len(eco_leader_person_set),
                    'eco_participant_org_count': len(eco_participant_org_set),
                    'eco_participant_person_count': len(eco_participant_person_set),
                    'type_count': len(total_attraction_set),
                    'type_observe_count': len(type_observe_set),
                    'type_observe_star_count': len(type_observe_star_set),
                    'type_observe_fork_count': len(type_observe_fork_set),
                    'type_observe_watch_count': len(type_observe_watch_set),
                    'type_issue_count': len(type_issue_set),
                    'type_issue_creator_count': len(type_issue_creator_set),
                    'type_issue_commenter_count': len(type_issue_commenter_set),
                    'type_code_count': len(type_code_set),
                    'type_code_author_count': len(type_code_author_set),
                    'type_code_pr_creator_count': len(type_code_pr_creator_set),
                    'type_code_pr_reviewer_count': len(type_code_pr_reviewer_set),
                    'type_forum_count': None,
                    'type_chat_count': None,
                    'type_media_count': None,
                    'type_event_count': None,
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
