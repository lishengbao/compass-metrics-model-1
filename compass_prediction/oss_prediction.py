from OssPrediction.api import predict
from compass_common.opensearch_client_utils import get_client
from compass_common.datetime import datetime_utcnow
from datetime import timedelta, datetime
import pandas as pd
import warnings

warnings.filterwarnings("ignore")
    
def get_model_data(client, index_name, repo, model_name):

    def get_query(repo, model_name, from_date, to_date, size=500,  search_after=[]):
        query = {
            "size": size,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "level.keyword": "repo"
                            }
                        },
                        {
                            "match_phrase": {
                                "label.keyword": repo
                            }
                        },
                        {
                            "match_phrase": {
                                "model_name.keyword": model_name
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "grimoire_creation_date": {
                                    "gte": from_date.strftime("%Y-%m-%d"),
                                    "lt": to_date.strftime("%Y-%m-%d")
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
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

    result_list = []
    search_after = []
    to_date = datetime_utcnow()
    from_date = to_date - timedelta(days=1095)
    while True:
        query = get_query(repo, model_name, from_date, to_date, 500, search_after)
        message_list = client.search(index=index_name, body=query)["hits"]["hits"]
        if len(message_list) == 0:
            break
        search_after = message_list[len(message_list) - 1]["sort"]
        result_list = result_list + [message["_source"] for message in message_list]
    return result_list

def get_organizations_activity_metrics_model(client, model_index, repo):
    return get_model_data(client, model_index, repo, "Organizations Activity")

def get_collaboration_development_index_metrics_model(client, model_index, repo):
    return get_model_data(client, model_index, repo, "Code_Quality_Guarantee")

def get_community_service_and_support_metrics_model(client, model_index, repo):
    return get_model_data(client, model_index, repo, "Community Support and Service")

def get_activity_metrics_model(client, model_index, repo):
    return get_model_data(client, model_index, repo, "Activity")

def prediction_activity(repo, model_data_list):
    """ Predict open source project activity """
    start_time2 = datetime.now()
    data_list = []
    for model_data in model_data_list:
        df = pd.json_normalize(model_data)
        data_list.append(df)
    data_list = [data_list]
    repo_name_list = [repo]
    model_list = ['XGBoost', 'AdaBoost', 'RandomForest']
    # select = "big"
    model_list_list = [['KNN'], ['RandomForest'], ['XGBoost'], ['SVM'], ['Logistic'], ['AdaBoost'], model_list]
    select_list = ["small", "big"]
    for item in model_list_list:
        for select in select_list:
            start_time1 = datetime.now()
            ans = predict(data_list=data_list, repo_name_list=repo_name_list,
                    model_list=item, select=select)
            for a in ans.values():
                print(f"{item} -- {select} --------------------------------------------------")
                print(f"inactive: {a['probability'][0][0]}, active: {a['probability'][0][1]}")
                print("inactive" if a["predictions"] == 0 else "active")
                # print(["inactive", "active"])
                # print(a["probability"])
                # print(a['feature'])
            print(f"prediction finish : {str(datetime.now() - start_time1)}")
    print(f"prediction finish : {str(datetime.now() - start_time2)}")

def prediction_activity2(repo, model_data_list):
    """ Predict open source project activity """
    start_time2 = datetime.now()
    data_list = []
    for model_data in model_data_list:
        df = pd.json_normalize(model_data)
        data_list.append(df)
    data_list = [data_list]
    repo_name_list = [repo]
    model_list = ['XGBoost', 'AdaBoost', 'RandomForest']
    select = "big"
    ans = predict(data_list=data_list, repo_name_list=repo_name_list,
            model_list=model_list, select=select)
    for a in ans.values():
        print(f"{model_list} -- {select} --------------------------------------------------")
        print(f"inactive: {a['probability'][0][0]}, active: {a['probability'][0][1]}")
        print("inactive" if a["predictions"] == 0 else "active")
        # print(["inactive", "active"])
        # print(a["probability"])
        # print(a['feature'])
    print(f"prediction finish : {str(datetime.now() - start_time2)}")



if __name__ == '__main__':
    start_time = datetime.now()
    es_url = ""
    repo = "https://github.com/pytorch/pytorch"
    client = get_client(es_url)
    model_data_list = []
    model_data_list.append(get_activity_metrics_model(client, "pytorch_metric_model", repo))
    model_data_list.append(get_collaboration_development_index_metrics_model(client, "pytorch_metric_model", repo))
    model_data_list.append(get_community_service_and_support_metrics_model(client, "pytorch_metric_model", repo))
    model_data_list.append(get_organizations_activity_metrics_model(client, "pytorch_metric_model", repo))
    prediction_activity2(repo, model_data_list)
    print(f"finish : {str(datetime.now() - start_time)}")



    
