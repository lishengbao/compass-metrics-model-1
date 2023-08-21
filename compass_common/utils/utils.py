import json

def get_all_repo(json_file, origin):
    all_repo = []
    all_repo_json = json.load(open(json_file))
    for project in all_repo_json:
        origin_software_artifact = origin + "-software-artifact"
        origin_governance = origin + "-governance"
        for key in all_repo_json[project].keys():
            if key == origin_software_artifact or key == origin_governance or key == origin:
                for repo in all_repo_json[project].get(key):
                    all_repo.append(repo)
    return all_repo

def get_all_org(json_file, origin):
    repo_list = get_all_repo(json_file, origin)
    org_set = {repo[:repo.rfind('/')] for repo in repo_list}
    return list(org_set)
    
