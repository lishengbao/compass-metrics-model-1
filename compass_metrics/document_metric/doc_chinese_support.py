'''
Descripttion: 
version: V1.0
Author: zyx
Date: 2025-02-17 10:25:54
LastEditors: zyx
LastEditTime: 2025-03-26 16:59:14
'''
'''
Descripttion: 
version: V1.0
Author: zyx
Date: 2025-02-17 10:25:54
LastEditors: zyx
LastEditTime: 2025-03-03 09:26:04
'''
import os 
import re
import json
import requests
from git import Repo
from compass_metrics.document_metric.utils import GITHUB_TOKEN,GITEE_TOKEN,TMP_PATH,JSON_REPOPATH
from compass_metrics.document_metric.utils import load_json,check_github_gitee,clone_repo,save_json

import unicodedata
GITHUB_HEADERS = {'Authorization': f'token {GITHUB_TOKEN}'}
GITEE_HEADERS = {'Authorization': f'token {GITEE_TOKEN}'}
REPOPATH = TMP_PATH
if not os.path.exists(REPOPATH):
    os.makedirs(REPOPATH)


def contains_chinese(text):
    for char in text:
        if 'CJK' in unicodedata.name(char, ''):
            print(char ,unicodedata.name(char, '') )
            return True
    return False
def chinese_ratio_exceeds_threshold(text, threshold=0.05):
    chinese_chars = sum(1 for char in text if re.search('[\u4e00-\u9fff]', char))
    total_chars = 1+len(text)
    # if (chinese_chars / total_chars) > threshold:
    #     print("12")
    return (chinese_chars / total_chars) > threshold


def doc_chinese_support(file_path):
    '''This function is used to support the Chinese version of the document'''
    # Read the content of the file
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        content = file.read()
        # Check if the content contains Chinese characters
        # if re.search('[\u4e00-\u9fff]', content):
        #     a = contains_chinese(content)
        #     return True
        # else:
        #     return False
        return chinese_ratio_exceeds_threshold(content)

def find_zh_files(json_path,url):
    '''Find all files containing Chinese characters in the specified folder'''
    zh_files = {"zh_files_number":0, "zh_files_details":[]}
    doc_details = load_json(json_path)["folder_document_details"]
    for doc_detail in doc_details:
        file_path = os.path.join(REPOPATH,doc_detail["path"])
        if doc_chinese_support(file_path):
            
            zh_files["zh_files_details"].append({})
            zh_files["zh_files_details"][zh_files["zh_files_number"]]["name"] = doc_detail["name"]
            zh_files["zh_files_details"][zh_files["zh_files_number"]]["path"] = doc_detail["path"]
            zh_files["zh_files_details"][zh_files["zh_files_number"]]["commit_time"] = get_file_commit_time(url, doc_detail["path"],platform=check_github_gitee(url))
            zh_files["zh_files_number"] += 1
    return zh_files

def get_file_commit_time(repo_url, file_path, platform='gitub'):
    '''Get the commit time of a file from a GitHub repository'''
    repo_name = repo_url.split('/')[-1]
    owner = repo_url.split('/')[-2]
    file_path = file_path.replace(repo_name, '')[1:]
    commits_url = f"https://api.github.com/repos/{owner}/{repo_name}/commits?path={file_path}"
    # print(commits_url)
    if platform == 'github':
        response = requests.get(commits_url, headers=GITHUB_HEADERS)
    else:
        response = requests.get(commits_url, headers=GITEE_HEADERS)

    if response.status_code == 200:
        commit_data = response.json()
        if commit_data:
            commit_time = commit_data[0]['commit']['committer']['date']
            return commit_time
        else:
            return None
    else:
        return None

def doc_chinexe_support_git(url,version):
    '''Check if the specified folder contains documents with Chinese characters'''
    repo_name = os.path.basename(url)+"-"+version
    
    if repo_name not in os.listdir(REPOPATH):
        print(f"Cloning {repo_name} repository...")
        clone_repo(url,version)
        
    
    
    json_path = os.path.join(JSON_REPOPATH, f"{repo_name}.json")

    if f"{repo_name}.json" not in os.listdir(JSON_REPOPATH):
        return ValueError(f"Start by performing the document quantity metric...")

    zh_files = find_zh_files(json_path,url)
    return zh_files


if __name__ == "__main__":
    # clone_repo("https://github.com/git-lfs/git-lfs")
    print(doc_chinexe_support_git("https://github.com/git-lfs/git-lfs",'v2.7.2'))
    # # url = r"C:\Users\zyx\Desktop\文档数量\folder_document_details.json"
    # # file_path = r"C:\Users\zyx\Desktop\文档数量\test.md"
    # # doc_chinese_support(file_path)
    # url = "https://github.com/numpy/numpy"
    # # doc_chinexe_support_git(url)
    # save_json(doc_chinexe_support_git(url), "文档支持中文.json")
    # # print(find_zh_files(url))
    # # print(get_file_commit_time(url, "README.md"))