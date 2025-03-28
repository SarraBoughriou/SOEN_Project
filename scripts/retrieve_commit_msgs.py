import pandas as pd
from pydriller import Repository
import os

#  Define all project paths
projects = [
    {
        "name": "jdt",
        "csv": "G:/defect-prediction-project/datasets/data/jdt/jdt.csv",
        "repo_urls": r"G:\defect-prediction-project\external\ISSTA21-JIT-DP\Data_Extraction\git_base\git_datasets\jdt\repo_urls",
        "clone_base": r"G:/defect-prediction-project/datasets/cloned_repos/jdt",
        "output_csv": "G:/defect-prediction-project/datasets/processed_data/enriched_jdt.csv"
    },
    {
        "name": "platform",
        "csv": "G:/defect-prediction-project/datasets/data/platform/platform.csv",
        "repo_urls": r"G:\defect-prediction-project\external\ISSTA21-JIT-DP\Data_Extraction\git_base\git_datasets\platform\repo_urls",
        "clone_base": r"G:/defect-prediction-project/datasets/cloned_repos/platform",
        "output_csv": "G:/defect-prediction-project/datasets/processed_data/enriched_platform.csv"
    },
    {
        "name": "gerrit",
        "csv": "G:/defect-prediction-project/datasets/data/gerrit/gerrit.csv",
        "repo_urls": r"G:\defect-prediction-project\external\ISSTA21-JIT-DP\Data_Extraction\git_base\git_datasets\gerrit\repo_urls",
        "clone_base": r"G:/defect-prediction-project/datasets/cloned_repos/gerrit",
        "output_csv": "G:/defect-prediction-project/datasets/processed_data/enriched_gerrit.csv"
    },
    {
        "name": "go",
        "csv": "G:/defect-prediction-project/datasets/data/go/go.csv",
        "repo_urls": r"G:\defect-prediction-project\external\ISSTA21-JIT-DP\Data_Extraction\git_base\git_datasets\go\repo_urls",
        "clone_base": r"G:/defect-prediction-project/datasets/cloned_repos/go",
        "output_csv": "G:/defect-prediction-project/datasets/processed_data/enriched_go.csv"
    },
    {
        "name": "qt",
        "csv": "G:/defect-prediction-project/datasets/data/qt/qt.csv",
        "repo_urls": r"G:\defect-prediction-project\external\ISSTA21-JIT-DP\Data_Extraction\git_base\git_datasets\qt\repo_urls",
        "clone_base": r"G:/defect-prediction-project/datasets/cloned_repos/qt",
        "output_csv": "G:/defect-prediction-project/datasets/processed_data/enriched_qt.csv"
    },
    {
        "name": "openstack",
        "csv": r"G:\defect-prediction-project\external\ISSTA21-JIT-DP\Data_Extraction\git_base\commits_csv\openstack.csv",
        "repo_urls": r"G:\defect-prediction-project\external\ISSTA21-JIT-DP\Data_Extraction\git_base\git_datasets\openstack\repo_urls",
        "clone_base": r"G:/defect-prediction-project/datasets/cloned_repos/openstack",
        "output_csv": "G:/defect-prediction-project/datasets/processed_data/enriched_openstack.csv"
    }
]

#  Process each project
for project in projects:
    print(f"\n Processing project: {project['name']}")

    # Load commit list
    df = pd.read_csv(project["csv"])
    commit_ids = set(df['_id'])

    # Read all repo URLs
    with open(project["repo_urls"], 'r') as f:
        repo_urls = [line.strip().replace("git clone ", "") for line in f if line.strip()]

    commit_data = []

    for repo_url in repo_urls:
        repo_name = repo_url.split("/")[-1]
        local_path = os.path.join(project["clone_base"], repo_name)

        # Clone if not already done
        if not os.path.exists(local_path):
            print(f" Cloning {repo_url}")
            os.system(f"git clone --depth=100000 --no-single-branch {repo_url} \"{local_path}\"")
        else:
            print(f" Repo already exists: {repo_name}")

        # Traverse commits in this repo
        try:
            for commit in Repository(local_path).traverse_commits():
                if commit.hash in commit_ids:
                    commit_data.append({
                        '_id': commit.hash,
                        'commit_message': commit.msg,
                    })
        except Exception as e:
            print(f" Failed processing {repo_url}: {e}")

    # Merge & Save
    commit_df = pd.DataFrame(commit_data)
    df_merged = df.merge(commit_df, on='_id', how='left')
    df_merged.to_csv(project["output_csv"], index=False)
    print(f" Saved enriched dataset to: {project['output_csv']}")
