import os
import pandas as pd
import subprocess
from pydriller import Repository

# --- List of all projects ---
projects = [
    "jdt", "platform", "gerrit", "go", "qt", "openstack"
]

base_dir = "G:/defect-prediction-project/datasets"

def get_git_diff(repo_path, commit_hash):
    try:
        result = subprocess.run(
            ["git", "show", "--pretty=", commit_hash],
            cwd=repo_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            encoding="utf-8",  # Force UTF-8
            errors="replace",  # Replace problematic characters
            timeout=10
        )
        if result.stdout:
            return result.stdout.strip()
        else:
            return ""
    except Exception as e:
        print(f" git show failed for {commit_hash[:8]}: {e}")
        return ""


# --- Loop through projects ---
for project in projects:
    print(f"\n Processing project: {project}")

    repo_root = os.path.join(base_dir, "cloned_repos", project)
    csv_path = os.path.join(base_dir, "processed_data", f"enriched_{project}.csv")
    output_path = os.path.join(base_dir, "finetune_data", f"train_openllama_{project}.txt")

    # Load enriched CSV
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f" CSV not found for {project}, skipping.")
        continue

    df['_id'] = df['_id'].astype(str).str.lower()
    df['commit_message'] = df['commit_message'].fillna("No commit message")
    commit_map = dict(zip(df['_id'], zip(df['commit_message'], df['bug'])))

    seen_commits = set()
    train_blocks = []

    # Traverse all sub-repos
    for subdir in os.listdir(repo_root):
        repo_path = os.path.join(repo_root, subdir)
        if not os.path.isdir(repo_path) or not os.path.exists(os.path.join(repo_path, ".git")):
            continue

        print(f" Searching in: {subdir}")
        try:
            for commit in Repository(repo_path).traverse_commits():
                chash = commit.hash.lower()
                if chash in commit_map and chash not in seen_commits:
                    seen_commits.add(chash)
                    msg, label = commit_map[chash]
                    msg = msg.replace("\n", " ").strip()

                    diff = get_git_diff(repo_path, chash)
                    if not diff:
                        print(f" Empty diff for {chash[:8]} — skipping")
                        continue

                    block = f"""[DEFECT]
Title: {msg}
Diff:
{diff}
[/DEFECT]
{int(label)}"""

                    train_blocks.append(block)
        except Exception as e:
            print(f" Failed processing {repo_path}: {e}")

    # Write to file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(train_blocks))

    print(f"Saved {len(train_blocks)} samples to: {output_path}")
