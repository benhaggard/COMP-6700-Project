"""
AIDev Project - Tasks 1-5
Minimal implementation to complete the project requirements
"""

import pandas as pd
import re
from datasets import load_dataset

# ============================================================================
# TASK 1: Extract Pull Request Data
# ============================================================================
print("Task 1: Extracting Pull Request Data...")
dataset = load_dataset("hao-li/AIDev", "all_pull_request")
all_pr_df = dataset['train'].to_pandas()

task1_df = pd.DataFrame({
    'TITLE': all_pr_df['title'],
    'ID': all_pr_df['id'],
    'AGENTNAME': all_pr_df['agent'],
    'BODYSTRING': all_pr_df['body'],
    'REPOID': all_pr_df['repo_id'],
    'REPOURL': all_pr_df['repo_url']
})
task1_df.to_csv('task1_output.csv', index=False, encoding='utf-8')
print(f"  Saved: {len(task1_df)} rows")

# ============================================================================
# TASK 2: Extract Repository Data
# ============================================================================
print("\nTask 2: Extracting Repository Data...")
dataset = load_dataset("hao-li/AIDev", "all_repository")
all_repo_df = dataset['train'].to_pandas()

task2_df = pd.DataFrame({
    'REPOID': all_repo_df['id'],
    'LANG': all_repo_df['language'],
    'STARS': all_repo_df['stars'],
    'REPOURL': all_repo_df['url']
})
task2_df.to_csv('task2_output.csv', index=False, encoding='utf-8')
print(f"  Saved: {len(task2_df)} rows")

# ============================================================================
# TASK 3: Extract PR Task Type Data
# ============================================================================
print("\nTask 3: Extracting PR Task Type Data...")
dataset = load_dataset("hao-li/AIDev", "pr_task_type")
pr_task_type_df = dataset['train'].to_pandas()

task3_df = pd.DataFrame({
    'PRID': pr_task_type_df['id'],
    'PRTITLE': pr_task_type_df['title'],
    'PRREASON': pr_task_type_df['reason'],
    'PRTYPE': pr_task_type_df['type'],
    'CONFIDENCE': pr_task_type_df['confidence']
})
task3_df.to_csv('task3_output.csv', index=False, encoding='utf-8')
print(f"  Saved: {len(task3_df)} rows")

# ============================================================================
# TASK 4: Extract PR Commit Details
# ============================================================================
print("\nTask 4: Extracting PR Commit Details...")
dataset = load_dataset("hao-li/AIDev", "pr_commit_details")
pr_commit_df = dataset['train'].to_pandas()

def clean_special_chars(text):
    if pd.isna(text):
        return ''
    text = str(text)
    return re.sub(r'[^\x20-\x7E\n\r\t]', '', text)

task4_df = pd.DataFrame({
    'PRID': pr_commit_df['pr_id'],
    'PRSHA': pr_commit_df['sha'],
    'PRCOMMITMESSAGE': pr_commit_df['message'],
    'PRFILE': pr_commit_df['filename'],
    'PRSTATUS': pr_commit_df['status'],
    'PRADDS': pr_commit_df['additions'],
    'PRDELSS': pr_commit_df['deletions'],
    'PRCHANGECOUNT': pr_commit_df['changes'],
    'PRDIFF': pr_commit_df['patch'].apply(clean_special_chars)
})
task4_df.to_csv('task4_output.csv', index=False, encoding='utf-8')
print(f"  Saved: {len(task4_df)} rows")

# ============================================================================
# TASK 5: Security Keyword Analysis
# ============================================================================
print("\nTask 5: Performing Security Keyword Analysis...")
SECURITY_KEYWORDS = [
    'race', 'racy', 'buffer', 'overflow', 'stack', 'integer', 'signedness',
    'underflow', 'improper', 'unauthenticated', 'gain access', 'permission',
    'cross site', 'css', 'xss', 'denial service', 'dos', 'crash', 'deadlock',
    'injection', 'request forgery', 'csrf', 'xsrf', 'forged', 'security',
    'vulnerability', 'vulnerable', 'exploit', 'attack', 'bypass', 'backdoor',
    'threat', 'expose', 'breach', 'violate', 'fatal', 'blacklist', 'overrun',
    'insecure'
]

def contains_security_keyword(text):
    if pd.isna(text):
        return 0
    text_lower = str(text).lower()
    for keyword in SECURITY_KEYWORDS:
        if ' ' in keyword:
            if keyword in text_lower:
                return 1
        else:
            if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                return 1
    return 0

# Merge Task 1 and Task 3 data
merged_df = pd.merge(
    task1_df[['ID', 'AGENTNAME', 'TITLE', 'BODYSTRING']], 
    task3_df[['PRID', 'PRTYPE', 'CONFIDENCE']], 
    left_on='ID', 
    right_on='PRID', 
    how='left'
)

# Fill missing values
merged_df['PRTYPE'] = merged_df['PRTYPE'].fillna('Unknown')
merged_df['CONFIDENCE'] = merged_df['CONFIDENCE'].fillna(0)

# Check security keywords
merged_df['SECURITY'] = merged_df.apply(
    lambda row: max(
        contains_security_keyword(row['TITLE']),
        contains_security_keyword(row['BODYSTRING'])
    ),
    axis=1
)

# Create final output
task5_df = pd.DataFrame({
    'ID': merged_df['ID'],
    'AGENT': merged_df['AGENTNAME'],
    'TYPE': merged_df['PRTYPE'],
    'CONFIDENCE': merged_df['CONFIDENCE'],
    'SECURITY': merged_df['SECURITY']
})
task5_df.to_csv('task5_output.csv', index=False, encoding='utf-8')

print(f"  Saved: {len(task5_df)} rows")
print(f"  Security flagged: {task5_df['SECURITY'].sum()} PRs")

print("\n" + "="*60)
print("ALL TASKS COMPLETED")
print("Generated: task1-5_output.csv files")
print("="*60)
