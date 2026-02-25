import asyncio
import aiohttp
import pandas as pd
import sys
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
import calendar

BASE_URL="https://api.github.com"

Type = "github-api"
## ensuring if the error.log file exists or not
if not os.path.exists(f'./logs/error/error-{Type}-{datetime.now().strftime("%Y-%m-%d %H:%M")}.log'):
    with open(f'./logs/error/error-{Type}-{datetime.now().strftime("%Y-%m-%d %H:%M")}.log', 'w'):
        pass

if not os.path.exists(f'./logs/logs-{Type}-{datetime.now().strftime("%Y-%m-%d %H:%M")}.log'):
    with open(f'./logs/logs-{Type}-{datetime.now().strftime("%Y-%m-%d %H:%M")}.log', 'w'):
        pass

## configure logging file
error_logger = logging.getLogger('error_logger')
error_handler = logging.FileHandler(f'./logs/error/error-{Type}-{datetime.now().strftime("%Y-%m-%d %H:%M")}.log')
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

debug_logger = logging.getLogger('debug_logger')
debug_handler = logging.FileHandler(f'./logs/logs-{Type}-{datetime.now().strftime("%Y-%m-%d %H:%M")}.log')
debug_handler.setLevel(logging.DEBUG)
debug_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
debug_handler.setFormatter(debug_formatter)
debug_logger.addHandler(debug_handler)
debug_logger.setLevel(logging.DEBUG)


async def get_user_commits(session, config, user):
    """Fetch the commits by a user in a repository."""
    commits = []
    url = f"{BASE_URL}/repos/{config['ORG_NAME']}/{config['REPO_NAME']}/commits"
    params = {
        "author": user,
        "since": config["SINCE"],
        "until": config["UNTIL"],
        "per_page": 100  # Get more results per page
    }
    print(f"[INFO] Fetching commits for user {user} in repository {config['REPO_NAME']}")
    debug_logger.debug(f'[INFO] Fetching commits for user {user} in repository {config["REPO_NAME"]}')
    
    async with session.get(url, headers=config['HEADERS'], params=params) as response:
        df = pd.DataFrame()
        
        if response.status == 200:
            try:
                commits = await response.json()
            except Exception as e:
                print(f"[ERROR] Failed to parse JSON response for {user}: {e}")
                error_logger.error(f"Failed to parse JSON response for {user}: {e}")
                return df
            
            # Check if response is a list
            if not isinstance(commits, list):
                print(f"[ERROR] Unexpected API response format for {user}")
                error_logger.error(f"Unexpected API response format for {user}: {commits}")
                return df
                
            if commits == []:
                print(f"[WARNING] {user} may not a contributor to the repository {config['REPO_NAME']}")
                debug_logger.debug(f'[WARNING] {user} may not a contributor to the repository {config["REPO_NAME"]}')   
                return df
                
            for commit in commits:
                try:
                    # Safely extract commit data with fallbacks
                    commit_data = {
                        'organization_name': config['ORG_NAME'],
                        'repository_name': config['REPO_NAME'],
                        'user': user,
                        'commit_sha': commit.get('sha', 'N/A'),
                        'commit_date': commit.get('commit', {}).get('author', {}).get('date', 'N/A'),
                        'commit_message': commit.get('commit', {}).get('message', 'N/A')
                    }
                    n_df = pd.DataFrame([commit_data])
                    df = pd.concat([df, n_df], ignore_index=True)
                except Exception as e:
                    print(f"[WARNING] Failed to parse commit data for {user}: {e}")
                    error_logger.error(f"Failed to parse commit for {user}: {e}")
                    continue
                    
            print(f"[SUCCESS] Commits fetched for user {user} in repository {config['REPO_NAME']}")
            debug_logger.debug(f'[SUCCESS] Commits fetched for user {user} in repository {config["REPO_NAME"]}')
            return df
        elif response.status == 404:
            print(f"[WARNING] Repository not found or user {user} has no access")
            debug_logger.debug(f'[WARNING] Repository not found or user {user} has no access')
            return df
        elif response.status == 403:
            error_text = await response.text()
            print(f"[ERROR] API rate limit exceeded or forbidden: {error_text}")
            error_logger.error(f"API rate limit exceeded or forbidden for {user}: {error_text}")
            return df
        else:
            error_text = await response.text()
            print(f"[ERROR] API returned status {response.status} for {user}")
            error_logger.error(f"API error for {user}: {response.status} - {error_text}")
            return df

async def audit_commits(session, config, users):
    """Audit Commits By username in a repository to a csv file (sequential)."""
    df = pd.DataFrame()

    for user in users:
        try:
            commits = await get_user_commits(session, config, user)
            df = pd.concat([df, commits], ignore_index=True)
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[ERROR] Failed to fetch commits for user {user} in repository {config['REPO_NAME']}")
            error_logger.error(
                f"Error occurred while fetching commits for user {user} in repository {config['REPO_NAME']}: {e}"
            )
            continue

    df.drop_duplicates(inplace=True)
    return df


# Save the DataFrame to a CSV file with custom text
def save_csv_with_meta_info(dataframe, filename, meta_info):
    # Open the file in write mode
    with open(filename, 'w') as f:
        # Write the custom text first
        f.write(meta_info)
        
        # Now write the DataFrame to the file
        dataframe.to_csv(f, index=False)

async def process_repository(session, repo, config, usernames):
    """Process a single repository asynchronously"""
    try:    
        # Skip empty lines
        if not repo or not repo.strip():
            print(f"[WARNING] Skipping empty repository line")
            return pd.DataFrame()
        
        repo = repo.strip()  # Remove whitespace
        
        # extracting organization name and repository name from the repository link
        parts = repo.split("/")
        if len(parts) < 2:
            print(f"[ERROR] Invalid repository URL format: {repo}")
            error_logger.error(f"Invalid repository URL format: {repo}")
            return pd.DataFrame()
        
        ORG_NAME = parts[-2]
        REPO_NAME = parts[-1].replace(".git", "")  # Handle .git extension
        
        # Validate extracted values
        if not ORG_NAME or not REPO_NAME:
            print(f"[ERROR] Could not extract org/repo from: {repo}")
            error_logger.error(f"Could not extract org/repo from: {repo}")
            return pd.DataFrame()
        
        # adding them to config
        repo_config = config.copy()
        repo_config["ORG_NAME"] = ORG_NAME
        repo_config["REPO_NAME"] = REPO_NAME

        # get audit for repo
        df = await audit_commits(session, repo_config, usernames)
        return df

    except Exception as e:
        print(f"[ERROR] Failed to audit repository {repo}: {e}")
        error_logger.error(f"Error occurred while auditing repository {repo}: {e}")
        return pd.DataFrame()

async def main():
    
    required_args = 8  
    if len(sys.argv) < required_args:
        print(f"[ERROR] Insufficient arguments. Provided: {len(sys.argv)-1}, Required: {required_args-1}")
        print(f"Usage: python {sys.argv[0]} USERNAMES MONTH_START MONTH_END TEAM_NAME IS_PERIOD PERIOD APPLICATION_NAME")
        print(f"Example: python {sys.argv[0]} 'user1 user2' 2024-01 2024-12 TeamName 0 6 AppName")
        error_logger.error(f"[ERROR] Insufficient arguments. Provided: {len(sys.argv)-1}, Required: {required_args-1}")
        sys.exit(1)

    # extracting the username
    usernames_ = sys.argv[1]
    # extracting the start month
    MONTH_START = sys.argv[2]
    # extracting the end month
    MONTH_END = sys.argv[3]
    # extracting the team_name
    TEAM_NAME = sys.argv[4]
    # extracting the IS_PERIOD
    IS_PERIOD = int(sys.argv[5])
    # extracting the period
    PERIOD = int(sys.argv[6])
    print(f"************************* RUNNING monthly-audit.py for {TEAM_NAME} ********************************")
    # creating datetime object
    start_month = datetime.strptime(MONTH_START, '%Y-%m')
    if IS_PERIOD == 1:
        # get the end month in datetime format
        end_month = start_month - relativedelta(months=PERIOD)
        MONTH_END = end_month.strftime('%Y-%m')
        MONTH_START, MONTH_END = MONTH_END, MONTH_START
        start_month, end_month = end_month, start_month
    else:
        # get the end month in datetime format
        end_month = datetime.strptime(MONTH_END, '%Y-%m')
        MONTH_END = end_month.strftime('%Y-%m')
    
    # Split the string into a list 
    usernames = usernames_.split()

    # get the github access token
    TOKEN = os.environ.get('GH_PAT')
    
    if not TOKEN:
        print("[ERROR] GH_PAT environment variable not set")
        error_logger.error("[ERROR] GH_PAT environment variable not set")
        sys.exit(1)
    
    # get the APPLICATION_NAME
    APPLICATION_NAME = sys.argv[7]

    # defining the header for REST-API get request
    HEADERS = {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {TOKEN}",
                "X-GitHub-Api-Version": "2022-11-28"
            }
    
    # extracting the first day and last day of the current month
    _, last_day_of_month = calendar.monthrange(end_month.year, end_month.month)
    SINCE = f"{MONTH_START}-01T00:00:00Z"
    UNTIL = f"{MONTH_END}-{last_day_of_month}T23:59:59Z"

    # config dictionary 
    config = {
        "TOKEN": TOKEN,
        "HEADERS": HEADERS,
        "SINCE": SINCE,
        "UNTIL": UNTIL,
    }

    # Check if repos.txt exists
    if not os.path.exists('./repos.txt'):
        print("[ERROR] repos.txt file not found")
        error_logger.error("[ERROR] repos.txt file not found")
        sys.exit(1)

    with open(f'./repos.txt') as f:
        repos = f.read().splitlines()
    
    # Filter out empty lines
    repos = [repo for repo in repos if repo.strip()]
    
    if not repos:
        print("[ERROR] No repositories found in repos.txt")
        error_logger.error("[ERROR] No repositories found in repos.txt")
        sys.exit(1)

    ## exported data collection
    all_Df = pd.DataFrame()
    print(f"[INFO] Starting audit for {TEAM_NAME} from {MONTH_START} to {MONTH_END}")
    debug_logger.debug(f'[INFO] Starting audit for {TEAM_NAME} from {MONTH_START} to {MONTH_END}')
    
    # Create aiohttp session and process all repositories concurrently
    async with aiohttp.ClientSession() as session:
        for repo in repos:
            df = await process_repository(session, repo, config, usernames)
            all_Df = pd.concat([all_Df, df], ignore_index=True)
    
    # Create audits directory if it doesn't exist
    os.makedirs('./audits', exist_ok=True)
    
    filename = f"./audits/{TEAM_NAME}-{MONTH_START}-to-{MONTH_END}-audit.csv"
    # meta info to be added before the header
    meta_info = f'\n\nAudit Report from {MONTH_START} to {MONTH_END}\n\nTeam_Name:{TEAM_NAME}\n\nApplication: {APPLICATION_NAME}\n\n'
    save_csv_with_meta_info(all_Df, filename, meta_info)
    print(f"[SUCCESS] Audit report generated for {TEAM_NAME} from {MONTH_START} to {MONTH_END} in ./audits/{TEAM_NAME}-{MONTH_START}-to-{MONTH_END}-audit.csv")
    debug_logger.debug(f'[SUCCESS] Audit report generated for {TEAM_NAME} from {MONTH_START} to {MONTH_END} in ./audits/{TEAM_NAME}-{MONTH_START}-to-{MONTH_END}-audit.csv')
    print(f"************************* COMPLETED monthly-audit.py for {TEAM_NAME} ********************************")

if __name__ == "__main__":
    asyncio.run(main())