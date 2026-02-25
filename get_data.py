import asyncio
import aiohttp
import sys
import os
from datetime import datetime
import logging


BASE_URL="http://10.100.6.201:8000/api/git"

Type = "api-git-internal-api"

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


def formatUsers(team_users):
    results = []
    for key, array in team_users.items():
        logins = " ".join(item["login"] for item in array)
        results.append(f"{key}={logins}")
    return results

async def getUserList(session, org_name, team_id):
    try:
        url = f"{BASE_URL}/sod-user-list"
        params = {
            "org_name":org_name,
            "team_id":team_id
        }
        print(f"[INFO] Fetching users of a team {team_id} in org {org_name}")
        debug_logger.debug(f'[INFO] Fetching user-lists {org_name} {team_id}')
        
        async with session.get(url, params=params) as response:
            if response.status == 200:
                results = await response.json()
                team_users = formatUsers(results)
                with open(f"./team_users.txt", 'w') as file:
                    file.write("\n".join(team_users) + '\n')
                return True
            else:
                raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"HTTP {response.status}"
                    )
    except Exception as e:
            print(f"[error] failed to fetch users of a team {team_id} in org {org_name}")
            error_logger.error(f"Error occurred while fetching users from org {org_name} in team_id {team_id}: {e}")
            return False
    
async def getRepoDetails(session, org_name, repo_name):
    try:
        url = f"{BASE_URL}/repo-details"
        params = {
            "org_name":org_name,
            "repo_name":repo_name,
        }
        print(f"[INFO] Fetching repo-details of org {org_name} repo {repo_name}")
        debug_logger.debug(f'[INFO] Fetching repo-details {org_name} {repo_name}')
        
        async with session.get(url, params=params) as response:
            if response.status == 200:
                repoDetails = await response.json()
                return repoDetails
            else:
                raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"HTTP {response.status}"
                    )
    except Exception as e:
            print(f"[error] failed to fetch repo-details of org {org_name} repo {repo_name}")
            error_logger.error(f"Error occurred while fetching repo-details from org {org_name} repo {repo_name} : {e}")
            return None  

def isAuditNeeded(repo_details, application_name):
       audit_value = repo_details.get('custom_properties', {}).get('Audit').lower()
       app_value = repo_details.get('custom_properties', {}).get('Application').lower()
       application_name=application_name.lower()
       return ((audit_value in ['yes','sox']) and app_value == application_name)

async def getFilteredRepositories(session, repos, org_name, application_name):
    tasks = [getRepoDetails(session, org_name, repo['name']) for repo in repos]
    repo_details_list = await asyncio.gather(*tasks, return_exceptions=True)

    repoLinks = []
    for i, repoDetails in enumerate(repo_details_list):
        # Skip if exception occurred or None returned
        if isinstance(repoDetails, Exception) or repoDetails is None:
            error_logger.error(f"Skipping repo {repos[i]['name']} due to error in fetching details.")
            continue
            
        if isAuditNeeded(repoDetails, application_name):
            repoLinks.append(f'{repos[i]["html_url"]}.git')
    
    return repoLinks

async def getRepoList(session, org_name, application_name):
    try:
        url = f"{BASE_URL}/repo-list"
        params = {
            "org_name":org_name
        }
        print(f"[INFO] Fetching repos of org {org_name} for application {application_name}")
        debug_logger.debug(f'[INFO] Fetching repo-lists {org_name} for application {application_name}')
        
        async with session.get(url, params=params) as response:
            if response.status == 200:
                repos = await response.json()
                repoLinks = await getFilteredRepositories(session, repos, org_name, application_name)
                with open(f"./repos.txt", 'w') as file:
                    file.write("\n".join(repoLinks) + '\n')
                return True
            else:
                raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"HTTP {response.status}"
                    )
    except Exception as e:
            print(f"[error] failed to fetch repos of org {org_name}")
            error_logger.error(f"Error occurred while fetching repos from org {org_name} : {e}")
            return False
        

async def main():
    print(f"************************* RUNNING get-data.py ********************************")
    # Get the usernames passed as an argument
    if len(sys.argv) < 4:
        print(f"[ERROR] program argument not provided expected two argument 1st ORG_NAME 2nd TEAM_ID 3rd APPLICATION_NAME")
        error_logger.error(f"[ERROR] program argument not provided expected two argument 1st ORG_NAME 2nd TEAM_ID 3rd APPLICATION_NAME")
        sys.exit(1)

    # extracting the orgName
    orgName = sys.argv[1]
    # extracting the teamId
    teamId = sys.argv[2]
    ## extracting the applicationName
    applicationName = sys.argv[3]

    print(f"[INFO] Fetching data for org {orgName} and team {teamId} and application {applicationName}")
     # Create aiohttp session and run both tasks concurrently
    async with aiohttp.ClientSession() as session:
        # Create tasks for both operations
        repo_task = getRepoList(session, orgName, applicationName)
        user_task = getUserList(session, orgName, teamId)
        
        # Gather both tasks concurrently
        repo_result, user_result = await asyncio.gather(repo_task, user_task)
        
        if repo_result == True:
            print(f"[SUCCESS] repos.txt file successfully populated with filtered data")
            debug_logger.debug(f'[SUCCESS] repos.txt file successfully populated with filtered data')
        if user_result == True:
            print(f"[SUCCESS] team_users.txt file successfully populated with users data")
            debug_logger.debug(f'[SUCCESS] team_users.txt file successfully populated with users data')
    print(f"************************* COMPLETED get-data.py ********************************")
        

if __name__ == "__main__":
    asyncio.run(main())