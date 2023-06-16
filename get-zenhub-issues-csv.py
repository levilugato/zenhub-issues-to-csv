import csv
import requests
import pandas as pd

# Set your workspace ID and API token
######## CHANGE ME ##################################
token = "your zenhub token"
workspace_id = "your workspace id"
output_dir = 'your/local/folder/to/save/csvs/'
#####################################################

# Define the GraphQL query for pipelines_ids
query_pipelines = '''
query work($workspace_id: ID!) {
  workspace(id: $workspace_id){
    displayName
    pipelinesConnection {
      nodes {
        name
        description
        id   
        }     
      }
    }
  }
'''

# Define the GraphQL query for issues
query_issues = '''
query ($pipelineId: ID!, $workspaceId: ID!, $filters: IssueSearchFiltersInput!) {
    searchIssuesByPipeline(pipelineId: $pipelineId, filters: $filters) {
        nodes {
            id
            title
            number
            body
            state
            createdAt
            updatedAt
            repository{
                name
                id
            }
            user{
                login
            }
            pipelineIssue(workspaceId: $workspaceId) {
                pipeline {
                    id
                    name
                }
            }
            parentZenhubEpics(first:10){
                nodes {
                    title
                }
            }
            assignees(first:10){
                nodes {
                    name
                }
            }
            estimate{
                value
                }
            sprints(first: 10) {
      nodes {
        id
        name
      }
    }
    labels(first: 10) {
      nodes {
        id
        name
        color
      }
    }
        }
    }
}
'''

def set_payload(query):
    return  {
        "query": query,
        "variables": variables
    }

# Set the request URL
url = "https://api.zenhub.com/public/graphql"

# Set the request headers
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}"
}

variables = {
    "workspace_id": workspace_id
}

response_pipelines = requests.post(url, headers=headers, json=set_payload(query_pipelines))
data_pipelines = response_pipelines.json()

pipeline_ids = [node['id'] for node in data_pipelines['data']['workspace']['pipelinesConnection']['nodes']]

for pipeline_id in pipeline_ids:

    print(pipeline_id)

    variables = {
        "workspaceId": workspace_id,
        "pipelineId": pipeline_id,
        "filters": {}
    }

    # Send the GraphQL request
    response = requests.post(url, headers=headers, json=set_payload(query_issues))
    data = response.json()

    try:
        # Extract the nodes from the response data
        nodes = data['data']['searchIssuesByPipeline']['nodes']

        if not nodes:
            print(f"No issues found for pipeline ID: {pipeline_id}")
            continue  # Skip to the next pipeline ID

        pipelineName = data['data']['searchIssuesByPipeline']['nodes'][0]['pipelineIssue']['pipeline']['name']
        
        # Convert the nodes to a DataFrame
        df = pd.json_normalize(nodes)
        output_file = output_dir + f'{pipelineName}.csv'

        # Save the DataFrame as CSV
        df.to_csv(output_file, index=False)

        print(f'Saved result to {output_file}')

    except KeyError:
        print(f"No data found for pipeline ID: {pipeline_id}")
