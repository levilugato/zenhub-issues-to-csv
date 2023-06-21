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
query ($pipelineId: ID!, $workspaceId: ID!, $filters: IssueSearchFiltersInput!, $after: String) {
  searchIssuesByPipeline(pipelineId: $pipelineId, filters: $filters, after: $after) {
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      id
      title
      number
      body
      state
      createdAt
      updatedAt
      repository {
        name
        id
      }
      user {
        login
      }
      pipelineIssue(workspaceId: $workspaceId) {
        pipeline {
          id
          name
        }
      }
      parentZenhubEpics(first: 10) {
        nodes {
          title
        }
      }
      assignees(first: 10) {
        nodes {
          name
        }
      }
      estimate {
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
          name
        }
      }
    }
  }
}
'''

def set_payload(query, variables):
    return {
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

try:
    response_pipelines = requests.post(url, headers=headers, json=set_payload(query_pipelines, variables))
    data_pipelines = response_pipelines.json()

    pipeline_ids = [node['id'] for node in data_pipelines['data']['workspace']['pipelinesConnection']['nodes']]

    for pipeline_id in pipeline_ids:
        variables = {
            "workspaceId": workspace_id,
            "pipelineId": pipeline_id,
            "filters": {},
            "after": None
        }

        page_counter = 1  # Counter for generating unique filenames

        while True:
            response = requests.post(url, headers=headers, json=set_payload(query_issues, variables))
            try:
                data = json.loads(response.content)

                nodes = data['data']['searchIssuesByPipeline']['nodes']

                if not nodes:
                    print(f"No issues found for pipeline ID: {pipeline_id}")
                    break

                pipelineName = data['data']['searchIssuesByPipeline']['nodes'][0]['pipelineIssue']['pipeline']['name']
                pipelineName = pipelineName.replace('Bugs/', 'Bugs-')

                df = pd.json_normalize(nodes)
                df['labels_name'] = df['labels.nodes'].apply(lambda x: ', '.join([label['name'] for label in x]))
                df.drop(columns=['labels.nodes'], inplace=True)

                output_file = output_dir + f'{pipelineName}_{page_counter}.csv'  # Include page counter in the filename
                df.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)
                print(f'Saved result to {output_file}')

                pageInfo = data['data']['searchIssuesByPipeline']['pageInfo']
                if pageInfo['hasNextPage']:
                    variables['after'] = pageInfo['endCursor']
                    page_counter += 1  # Increment the page counter
                else:
                    break

            except (KeyError, json.JSONDecodeError) as e:
                print(f"Error processing data for pipeline ID: {pipeline_id}")
                print(e)
                break

except requests.RequestException as e:
    print("Error occurred during the API request.")
    print(e)
