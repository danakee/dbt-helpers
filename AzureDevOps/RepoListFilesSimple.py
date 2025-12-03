#!/usr/bin/env python3
"""
Simple example of using the Azure DevOps file finder
Customize the variables at the top to match your environment
"""

import os
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication

# CONFIGURE THESE
ORGANIZATION_URL = "https://dev.azure.com/yourorg"  # Your Azure DevOps URL
PROJECT_NAME = "YourProject"                         # Your project name
REPOSITORY_NAME = "YourRepo"                         # Your repository name (optional)
BRANCH_NAME = "main"                                 # Branch to search
FILE_PATTERNS = ["*.py", "*.json"]                   # Files to find

# Get PAT from environment variable
PAT = os.environ.get('AZURE_DEVOPS_PAT')
if not PAT:
    print("Please set AZURE_DEVOPS_PAT environment variable")
    exit(1)


def main():
    # Connect to Azure DevOps
    credentials = BasicAuthentication('', PAT)
    connection = Connection(base_url=ORGANIZATION_URL, creds=credentials)
    
    # Get clients
    git_client = connection.clients.get_git_client()
    core_client = connection.clients.get_core_client()
    
    # Get project
    project = core_client.get_project(PROJECT_NAME)
    print(f"Project: {project.name}")
    
    # Get repositories
    repositories = git_client.get_repositories(project.id)
    
    # Filter to specific repo if needed
    if REPOSITORY_NAME:
        repositories = [r for r in repositories if r.name == REPOSITORY_NAME]
    
    print(f"Searching {len(repositories)} repository/repositories...\n")
    
    # Search each repository
    for repo in repositories:
        print(f"\n=== Repository: {repo.name} ===")
        
        # Get items from repository
        items = git_client.get_items(
            repository_id=repo.id,
            project=project.id,
            recursion_level="Full",  # Get all items recursively
            version_descriptor={
                "version": BRANCH_NAME,
                "version_type": "branch"
            }
        )
        
        # Filter and display matching files
        matching_files = []
        for item in items:
            if item.git_object_type == "blob":  # It's a file
                filename = item.path.split('/')[-1]
                
                # Check if file matches any pattern
                for pattern in FILE_PATTERNS:
                    if pattern.startswith('*.'):
                        extension = pattern[1:]  # Get extension with dot
                        if filename.endswith(extension):
                            matching_files.append(item.path)
                            break
                    elif pattern in filename:
                        matching_files.append(item.path)
                        break
        
        # Display results
        if matching_files:
            for filepath in matching_files:
                print(f"  {filepath}")
            print(f"\nFound {len(matching_files)} matching file(s)")
        else:
            print("  No matching files found")


if __name__ == "__main__":
    main()