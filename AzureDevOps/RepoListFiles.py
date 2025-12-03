#!/usr/bin/env python3
"""
Azure DevOps Repository File Finder
Query repositories and list files matching specific patterns
"""

import os
import sys
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
from azure.devops.v7_0.git.models import GitVersionDescriptor
import fnmatch
import argparse


def get_connection(organization_url, personal_access_token):
    """
    Establish connection to Azure DevOps
    
    Args:
        organization_url: URL of your Azure DevOps organization (e.g., https://dev.azure.com/yourorg)
        personal_access_token: PAT with Code (Read) permissions
    """
    credentials = BasicAuthentication('', personal_access_token)
    connection = Connection(base_url=organization_url, creds=credentials)
    return connection


def get_all_items_recursive(git_client, repository_id, project_id, path="/", version_descriptor=None):
    """
    Recursively get all items (files and folders) in a repository
    
    Args:
        git_client: Git client from Azure DevOps connection
        repository_id: Repository ID
        project_id: Project ID
        path: Path to search (default is root)
        version_descriptor: Branch/tag/commit to query (default is default branch)
    
    Returns:
        List of all file items with their paths
    """
    all_files = []
    
    try:
        items = git_client.get_items(
            repository_id=repository_id,
            project=project_id,
            scope_path=path,
            recursion_level="OneLevel",
            version_descriptor=version_descriptor
        )
        
        for item in items:
            if item.git_object_type == "blob":  # It's a file
                all_files.append({
                    'path': item.path,
                    'name': item.path.split('/')[-1],
                    'size': item.size,
                    'url': item.url
                })
            elif item.git_object_type == "tree" and item.path != path:  # It's a folder
                # Recursively get items from subfolder
                subfolder_files = get_all_items_recursive(
                    git_client, repository_id, project_id, item.path, version_descriptor
                )
                all_files.extend(subfolder_files)
    
    except Exception as e:
        print(f"Error processing path {path}: {str(e)}", file=sys.stderr)
    
    return all_files


def filter_files(files, patterns):
    """
    Filter files based on file patterns (e.g., *.py, *.json)
    
    Args:
        files: List of file dictionaries
        patterns: List of file patterns to match (supports wildcards)
    
    Returns:
        Filtered list of files
    """
    if not patterns:
        return files
    
    filtered = []
    for file in files:
        filename = file['name']
        for pattern in patterns:
            if fnmatch.fnmatch(filename, pattern):
                filtered.append(file)
                break
    
    return filtered


def main():
    parser = argparse.ArgumentParser(
        description='List files in Azure DevOps repositories matching specific patterns'
    )
    parser.add_argument(
        '--org-url',
        required=True,
        help='Azure DevOps organization URL (e.g., https://dev.azure.com/yourorg)'
    )
    parser.add_argument(
        '--pat',
        help='Personal Access Token (or set AZURE_DEVOPS_PAT environment variable)'
    )
    parser.add_argument(
        '--project',
        required=True,
        help='Project name'
    )
    parser.add_argument(
        '--repository',
        help='Repository name (if not specified, searches all repositories)'
    )
    parser.add_argument(
        '--branch',
        default='main',
        help='Branch name (default: main)'
    )
    parser.add_argument(
        '--patterns',
        nargs='+',
        help='File patterns to match (e.g., *.py *.json config.*)'
    )
    parser.add_argument(
        '--output',
        choices=['simple', 'detailed', 'csv'],
        default='simple',
        help='Output format (default: simple)'
    )
    parser.add_argument(
        '--path',
        default='/',
        help='Starting path in repository (default: /)'
    )
    
    args = parser.parse_args()
    
    # Get PAT from argument or environment variable
    pat = args.pat or os.environ.get('AZURE_DEVOPS_PAT')
    if not pat:
        print("Error: Personal Access Token required. Provide via --pat or AZURE_DEVOPS_PAT environment variable")
        sys.exit(1)
    
    try:
        # Connect to Azure DevOps
        print(f"Connecting to {args.org_url}...", file=sys.stderr)
        connection = get_connection(args.org_url, pat)
        
        # Get Git client
        git_client = connection.clients.get_git_client()
        
        # Get project
        core_client = connection.clients.get_core_client()
        project = core_client.get_project(args.project)
        
        print(f"Project: {project.name}", file=sys.stderr)
        
        # Get repositories
        repositories = git_client.get_repositories(project.id)
        
        # Filter to specific repository if requested
        if args.repository:
            repositories = [r for r in repositories if r.name == args.repository]
            if not repositories:
                print(f"Error: Repository '{args.repository}' not found")
                sys.exit(1)
        
        print(f"Searching {len(repositories)} repository/repositories...\n", file=sys.stderr)
        
        # Version descriptor for branch
        version_descriptor = GitVersionDescriptor(
            version=args.branch,
            version_type="branch"
        )
        
        all_results = []
        
        # Search each repository
        for repo in repositories:
            print(f"Scanning repository: {repo.name}...", file=sys.stderr)
            
            try:
                # Get all files recursively
                files = get_all_items_recursive(
                    git_client,
                    repo.id,
                    project.id,
                    args.path,
                    version_descriptor
                )
                
                # Filter by patterns if specified
                if args.patterns:
                    files = filter_files(files, args.patterns)
                
                # Add repository name to results
                for file in files:
                    file['repository'] = repo.name
                    all_results.append(file)
                
                print(f"  Found {len(files)} matching file(s)", file=sys.stderr)
            
            except Exception as e:
                print(f"  Error scanning repository: {str(e)}", file=sys.stderr)
        
        # Output results
        print(f"\n{'='*80}\n", file=sys.stderr)
        print(f"Total files found: {len(all_results)}\n", file=sys.stderr)
        
        if args.output == 'simple':
            for file in all_results:
                print(f"{file['repository']}: {file['path']}")
        
        elif args.output == 'detailed':
            for file in all_results:
                print(f"Repository: {file['repository']}")
                print(f"Path:       {file['path']}")
                print(f"Name:       {file['name']}")
                print(f"Size:       {file['size']} bytes")
                print(f"URL:        {file['url']}")
                print("-" * 80)
        
        elif args.output == 'csv':
            print("Repository,Path,Name,Size,URL")
            for file in all_results:
                print(f"{file['repository']},{file['path']},{file['name']},{file['size']},{file['url']}")
        
        return 0
    
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())