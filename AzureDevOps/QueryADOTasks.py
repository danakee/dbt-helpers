"""
Azure DevOps Task Query Script
Queries tasks for a given area and iteration, displaying:
- Task ID
- Task Name  
- Original Estimate
- Remaining Work
- Completed Work
"""

from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
from azure.devops.v7_0.work_item_tracking.models import Wiql

# ============================================================================
# CONFIGURATION - Update these values for your environment
# ============================================================================

# Your Azure DevOps organization URL (e.g., "https://dev.azure.com/your-org")
ORGANIZATION_URL = "https://dev.azure.com/YOUR_ORGANIZATION"

# Your Personal Access Token (PAT) - Generate from Azure DevOps User Settings
PERSONAL_ACCESS_TOKEN = "YOUR_PAT_HERE"

# Project name
PROJECT_NAME = "YOUR_PROJECT_NAME"

# Filter criteria
AREA_PATH = "YOUR_PROJECT_NAME\\Area\\SubArea"  # e.g., "MyProject\\Team\\Backend"
ITERATION_PATH = "YOUR_PROJECT_NAME\\Sprint 1"   # e.g., "MyProject\\2024\\Sprint 5"

# ============================================================================
# MAIN LOGIC
# ============================================================================

def query_tasks():
    """Query Azure DevOps for tasks matching area and iteration criteria"""
    
    # Step 1: Authenticate and create connection
    credentials = BasicAuthentication('', PERSONAL_ACCESS_TOKEN)
    connection = Connection(base_url=ORGANIZATION_URL, creds=credentials)
    
    # Step 2: Get Work Item Tracking client
    wit_client = connection.clients.get_work_item_tracking_client()
    
    # Step 3: Build WIQL query
    # WIQL = Work Item Query Language (similar to SQL)
    wiql_query = f"""
    SELECT
        [System.Id],
        [System.Title],
        [Microsoft.VSTS.Scheduling.OriginalEstimate],
        [Microsoft.VSTS.Scheduling.RemainingWork],
        [Microsoft.VSTS.Scheduling.CompletedWork]
    FROM 
        WorkItems
    WHERE
        [System.WorkItemType] = 'Task'
        AND [System.AreaPath] = '{AREA_PATH}'
        AND [System.IterationPath] = '{ITERATION_PATH}'
        AND [System.State] <> 'Removed'
    ORDER BY 
        [System.Id]
    """
    
    print("Executing query...")
    print(f"Area Path: {AREA_PATH}")
    print(f"Iteration Path: {ITERATION_PATH}\n")
    
    # Step 4: Execute the query
    wiql = Wiql(query=wiql_query)
    query_results = wit_client.query_by_wiql(wiql, project=PROJECT_NAME).work_items
    
    if not query_results:
        print("No tasks found matching the criteria.")
        return
    
    # Step 5: Get full work item details
    # The query only returns IDs, we need to fetch full details
    work_item_ids = [item.id for item in query_results]
    work_items = wit_client.get_work_items(
        ids=work_item_ids,
        fields=[
            "System.Id",
            "System.Title",
            "Microsoft.VSTS.Scheduling.OriginalEstimate",
            "Microsoft.VSTS.Scheduling.RemainingWork",
            "Microsoft.VSTS.Scheduling.CompletedWork"
        ]
    )
    
    # Step 6: Display results
    print(f"Found {len(work_items)} task(s)\n")
    print("=" * 100)
    print(f"{'ID':<8} {'Task Name':<40} {'Original':<12} {'Remaining':<12} {'Completed':<12}")
    print("=" * 100)
    
    for item in work_items:
        fields = item.fields
        
        # Extract field values (handle None/missing values)
        task_id = fields.get('System.Id', 'N/A')
        task_name = fields.get('System.Title', 'N/A')
        original = fields.get('Microsoft.VSTS.Scheduling.OriginalEstimate') or 0
        remaining = fields.get('Microsoft.VSTS.Scheduling.RemainingWork') or 0
        completed = fields.get('Microsoft.VSTS.Scheduling.CompletedWork') or 0
        
        # Display row
        print(f"{task_id:<8} {task_name:<40} {original:<12} {remaining:<12} {completed:<12}")
    
    print("=" * 100)
    
    # Step 7: Show summary statistics
    total_original = sum(item.fields.get('Microsoft.VSTS.Scheduling.OriginalEstimate') or 0 
                        for item in work_items)
    total_remaining = sum(item.fields.get('Microsoft.VSTS.Scheduling.RemainingWork') or 0 
                         for item in work_items)
    total_completed = sum(item.fields.get('Microsoft.VSTS.Scheduling.CompletedWork') or 0 
                         for item in work_items)
    
    print(f"\nSummary:")
    print(f"  Total Original Estimate: {total_original} hours")
    print(f"  Total Remaining Work:    {total_remaining} hours")
    print(f"  Total Completed Work:    {total_completed} hours")


if __name__ == "__main__":
    try:
        query_tasks()
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Verify your PAT has 'Work Items (Read)' permission")
        print("2. Check your organization URL format")
        print("3. Verify project name, area path, and iteration path are correct")