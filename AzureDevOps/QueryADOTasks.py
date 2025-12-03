"""
Azure DevOps Task Query Script
Queries tasks for a given area and iteration, displaying:
- Task ID
- Task Name
- Parent ID
- Parent Name
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

ORGANIZATION_URL = "https://dev.azure.com/FlightSafety-International"
PERSONAL_ACCESS_TOKEN = "YOUR_PAT_HERE"
PROJECT_NAME = "Enterprise Portfolio"
# Filter criteria (PROJECT_NAME will be automatically prepended)
AREA_PATH_SUFFIX = "IT Execution\\Data Int and Vis\\Simulations"
ITERATION_PATH_SUFFIX = "2025\\25.4\\25.4.5"

# ============================================================================
# MAIN LOGIC
# ============================================================================

def query_tasks():
    """Query Azure DevOps for tasks matching area and iteration criteria"""
    
    # Build full paths by prepending project name
    AREA_PATH = f"{PROJECT_NAME}\\{AREA_PATH_SUFFIX}"
    ITERATION_PATH = f"{PROJECT_NAME}\\{ITERATION_PATH_SUFFIX}"
    
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
    FROM WorkItems
    WHERE
        [System.WorkItemType] = 'Task'
        AND [System.AreaPath] = '{AREA_PATH}'
        AND [System.IterationPath] = '{ITERATION_PATH}'
        AND [System.State] <> 'Removed'
    ORDER BY [System.Id]
    """
    
    print("Executing query...")
    print(f"Area Path: {AREA_PATH}")
    print(f"Iteration Path: {ITERATION_PATH}\n")
    
    # Step 4: Execute the query
    wiql = Wiql(query=wiql_query)
    query_results = wit_client.query_by_wiql(wiql).work_items
    
    if not query_results:
        print("No tasks found matching the criteria.")
        return
    
    # Step 5: Get full work item details (including relations)
    work_item_ids = [item.id for item in query_results]
    work_items = wit_client.get_work_items(
        ids=work_item_ids,
        expand="Relations"  # This gives us the parent/child relationships
    )
    
    # Step 6: Collect all parent IDs so we can fetch them in batch
    parent_ids = set()
    for item in work_items:
        if item.relations:
            for relation in item.relations:
                # Parent link type is "System.LinkTypes.Hierarchy-Reverse"
                if relation.rel == "System.LinkTypes.Hierarchy-Reverse":
                    # Extract ID from URL (format: https://.../_apis/wit/workItems/12345)
                    parent_id = int(relation.url.split('/')[-1])
                    parent_ids.add(parent_id)
    
    # Step 7: Fetch parent work items in batch
    parent_work_items = {}
    if parent_ids:
        parents = wit_client.get_work_items(ids=list(parent_ids))
        for parent in parents:
            parent_work_items[parent.id] = parent.fields.get('System.Title', 'N/A')
    
    # Step 8: Display results
    print(f"Found {len(work_items)} task(s)\n")
    print("=" * 140)
    print(f"{'ID':<8} {'Parent ID':<12} {'Parent Name':<30} {'Task Name':<30} {'Original':<10} {'Remaining':<10} {'Completed':<10}")
    print("=" * 140)
    
    for item in work_items:
        fields = item.fields
        
        # Extract field values
        task_id = fields.get('System.Id', 'N/A')
        task_name = fields.get('System.Title', 'N/A')
        original = fields.get('Microsoft.VSTS.Scheduling.OriginalEstimate') or 0
        remaining = fields.get('Microsoft.VSTS.Scheduling.RemainingWork') or 0
        completed = fields.get('Microsoft.VSTS.Scheduling.CompletedWork') or 0
        
        # Find parent information
        parent_id = 'N/A'
        parent_name = 'N/A'
        
        if item.relations:
            for relation in item.relations:
                if relation.rel == "System.LinkTypes.Hierarchy-Reverse":
                    parent_id = int(relation.url.split('/')[-1])
                    parent_name = parent_work_items.get(parent_id, 'N/A')
                    break
        
        # Truncate long names for display
        task_name_display = (task_name[:27] + '...') if len(task_name) > 30 else task_name
        parent_name_display = (parent_name[:27] + '...') if len(parent_name) > 30 else parent_name
        
        # Display row
        print(f"{task_id:<8} {str(parent_id):<12} {parent_name_display:<30} {task_name_display:<30} {original:<10} {remaining:<10} {completed:<10}")
    
    print("=" * 140)
    
    # Step 9: Show summary statistics
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