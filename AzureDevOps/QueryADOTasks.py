"""
Azure DevOps Task Query Script
Queries tasks for a given area and iteration, displaying:
- Task ID
- Parent ID and Name
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
ORGANIZATION_URL = "https://dev.azure.com/FlightSafety-International"

# Your Personal Access Token (PAT) - Generate from Azure DevOps User Settings
PERSONAL_ACCESS_TOKEN = "YOUR_PAT_HERE"

# Project name
PROJECT_NAME = "Enterprise Portfolio"

# Filter criteria (PROJECT_NAME will be automatically prepended)
AREA_PATH_SUFFIX = "IT Execution\\Data Int and Vis\\Simulations"
ITERATION_PATH_SUFFIX = "2025\\25.4\\25.4.5"

# Assigned To filter - Set to your name or None to see all tasks
ASSIGNED_TO = "@Me"  # Use @Me for your tasks, or None for all tasks

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_iteration_dates(connection, project_name, iteration_path):
    """Fetch the start and end dates for a given iteration"""
    try:
        # Get the work client
        work_client = connection.clients.get_work_client()
        
        # Get all team contexts for the project
        team_client = connection.clients.get_teams_client()
        teams = team_client.get_teams(project_name)
        
        # Try to find the iteration in any team
        for team in teams:
            try:
                team_context = {
                    'project': project_name,
                    'team': team.name
                }
                
                # Get team iterations
                iterations = work_client.get_team_iterations(team_context)
                
                for iteration in iterations:
                    if iteration.path == iteration_path:
                        start_date = iteration.attributes.start_date if iteration.attributes else None
                        end_date = iteration.attributes.finish_date if iteration.attributes else None
                        return start_date, end_date
            except:
                continue
        
        return None, None
    except Exception as e:
        print(f"Warning: Could not retrieve iteration dates: {e}")
        return None, None

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
    
    # Step 2: Get iteration dates
    start_date, end_date = get_iteration_dates(connection, PROJECT_NAME, ITERATION_PATH)
    
    # Step 3: Get Work Item Tracking client
    wit_client = connection.clients.get_work_item_tracking_client()
    
    # Step 4: Build WIQL query with optional assigned-to filter
    assigned_to_clause = ""
    if ASSIGNED_TO:
        assigned_to_clause = f"AND [System.AssignedTo] = '{ASSIGNED_TO}'"
    
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
        {assigned_to_clause}
    ORDER BY 
        [System.Id]
    """
    
    print("Executing query...")
    print(f"Area Path: {AREA_PATH}")
    print(f"Iteration Path: {ITERATION_PATH}")
    
    # Display iteration dates if available
    if start_date and end_date:
        print(f"  Start Date: {start_date.strftime('%Y-%m-%d')}")
        print(f"  End Date:   {end_date.strftime('%Y-%m-%d')}")
    elif start_date or end_date:
        if start_date:
            print(f"  Start Date: {start_date.strftime('%Y-%m-%d')}")
        if end_date:
            print(f"  End Date:   {end_date.strftime('%Y-%m-%d')}")
    
    if ASSIGNED_TO:
        print(f"Assigned To: {ASSIGNED_TO}")
    print()
    
    # Step 5: Execute the query
    wiql = Wiql(query=wiql_query)
    query_results = wit_client.query_by_wiql(wiql).work_items
    
    if not query_results:
        print("No tasks found matching the criteria.")
        return
    
    # Step 6: Get full work item details (including relations)
    work_item_ids = [item.id for item in query_results]
    work_items = wit_client.get_work_items(
        ids=work_item_ids,
        expand="Relations"
    )
    
    # Step 7: Collect all parent IDs
    parent_ids = set()
    for item in work_items:
        if item.relations:
            for relation in item.relations:
                if relation.rel == "System.LinkTypes.Hierarchy-Reverse":
                    parent_id = int(relation.url.split('/')[-1])
                    parent_ids.add(parent_id)
    
    # Step 8: Fetch parent work items in batch
    parent_work_items = {}
    if parent_ids:
        parents = wit_client.get_work_items(ids=list(parent_ids))
        for parent in parents:
            parent_work_items[parent.id] = parent.fields.get('System.Title', 'N/A')
    
    # Step 9: Display results
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
    
    # Step 10: Show summary statistics
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
