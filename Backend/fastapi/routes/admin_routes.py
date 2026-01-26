import os
import sys
import uuid
import subprocess
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query, Body
from Backend.fastapi.security.credentials import get_current_user
from Backend.helper.metadata_manager import metadata_manager
from Backend import db
from Backend.helper.metadata import fetch_movie_metadata, fetch_tv_metadata
from bson import ObjectId

from Backend.config import Telegram

router = APIRouter()

@router.post("/update")
async def update_server(current_user: str = Depends(get_current_user)):
    """
    Pull latest changes from git without restarting.
    """
    try:
        # Get current commit before pulling
        old_commit_process = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True
        )
        old_commit = old_commit_process.stdout.strip() if old_commit_process.returncode == 0 else "unknown"
        
        # Determine target branch
        current_branch = None
        
        # 1. Check config
        if Telegram.UPSTREAM_BRANCH:
            current_branch = Telegram.UPSTREAM_BRANCH
        
        # 2. Fallback to detecting current branch
        if not current_branch:
            branch_process = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], 
                capture_output=True, 
                text=True
            )
            
            if branch_process.returncode == 0:
                current_branch = branch_process.stdout.strip()
            else:
                current_branch = "main" # Fallback to main if detection fails
        
        # Run git pull with explicit remote and branch
        process = subprocess.run(
            ["git", "pull", "origin", current_branch], 
            capture_output=True, 
            text=True
        )
        
        if process.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Git pull failed: {process.stderr}")
        
        # Get new commit after pulling
        new_commit_process = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True
        )
        new_commit = new_commit_process.stdout.strip() if new_commit_process.returncode == 0 else "unknown"
        
        # Get commit message
        commit_msg_process = subprocess.run(
            ["git", "log", "-1", "--pretty=%B"],
            capture_output=True,
            text=True
        )
        commit_message = commit_msg_process.stdout.strip() if commit_msg_process.returncode == 0 else ""
        
        # Check if there were any updates
        if old_commit == new_commit:
            update_status = "Already up to date"
        else:
            update_status = f"Updated from {old_commit} to {new_commit}"
        
        return {
            "message": update_status,
            "old_commit": old_commit,
            "new_commit": new_commit,
            "commit_message": commit_message,
            "git_output": process.stdout,
            "note": "Updates pulled successfully. Use Restart Server if you want to apply changes."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/restart")
async def restart_server(background_tasks: BackgroundTasks, current_user: str = Depends(get_current_user)):
    """
    Restart the server process.
    """
    background_tasks.add_task(restart_process)
    return {"message": "Server restarting..."}

@router.post("/fixmetadata")
async def trigger_fix_metadata(
    background_tasks: BackgroundTasks, 
    type: str = Query("full", regex="^(full|file_info)$"),
    current_user: str = Depends(get_current_user)
):
    """
    Trigger the fix metadata background task.
    """
    if metadata_manager.IS_RUNNING:
         raise HTTPException(status_code=409, detail="Metadata fix is already running")

    # Start the task in background (pass type)
    background_tasks.add_task(metadata_manager.run_fix_metadata, None, 20, type)
    return {"message": f"Metadata fix ({type}) started in background"}

@router.post("/fixmetadata/cancel")
async def cancel_fix_metadata(current_user: str = Depends(get_current_user)):
    """
    Cancel the currently running metadata fix task.
    """
    if not metadata_manager.IS_RUNNING:
        raise HTTPException(status_code=400, detail="No metadata fix is running")
    
    metadata_manager.CANCEL_REQUESTED = True
    return {"message": "Cancellation requested. The task will stop shortly."}

@router.get("/fixmetadata/status")
async def get_fix_metadata_status(current_user: str = Depends(get_current_user)):
    """
    Get the current status of the metadata fix task.
    """
    return {
        "is_running": metadata_manager.IS_RUNNING,
        "progress": metadata_manager.last_progress
    }

def restart_process():
    """
    Restart the current python process.
    This is a helper function that might need adjustment based on how the app is run (Docker vs bare metal).
    For Docker, exiting usually triggers a container restart policy if set.
    """
    import time
    import shutil
    
    time.sleep(1) # Give time for response to be sent
    
    # Always use the current interpreter to ensure we stay in the same environment (venv/system)
    # This avoids issues where 'uv' might try to use a different env or is not found in PATH context
    os.execv(sys.executable, [sys.executable, '-m', 'Backend'])

# ---------------------------
# MANAGED UPDATES ROUTES
# ---------------------------

@router.get("/updates/pending")
async def get_pending_updates_route(
    page: int = Query(1, ge=1), 
    page_size: int = Query(20, ge=1, le=100),
    current_user: str = Depends(get_current_user)
):
    results, total = await db.get_pending_updates(page, page_size)
    return {
        "results": results,
        "total": total,
        "page": page,
        "page_size": page_size
    }

@router.post("/updates/delete_all")
async def delete_all_updates_route(
    payload: dict = Body(...),
    current_user: str = Depends(get_current_user)
):
    from Backend.helper.task_manager import delete_multiple_messages

    pending_id = payload.get("pending_id")
    if not pending_id:
        raise HTTPException(status_code=400, detail="Missing pending_id")

    success, msg, deletion_list = await db.delete_pending_update_and_files(pending_id)
    
    if not success:
         raise HTTPException(status_code=400, detail=msg)

    if deletion_list:
        await delete_multiple_messages(deletion_list)

    return {"message": msg}

@router.post("/updates/resolve")
async def resolve_update_route(
    payload: dict = Body(...),
    current_user: str = Depends(get_current_user)
):
    from Backend.helper.task_manager import delete_multiple_messages

    pending_id = payload.get("pending_id")
    decision = payload.get("action") # "keep_old", "keep_new"
    
    if not pending_id or not decision:
        raise HTTPException(status_code=400, detail="Missing pending_id or action")
        
    success, msg, deletion_list = await db.resolve_pending_update(pending_id, decision)
    if not success:
        raise HTTPException(status_code=400, detail=msg)
    
    # Process deletions if any
    if deletion_list:
        # Fire and forget for single resolve? Or await?
        # Await is safer for consistency.
        await delete_multiple_messages(deletion_list)
        
    return {"message": msg}

@router.post("/updates/search_tmdb")
async def search_tmdb_route(
    payload: dict = Body(...),
    current_user: str = Depends(get_current_user)
):
    from Backend.helper.metadata import search_tmdb_candidates
    
    query = payload.get("query")
    media_type = payload.get("type", "movie") # movie, tv
    year = payload.get("year")
    
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")
        
    try:
        results = await search_tmdb_candidates(query, media_type, year)
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/updates/reidentify")
async def reidentify_update_route(
    payload: dict = Body(...),
    current_user: str = Depends(get_current_user)
):
    pending_id = payload.get("pending_id")
    tmdb_id = payload.get("tmdb_id")
    # Optional parameters for overriding/correcting S/E
    override_season = payload.get("season")
    override_episode = payload.get("episode")
    
    if not pending_id or not tmdb_id:
        raise HTTPException(status_code=400, detail="Missing parameters")

    try:
        pending_doc = await db.dbs["tracking"]["pending_updates"].find_one({"_id": ObjectId(pending_id)})
    except:
        raise HTTPException(status_code=400, detail="Invalid Pending ID")
        
    if not pending_doc:
        raise HTTPException(status_code=404, detail="Pending update not found")
        
    file_info = pending_doc["new_file"]
    encoded_string = file_info.get("id") 
    quality = pending_doc["quality"]
    
    new_metadata = None
    try:
        if pending_doc["media_type"] == "movie":
            new_metadata = await fetch_movie_metadata(
                title=pending_doc["metadata"]["title"], 
                encoded_string=encoded_string,
                quality=quality,
                default_id=tmdb_id
            )
        else:
            # Use overrides if provided, else fall back to pending doc
            sn = override_season if override_season is not None else pending_doc["season"]
            ep = override_episode if override_episode is not None else pending_doc["episode"]
            
            new_metadata = await fetch_tv_metadata(
                title=pending_doc["metadata"]["title"],
                season=sn,
                episode=ep,
                encoded_string=encoded_string,
                quality=quality,
                default_id=tmdb_id
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Metadata fetch failed: {str(e)}")
        
    if not new_metadata:
        raise HTTPException(status_code=400, detail="Failed to fetch metadata for provided ID")
        
    success, msg = await db.reidentify_pending_update(pending_id, new_metadata)
    if not success:
         raise HTTPException(status_code=400, detail=msg)
    return {"message": msg}

@router.post("/updates/bulk_resolve")
async def bulk_resolve_updates(
    background_tasks: BackgroundTasks,
    payload: dict = Body(...),
    current_user: str = Depends(get_current_user)
):
    """
    Resolve multiple pending updates in bulk (Background Task).
    Returns a task_id to poll status.
    """
    selections = payload.get("selections", [])
    if not selections:
        raise HTTPException(status_code=400, detail="No selections provided")
    
    task_id = str(uuid.uuid4())
    background_tasks.add_task(db.run_bulk_resolve_background, task_id, selections)
    
    return {
        "message": "Bulk operation initiated.",
        "task_id": task_id
    }

@router.get("/updates/task/{task_id}")
async def get_task_status_route(
    task_id: str,
    current_user: str = Depends(get_current_user)
):
    """
    Poll status of a bulk resolve task.
    """
    status = db.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")
        
    return status
