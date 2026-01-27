"""
Workload Sanitizer - Auto-corrects stuck workload counts

This utility runs periodically to ensure workload counts match reality.
If a bot shows workload but has no actual streams, it resets the count.
"""

import asyncio
from Backend.pyrofork.bot import work_loads, multi_clients
from Backend.helper.custom_dl import ACTIVE_STREAMS
from Backend.logger import LOGGER


async def sanitize_workloads():
    """
    Validate and correct workload counts based on actual active streams.
    Runs every 30 seconds as a background task.
    """
    LOGGER.info("Workload Sanitizer started")
    
    while True:
        try:
            await asyncio.sleep(30)  # Run every 30 seconds
            
            # Calculate expected workloads based on ACTIVE_STREAMS
            expected_loads = {cid: 0 for cid in multi_clients.keys()}
            
            for stream_id, stream_info in ACTIVE_STREAMS.items():
                # Primary bot
                primary_idx = stream_info.get("client_index")
                if primary_idx is not None and primary_idx in expected_loads:
                    expected_loads[primary_idx] += 1
                
                # Helper bots
                for helper_idx in stream_info.get("additional_indices", []):
                    if helper_idx in expected_loads:
                        expected_loads[helper_idx] += 1
            
            # Correct any mismatches
            corrections_made = False
            for cid in list(work_loads.keys()):
                actual = work_loads.get(cid, 0)
                expected = expected_loads.get(cid, 0)
                
                if actual != expected:
                    LOGGER.warning(
                        f"Workload mismatch detected for client {cid}: "
                        f"actual={actual}, expected={expected}. Auto-correcting..."
                    )
                    work_loads[cid] = expected
                    corrections_made = True
            
            if corrections_made:
                LOGGER.info("Workload sanitization complete - counts corrected")
                
        except Exception as e:
            LOGGER.error(f"Error in workload sanitizer: {e}")
            await asyncio.sleep(60)  # Wait longer on error
