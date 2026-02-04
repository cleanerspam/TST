from typing import Dict, Any, List
from Backend.logger import LOGGER

class QualityArbiter:
    """
    The 'Judge'. 
    Calculates a numerical QualityScore for media files based on 'Smart Upgrade v9' rules.
    """

    @staticmethod
    def calculate_score(probe_data: Dict[str, Any], semantic_tags: Dict[str, Any], file_info: Dict[str, Any]) -> dict:
        """
        Computes the final score + breakdown.
        Total Score = Base + Audio + Video + Subtitles + Meta
        """
        score = 0
        breakdown = []
        
        # ---------------------------------------------------------------------
        # 1. Base Score (Resolution)
        # ---------------------------------------------------------------------
        # Use Granular Height if available, otherwise Semantic tags
        p_height = probe_data.get("video", {}).get("height", 0)
        s_1080p = semantic_tags.get("is_1080p", False)
        
        if p_height > 0:
            score += p_height * 2
            breakdown.append(f"Base Resolution Height (x2) (+{p_height * 2})")
        elif s_1080p:
            score += 1080 * 2
            breakdown.append("Base Semantic 1080p (+2160)")
        else:
            # Fallback for SD or missing height
            score += 480 * 2
            breakdown.append("Base SD Fallback (+960)")

        # ---------------------------------------------------------------------
        # 2. Hierarchical Storage & Source Rules
        # ---------------------------------------------------------------------
        # Preferred Uploaders List (Ranked High to Low)
        PREFERRED_UPLOADERS = ["psa", "galatone", "mkvcinemas"] 
        
        filename = file_info.get("filename", file_info.get("name", "")).lower()
        uploader_bonus = 300 # Default for non-preferred
        found_uploader = None
        
        for idx, uploader in enumerate(PREFERRED_UPLOADERS):
            if uploader.lower() in filename:
                # Formula: 310 + (ranks above lowest) * 10
                # If idx=0 (Top) and len=3, bonus = 310 + (3-1-0)*10 = 330
                # If idx=2 (Bottom) and len=3, bonus = 310 + (3-1-2)*10 = 310
                uploader_bonus = 310 + (len(PREFERRED_UPLOADERS) - 1 - idx) * 10
                found_uploader = uploader
                break
        
        score += uploader_bonus
        if found_uploader:
            breakdown.append(f"Preferred Uploader: {found_uploader} (+{uploader_bonus})")
        else:
            breakdown.append("Standard Source (+300)")

        # DC4 Preference (+50 per user request)
        if str(file_info.get("dc_id", "")).strip() == "4":
            score += 50
            breakdown.append("DC4 Source (+50)")

        # Video Type Preference (+20 per user request)
        f_type = str(file_info.get("file_type", "video")).lower()
        if f_type == "video":
            score += 20
            breakdown.append("Video Type Bonus (+20)")
        elif f_type == "document":
            score -= 100 # Keep existing penalty for documents if necessary, but user wants +20 for video
            breakdown.append("Document Type Penalty (-100)")

        # ---------------------------------------------------------------------
        # 3. Combo & Container Rules
        # ---------------------------------------------------------------------
        # Perfect Combo Detection
        audio_tracks = probe_data.get("audio", [])
        langs = [t.get("lang", "und") for t in audio_tracks]
        has_hin = any("hi" in l or "hin" in l for l in langs) or semantic_tags.get("is_dual_audio")
        has_eng = any("en" in l or "eng" in l for l in langs)
        
        subs = probe_data.get("subtitle", [])
        sub_langs = [s.get("lang", "und") for s in subs]
        probe_has_eng = any("en" in l or "eng" in l for l in sub_langs) or semantic_tags.get("sub_combo") in ["eng", "hin_eng"]
        
        has_perfect_combo = has_hin and has_eng and probe_has_eng
        
        if has_perfect_combo:
            score += 250
            breakdown.append("Perfect Combo: Dual Audio + Eng Subs (+250)")

        # Container Rules
        container = probe_data.get("container", "").lower()
        if "mkv" in container or filename.endswith(".mkv"):
            score += 100
            breakdown.append("MKV Container (+100)")
        
        # Stricter MP4 Penalty
        if "mp4" in container or "mp4" in filename or filename.endswith(".mp4"):
            score -= 5000
            breakdown.append("MP4 Container/Name (-5000)")
            
        # ---------------------------------------------------------------------
        # 3. Video Rules
        # ---------------------------------------------------------------------
        # Codec
        v_codec = probe_data.get("video", {}).get("codec", "").lower()
        if not v_codec and semantic_tags.get("is_hevc"): v_codec = "hevc"
        
        if "hevc" in v_codec or "h265" in v_codec:
            score += 100
            breakdown.append("HEVC/x265 (+100)")
        elif "h264" in v_codec or "avc" in v_codec:
            score += 90
            breakdown.append("AVC/x264 (+90)")
            
        # 10-bit Color
        depth = probe_data.get("video", {}).get("depth", 8)
        if depth == 10 or semantic_tags.get("is_10bit"):
            score += 50
            breakdown.append("10-bit Color (+50)")

        # Anti-HDR
        is_hdr = probe_data.get("video", {}).get("is_hdr", False)
        if is_hdr:
            score -= 500
            breakdown.append("HDR/DV Penalty (-500)")
        else:
            score += 200
            breakdown.append("SDR Preferred (+200)")

        # ---------------------------------------------------------------------
        # 4. Audio Rules (The Hierarchy)
        # ---------------------------------------------------------------------
        audio_tracks = probe_data.get("audio", [])
        track_count = len(audio_tracks)
        
        # Bloat Check
        # Trust Semantic "Dual Audio" -> likely 2 tracks if probe failed
        if track_count == 0 and semantic_tags.get("is_dual_audio"):
             track_count = 2

        if track_count > 3:
            score -= 400
            breakdown.append(f"Audio Bloat {track_count} Tracks (-400)")
            
        # Language Analysis
        langs = [t.get("lang", "und") for t in audio_tracks]
        titles = [t.get("title", "").lower() for t in audio_tracks]
        
        has_hin = any("hi" in l or "hin" in l for l in langs) or semantic_tags.get("is_dual_audio")
        has_eng = any("en" in l or "eng" in l for l in langs)
        
        if has_hin and has_eng:
            score += 300
            breakdown.append("Dual Audio Hin+Eng (+300)")
        elif has_hin:
            score += 150
            breakdown.append("Hindi Audio (+150)")
        elif has_eng:
            score += 150
            breakdown.append("English Audio (+150)")
            
        # Codec Analysis
        # Just check if ANY track is good
        codecs = [t.get("codec", "").lower() for t in audio_tracks]
        
        if any(c == "aac" for c in codecs) or semantic_tags.get("has_aac"):
            score += 100
            breakdown.append("AAC Codec (+100)")
        elif any("eac3" in c for c in codecs) or semantic_tags.get("has_eac3"):
            score += 80
            breakdown.append("EAC3 Codec (+80)")
        elif any("mp3" in c for c in codecs):
            score -= 10
            breakdown.append("MP3 Penalty (-10)")
        elif any("truehd" in c for c in codecs) or semantic_tags.get("has_truehd"):
            score -= 50
            breakdown.append("TrueHD Penalty (-50)")

        # ---------------------------------------------------------------------
        # 5. Subtitle Rules
        # ---------------------------------------------------------------------
        subs = probe_data.get("subtitle", [])
        
        # Semantic Fallback
        if not subs:
            if semantic_tags.get("sub_combo") == "hin_eng":
                score += 350
                breakdown.append("Semantic Hin+Eng Subs (+350)")
            elif semantic_tags.get("sub_combo") == "eng":
                score += 100
                breakdown.append("Semantic Eng Subs (+100)")
            else:
                score -= 350
                breakdown.append("No Subtitles Penalty (-350)")
        else:
            # Probe Analysis
            sub_langs = [s.get("lang", "und") for s in subs]
            has_sdh = any(s.get("is_sdh") for s in subs)
            probe_has_hin = any("hi" in l or "hin" in l for l in sub_langs)
            probe_has_eng = any("en" in l or "eng" in l for l in sub_langs)
            
            if probe_has_hin and probe_has_eng:
                if has_sdh:
                    score += 400
                    breakdown.append("Hin+Eng SDH Subs (+400)")
                else:
                    score += 350
                    breakdown.append("Hin+Eng Subs (+350)")
            elif probe_has_eng:
                score += 100
                breakdown.append("English Subs (+100)")
            else:
                score -= 350
                breakdown.append("No English Subtitles Penalty (-350)")

        # ---------------------------------------------------------------------
        # 6. Novelty
        # ---------------------------------------------------------------------
        # Duration Check (Fake Sample)
        dur = probe_data.get("duration", 0)
        if dur > 0 and dur < 1200: # < 20 mins
            score -= 2000
            breakdown.append("Sample/Short Video (-2000)")

        return {
            "total_score": score,
            "breakdown": breakdown,
            "has_perfect_combo": has_perfect_combo,
            "height": p_height,
            "can_auto_resolve": score > 0
        }

    @staticmethod
    def compare(old_score: dict, new_score: dict, old_size: int, new_size: int) -> str:
        """
        Returns 'keep_new' or 'keep_old' based on Arbitration Logic.
        """
        s_old = old_score["total_score"]
        s_new = new_score["total_score"]
        
        # ---------------------------------------------------------------------
        # 1. Perfect Combo Tie-Breaker (User Request: Resolution-Aware Size Savings)
        # ---------------------------------------------------------------------
        if old_score.get("has_perfect_combo") and new_score.get("has_perfect_combo"):
            h = old_score.get("height", 0)
            # Thresholds in bytes
            if h >= 1000:
                threshold = 250 * 1024 * 1024
            elif h >= 700:
                threshold = 100 * 1024 * 1024
            else:
                threshold = 50 * 1024 * 1024
            
            size_diff = abs(new_size - old_size)
            
            if size_diff > threshold:
                if new_size < old_size:
                    LOGGER.info(f"DECISION: Keep New (Both have Perfect Combo, New is significantly Smaller: -{size_diff/1024/1024:.1f}MB)")
                    return "keep_new"
                else:
                    LOGGER.info(f"DECISION: Keep Old (Both have Perfect Combo, Old is significantly Smaller: -{size_diff/1024/1024:.1f}MB)")
                    return "keep_old"
            else:
                LOGGER.info(f"DECISION: Standard Compare (Size diff {size_diff/1024/1024:.1f}MB < threshold {threshold/1024/1024}MB)")

        diff = s_new - s_old
        
        # 2. Clear Winner (> 100 pts) 
        if diff > 100:
            LOGGER.info(f"DECISION: Keep New (Clear Winner +{diff})")
            return "keep_new"
        if diff < -100:
            LOGGER.info(f"DECISION: Keep Old (Clear Loser {diff})")
            return "keep_old"
            
        # 2. Dynamic Efficiency (Score Better by 50+)
        # Allow 10% size increase if score is significantly better
        if diff >= 50:
             allowable_size = old_size * 1.10
             LOGGER.info(f"Dynamic Limit: {allowable_size/1024/1024:.1f}MB (Old:{old_size/1024/1024:.1f}MB)")
             
             if new_size <= allowable_size:
                 LOGGER.info("DECISION: Keep New (Better Score, Size within 10% limit)")
                 return "keep_new"
             else:
                 LOGGER.info("DECISION: Keep Old (Better Score but Size > 10% Limit)")
                 return "keep_old"

        # 3. Efficiency Tie-Breaker (Score Diff < 50)
        # Strict efficiency: Prefer smaller file
        # But if size is virtually identical (< 1% diff), prefer higher score or newer
        size_diff_ratio = abs(new_size - old_size) / (old_size or 1)
        
        if size_diff_ratio < 0.01: # < 1% diff
             if s_new >= s_old:
                 LOGGER.info("DECISION: Keep New (Identical Size, Score >= Old)")
                 return "keep_new"
             else:
                 return "keep_old"

        if new_size < old_size:
            LOGGER.info("DECISION: Keep New (Smaller File, Similar Score)")
            return "keep_new"
        
        LOGGER.info("DECISION: Keep Old (Larger File, Similar Score)")
        return "keep_old"
