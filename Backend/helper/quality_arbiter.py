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
        # Use Semantic as Truth if Probe misses
        p_height = probe_data.get("video", {}).get("height", 0)
        s_1080p = semantic_tags.get("is_1080p", False)
        
        if p_height >= 1000 or s_1080p:
            score += 1000
            breakdown.append("Base 1080p (+1000)")
        elif p_height >= 700:
            score += 700
            breakdown.append("Base 720p (+700)")
        else:
            score += 480
            breakdown.append("Base SD (+480)")

        # ---------------------------------------------------------------------
        # 2. Storage & Source Rules
        # ---------------------------------------------------------------------
        # DC4 Bonus
        if str(file_info.get("dc_id", "")).strip() == "4":
            score += 200
            breakdown.append("DC4 Source (+200)")
            
        # Container Type
        container = probe_data.get("container", "").lower()
        filename = file_info.get("filename", "").lower()
        
        if "mkv" in container or filename.endswith(".mkv"):
            score += 100
            breakdown.append("MKV Container (+100)")
        elif "mp4" in container or filename.endswith(".mp4"):
            score -= 500
            breakdown.append("MP4 Container (-500)")
            
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
            score += 50
            breakdown.append("AVC/x264 (+50)")
            
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
                score -= 1000
                breakdown.append("No Subtitles Penalty (-1000)")
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
                score -= 100
                breakdown.append("Unknown/Bad Subs (-100)")

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
            "can_auto_resolve": score > 0
        }

    @staticmethod
    def compare(old_score: dict, new_score: dict, old_size: int, new_size: int) -> str:
        """
        Returns 'keep_new' or 'keep_old' based on Arbitration Logic.
        """
        s_old = old_score["total_score"]
        s_new = new_score["total_score"]
        
        diff = s_new - s_old
        
        # 1. Clear Winner (> 100 pts)
        if diff > 100:
            return "keep_new"
        if diff < -100:
            return "keep_old"
            
        # 2. Tie-Breaker (Efficiency)
        # If score is roughly same (+- 50), prefer smaller file
        if new_size < old_size:
            return "keep_new"
        
        return "keep_old"
