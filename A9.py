from __future__ import annotations
import os
import io
import re
import sys
import math
import json
import gc
import traceback
import logging
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Optional, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image as PILImage
import cv2
import pdfplumber
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as XLImage
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential

# -------------------------------
# =========== CONFIG ============
# -------------------------------
@dataclass
class Config:
    BASE_FOLDER: str = r"C:\Users\Desktop\FILENAME"  # PLEASE ADJUST YOUR FILE PATH 
    MASTER_KEYWORDS: Tuple[str, ...] = ("exp", "expense", "report")
    IMAGE_EXTS: Tuple[str, ...] = (".png", ".jpg", ".jpeg", ".tiff", ".bmp")
    EXCEL_EXTS: Tuple[str, ...] = (".xlsx", ".xlsm", ".xls")
    PDF_EXTS: Tuple[str, ...] = (".pdf",)
    OCR_LANGS: Tuple[str, ...] = ("ch_tra", "en")
    OCR_MIN_CONF: float = 0.40
    PDF_RENDER_DPI: int = 300
    AMOUNT_TOLERANCE: float = 0.01
    COMBO_MAX_LEN: int = 3
    CANDIDATE_AMOUNT_MIN: float = 0.1
    DEDUPLICATE_MATCHES: bool = True
    WRITE_ANNOTATED_IMAGES: bool = True
    PRINT_DEBUG: bool = True
    SAVE_CONVERTED_PDF_MASTER_AS_EXCEL: bool = True
    MAX_WORKERS: int = 1        # safer default; increase if stable
    BATCH_SIZE: int = 10
    MAX_FILE_SIZE_MB: int = 100
    CLEANUP_TEMP_FILES: bool = True
    LOG_FILE: str = "reconciliation.log"
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if self.OCR_MIN_CONF < 0 or self.OCR_MIN_CONF > 1:
            raise ValueError("OCR_MIN_CONF must be between 0 and 1")
        if self.AMOUNT_TOLERANCE < 0:
            raise ValueError("AMOUNT_TOLERANCE must be non-negative")
        if not os.path.exists(self.BASE_FOLDER):
            raise FileNotFoundError(f"BASE_FOLDER does not exist: {self.BASE_FOLDER}")
        if self.MAX_WORKERS < 1:
            raise ValueError("MAX_WORKERS must be at least 1")


CFG = Config()

# -------------------------------
# ======== LOGGING SETUP ========
# -------------------------------
def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(CFG.LOG_FILE, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


def log(msg: str, level=logging.INFO):
    """Unified logging function"""
    if CFG.PRINT_DEBUG or level != logging.DEBUG:
        logger.log(level, msg)


# -------------------------------
# ======= SMALL UTILITIES =======
# -------------------------------
def safe_float(x: Any) -> Optional[float]:
    """Safely convert value to float"""
    try:
        if x is None:
            return None
        if isinstance(x, str):
            s = x.strip().replace(",", "")
            if s.lower() in ("nan", "", "null", "none"):
                return None
            return float(s)
        return float(x)
    except Exception:
        return None


def validate_file_path(file_path: str) -> bool:
    """Validate file path for security"""
    try:
        # Prevent path traversal
        resolved = Path(file_path).resolve()
        base = Path(CFG.BASE_FOLDER).resolve()
        return str(resolved).startswith(str(base)) and resolved.is_file()
    except Exception:
        return False


def check_file_size(file_path: str) -> bool:
    """Check if file size is within limits"""
    try:
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        return size_mb <= CFG.MAX_FILE_SIZE_MB
    except Exception:
        return False


_AMOUNT_CLEAN_RE = re.compile(
    r"(HK\$|\$|\u20ac|\u00a3|\u00a5|\(|\)|小寫|,|\u200b|\u00a0)", re.IGNORECASE
)


def extract_amounts_from_text_enhanced(text: str) -> List[Dict[str, Any]]:
    """
    Enhanced amount extraction with context and metadata
    """
    if not text:
        return []
    
    amounts = []
    # Look for patterns with context
    patterns = [
        (r'(?:HK\$|\$|€|£|¥)\s*(\d+(?:,\d{3})*(?:\.\d{2})?)', 'currency_prefix'),
        (r'(\d+(?:,\d{3})*(?:\.\d{2})?)\s*(?:HK\$|\$|€|£|¥)', 'currency_suffix'),
        (r'總計[:\s]*(\d+(?:\.\d{2})?)', 'total_chinese'),
        (r'TOTAL[:\s]*(\d+(?:\.\d{2})?)', 'total_english'),
        (r'AMOUNT[:\s]*(\d+(?:\.\d{2})?)', 'amount_label'),
        (r'(\d+(?:\.\d{2})?)\s*$', 'line_end'),  # Amount at end of line
    ]
    
    # First clean the text
    cleaned = _AMOUNT_CLEAN_RE.sub("", text)
    cleaned = re.sub(r"\s*\.\s*", ".", cleaned)
    cleaned = cleaned.replace("．", ".")
    
    for pattern, context in patterns:
        matches = re.finditer(pattern, cleaned, re.IGNORECASE)
        for match in matches:
            amount_str = match.group(1).replace(',', '')
            try:
                amount = abs(float(amount_str))
                if amount >= CFG.CANDIDATE_AMOUNT_MIN:
                    # Check if this amount might be a duplicate (nearby positions)
                    is_duplicate = False
                    for existing in amounts:
                        if abs(existing['value'] - amount) < 0.01:
                            is_duplicate = True
                            break
                    
                    if not is_duplicate:
                        amounts.append({
                            'value': amount,
                            'context': context,
                            'full_match': match.group(0),
                            'position': match.span(),
                            'confidence': 1.0 if context.startswith('total') else 0.8
                        })
            except ValueError:
                continue
    
    # Sort by confidence and value
    amounts.sort(key=lambda x: (-x['confidence'], -x['value']))
    return amounts


def poly_to_int_pts(bbox: List[Tuple[float, float]]) -> np.ndarray:
    """Convert bbox to integer points for OpenCV"""
    pts = np.array(bbox, dtype=np.float32)
    pts = pts.reshape((-1, 1, 2)).astype(np.int32)
    return pts


# -------------------------------
# ========== DATA TYPES =========
# -------------------------------
@dataclass
class OCRBox:
    ocr_id: int
    src_file: str
    src_type: str
    page_label: str
    page_num: Optional[int]
    text: str
    conf: float
    bbox: List[Tuple[float, float]]
    raster_w: int
    raster_h: int
    raster_path: Optional[str]
    amount_candidates: List[Dict[str, Any]] = field(default_factory=list)
    ocr_quality_score: float = 1.0
    
    def __post_init__(self):
        """Calculate quality score after initialization"""
        self.calculate_quality_score()
    
    def calculate_quality_score(self):
        """Calculate overall quality based on confidence and text characteristics"""
        score = self.conf
        
        # Penalize very short or suspicious texts
        if len(self.text.strip()) < 3:
            score *= 0.7
        elif len(self.text.strip()) > 50:
            score *= 0.9  # Very long texts might be noisy
        
        # Bonus for numbers with decimal places (likely amounts)
        if re.search(r'\d+\.\d{2}', self.text):
            score = min(score * 1.2, 1.0)
        
        # Penalize excessive special characters
        special_char_ratio = len(re.findall(r'[^a-zA-Z0-9\s]', self.text)) / max(len(self.text), 1)
        if special_char_ratio > 0.3:
            score *= 0.8
        
        self.ocr_quality_score = min(max(score, 0), 1)  # Clamp between 0 and 1
    
    def best_amount(self) -> Optional[float]:
        """Get the best amount candidate"""
        if self.amount_candidates:
            return self.amount_candidates[0]['value']
        return None
    
    def best_amount_with_context(self) -> Optional[Dict[str, Any]]:
        """Get the best amount candidate with context"""
        return self.amount_candidates[0] if self.amount_candidates else None


@dataclass
class MatchResult:
    master_index: int
    master_amount: float
    master_note: str
    matched_ocr_ids: List[int]
    labels: List[str]
    match_confidence: float = 1.0
    match_details: Dict[str, Any] = field(default_factory=dict)


# -------------------------------
# ======== OCR & LOADING ========
# -------------------------------
class OCREngine:
    def __init__(self, langs: Tuple[str, ...], gpu: bool = False):
        import easyocr
        self.reader = easyocr.Reader(list(langs), gpu=gpu)
        log(f"OCR Engine initialized with languages: {langs}, GPU: {gpu}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def read_np_with_retry(self, np_img: np.ndarray) -> List[Tuple[List[Tuple[float, float]], str, float]]:
        """Read text from numpy image with retry logic"""
        return self.reader.readtext(np_img)
    
    def read_np(self, np_img: np.ndarray) -> List[Tuple[List[Tuple[float, float]], str, float]]:
        """Read text from numpy image"""
        try:
            return self.read_np_with_retry(np_img)
        except Exception as e:
            log(f"OCR failed after retries: {e}", logging.ERROR)
            return []


class Loader:
    def __init__(self, config: Config, ocr: OCREngine):
        self.cfg = config
        self.ocr = ocr
        self.ocr_counter = 0
        self._pdf_rasters_cache: Dict[Tuple[str, int], str] = {}
        self._temp_files: Set[str] = set()
    
    def __del__(self):
        """Cleanup temporary files on destruction"""
        self.cleanup_temp_files()
    
    def cleanup_temp_files(self):
        """Remove temporary raster files"""
        if not self.cfg.CLEANUP_TEMP_FILES:
            return
        
        for temp_file in self._temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    log(f"Cleaned up temp file: {temp_file}", logging.DEBUG)
            except Exception as e:
                log(f"Failed to cleanup {temp_file}: {e}", logging.WARNING)
        self._temp_files.clear()
    
    # ---------- Master discovery ----------
    def is_master_file(self, filename: str) -> bool:
        lower = filename.lower()
        return any(kw in lower for kw in self.cfg.MASTER_KEYWORDS)
    
    def find_master_file(self, folder: str) -> Tuple[Optional[str], Optional[str]]:
        excel_master = None
        pdf_master = None
        for fn in os.listdir(folder):
            fp = os.path.join(folder, fn)
            if not os.path.isfile(fp):
                continue
            _, ext = os.path.splitext(fn.lower())
            if self.is_master_file(fn):
                if ext in self.cfg.EXCEL_EXTS:
                    excel_master = fp
                elif ext in self.cfg.PDF_EXTS:
                    pdf_master = fp
        if excel_master:
            return excel_master, "excel"
        if pdf_master:
            return pdf_master, "pdf"
        return None, None
    
    # ---------- Master reading ----------
    def read_master_excel(self, path: str) -> pd.DataFrame:
        try:
            df = pd.read_excel(path, engine="openpyxl")
            log(f"Read master Excel: {path} ({len(df)} rows)")
            return df
        except Exception as e:
            log(f"Failed to read master Excel {path}: {e}", logging.ERROR)
            return pd.DataFrame()
    
    def read_master_pdf_to_df(self, path: str) -> pd.DataFrame:
        tables: List[pd.DataFrame] = []
        try:
            with pdfplumber.open(path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    try:
                        table = page.extract_table()
                        if table:
                            df = pd.DataFrame(table)
                            tables.append(df)
                            log(f"Extracted table from PDF page {page_num}")
                    except Exception as e:
                        log(f"Failed to extract table from page {page_num}: {e}", logging.WARNING)
                        continue
        except Exception as e:
            log(f"Failed to open PDF {path}: {e}", logging.ERROR)
        
        if tables:
            return pd.concat(tables, ignore_index=True)
        return pd.DataFrame()
    
    # ---------- PDF rasterization (single pass, reusable) ----------
    def _render_pdf_page(self, pdf_path: str, page_num: int) -> Tuple[np.ndarray, str]:
        """
        Renders a PDF page to a raster (RGB) at configured DPI and saves to disk.
        Returns (np_img_rgb, saved_path).
        """
        cache_key = (pdf_path, page_num)
        if cache_key in self._pdf_rasters_cache and os.path.isfile(self._pdf_rasters_cache[cache_key]):
            saved_path = self._pdf_rasters_cache[cache_key]
            np_img = cv2.cvtColor(cv2.imread(saved_path), cv2.COLOR_BGR2RGB)
            return np_img, saved_path
        
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[page_num - 1]
            pil_image = page.to_image(resolution=self.cfg.PDF_RENDER_DPI).original
            np_img = np.array(pil_image)
        
        out_path = os.path.join(
            os.path.dirname(pdf_path), 
            f"__raster_{os.path.basename(pdf_path)}_p{page_num}_{self.cfg.PDF_RENDER_DPI}dpi.png"
        )
        cv2.imwrite(out_path, cv2.cvtColor(np_img, cv2.COLOR_RGB2BGR))
        self._pdf_rasters_cache[cache_key] = out_path
        self._temp_files.add(out_path)
        return np_img, out_path
    
    # ---------- OCR for files ----------
    def ocr_image_file(self, path: str, project_name: str) -> List[OCRBox]:
        if not validate_file_path(path) or not check_file_size(path):
            log(f"File validation failed: {path}", logging.WARNING)
            return []
        
        _, ext = os.path.splitext(path.lower())
        if ext not in self.cfg.IMAGE_EXTS:
            return []
        
        np_img = cv2.imread(path)
        if np_img is None or np_img.size == 0:
            log(f"Failed to load image: {path}", logging.WARNING)
            return []
        
        np_img = cv2.cvtColor(np_img, cv2.COLOR_BGR2RGB)
        return self._ocr_np(np_img, os.path.basename(path), "image", "Image", None, path)
    
    def ocr_pdf_file(self, path: str, project_name: str) -> List[OCRBox]:
        if not validate_file_path(path) or not check_file_size(path):
            log(f"File validation failed: {path}", logging.WARNING)
            return []
        
        results: List[OCRBox] = []
        try:
            with pdfplumber.open(path) as pdf:
                total_pages = len(pdf.pages)
                for page_num in range(1, total_pages + 1):
                    try:
                        np_img, raster_path = self._render_pdf_page(path, page_num)
                        page_results = self._ocr_np(
                            np_img, os.path.basename(path), "pdf", 
                            f"Page {page_num}", page_num, raster_path
                        )
                        results.extend(page_results)
                    except Exception as e:
                        log(f"Failed to process PDF page {page_num}: {e}", logging.WARNING)
                        continue
        except Exception as e:
            log(f"OCR PDF failed for {path}: {e}", logging.ERROR)
        return results
    
    def ocr_excel_embedded_images(self, path: str, project_name: str) -> List[OCRBox]:
        if not validate_file_path(path) or not check_file_size(path):
            log(f"File validation failed: {path}", logging.WARNING)
            return []
        
        out: List[OCRBox] = []
        try:
            wb = load_workbook(path)
            for ws in wb.worksheets:
                images = getattr(ws, "_images", [])
                if not images:
                    continue
                for idx, img in enumerate(images, start=1):
                    if not isinstance(img, XLImage):
                        continue
                    try:
                        # Extract bytes; openpyxl stores original content
                        img_bytes = img._data() if hasattr(img, "_data") else None
                        if img_bytes is None:
                            continue
                        pil_img = PILImage.open(io.BytesIO(img_bytes)).convert("RGB")
                        np_img = np.array(pil_img)
                        page_label = f"{ws.title} - Embedded Image {idx}"
                        # Save a stable raster for annotation
                        raster_path = os.path.join(
                            os.path.dirname(path), 
                            f"__raster_{os.path.basename(path)}_{ws.title}_img{idx}.png"
                        )
                        cv2.imwrite(raster_path, cv2.cvtColor(np_img, cv2.COLOR_RGB2BGR))
                        self._temp_files.add(raster_path)
                        
                        results = self._ocr_np(
                            np_img, os.path.basename(path), "excel-embedded", 
                            page_label, None, raster_path
                        )
                        out.extend(results)
                    except Exception as e:
                        log(f"Failed to OCR embedded img in {path} [{ws.title} #{idx}]: {e}", logging.WARNING)
        except Exception as e:
            log(f"Failed to read Excel for embedded images {path}: {e}", logging.WARNING)
        return out
    
    # ---------- Core OCR over a numpy image ----------
    def _ocr_np(self, np_img: np.ndarray, src_basename: str, src_type: str, page_label: str,
                page_num: Optional[int], raster_path: Optional[str]) -> List[OCRBox]:
        h, w = np_img.shape[:2]
        ocr_out: List[OCRBox] = []
        try:
            easy_out = self.ocr.read_np(np_img)
            for bbox, text, conf in easy_out:
                if conf < self.cfg.OCR_MIN_CONF:
                    continue
                amounts = extract_amounts_from_text_enhanced(text)
                box = OCRBox(
                    ocr_id=self.ocr_counter,
                    src_file=src_basename,
                    src_type=src_type,
                    page_label=page_label,
                    page_num=page_num,
                    text=text,
                    conf=float(conf),
                    bbox=[tuple(map(float, p)) for p in bbox],
                    raster_w=int(w),
                    raster_h=int(h),
                    raster_path=raster_path,
                    amount_candidates=amounts
                )
                ocr_out.append(box)
                self.ocr_counter += 1
        except Exception as e:
            log(f"EasyOCR failed on image ({src_basename} {page_label}): {e}", logging.ERROR)
        return ocr_out
    
    def process_file_batch(self, file_batch: List[Tuple[str, str, str]], project_name: str) -> List[OCRBox]:
        """Process a batch of files"""
        results = []
        for fp, ext, _proj_name in file_batch:
            try:
                if ext in self.cfg.IMAGE_EXTS:
                    results.extend(self.ocr_image_file(fp, project_name))
                elif ext in self.cfg.PDF_EXTS:
                    results.extend(self.ocr_pdf_file(fp, project_name))
                elif ext in self.cfg.EXCEL_EXTS:
                    results.extend(self.ocr_excel_embedded_images(fp, project_name))
            except Exception as e:
                log(f"Skipped {fp}: {e}", logging.WARNING)
        return results


# -------------------------------
# ===== MASTER NORMALIZATION ====
# -------------------------------
def normalize_master_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Try to locate columns for:
      - Item Amount
      - Receipt Flag (Y/N)
    If not found by exact names, attempt heuristic matching.
    Returns a new DataFrame with standardized columns:
      ['Item Amount', 'Receipt Flag', ...others]
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["Item Amount", "Receipt Flag"])
    
    # Clean header names
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    
    # Try direct names first
    col_item = None
    amount_keywords = ["item amount", "amount", "amt", "item_amt", "value", "price", "cost"]
    for c in df.columns:
        cl = c.lower()
        for kw in amount_keywords:
            if kw in cl:
                col_item = c
                break
        if col_item:
            break
    
    # Heuristic match by tokens if still None
    if col_item is None:
        # Look for columns with mostly numbers
        for c in df.columns:
            try:
                numeric_ratio = df[c].apply(lambda x: isinstance(safe_float(x), float)).mean()
                if numeric_ratio > 0.5:
                    col_item = c
                    break
            except Exception:
                continue
    
    if col_item is None:
        # last resort: unnamed numeric-looking column
        unnamed = [c for c in df.columns if "unnamed" in c.lower()]
        col_item = unnamed[0] if unnamed else df.columns[0]
        log(f"Using fallback amount column: {col_item}", logging.WARNING)
    
    # Find receipt flag column
    col_receipt = None
    receipt_keywords = ["receipt", "flag", "yn", "y/n", "identified", "id"]
    for c in df.columns:
        cl = c.lower()
        if any(kw in cl for kw in receipt_keywords):
            col_receipt = c
            break
    
    # Standardize columns
    if col_receipt is None:
        # Create a column defaulting to 'Y' if amounts exist
        df["Receipt Flag"] = df[col_item].apply(
            lambda x: "Y" if pd.notna(x) and safe_float(x) is not None else "N"
        )
    else:
        df["Receipt Flag"] = df[col_receipt].astype(str).str.strip().str.upper().map(
            lambda s: "Y" if s in ("Y", "YES", "TRUE", "1") else "N"
        )
    
    # Standardize item amount
    df["Item Amount"] = df[col_item].apply(safe_float)
    
    # Add metadata
    df["_source_column"] = col_item
    if col_receipt:
        df["_receipt_column"] = col_receipt
    
    return df


# -------------------------------
# ======== MATCHING LOGIC =======
# -------------------------------
def find_combinations(numbers: List[Tuple[int, float, float]],  # (ocr_id, amount, confidence)
                      target: float,
                      tolerance: float,
                      max_len: int) -> List[List[Tuple[int, float, float]]]:
    """
    Returns a list of candidate combos that sum to target within tolerance.
    Enhanced with confidence scores and pruning.
    """
    # Sort by amount descending for better pruning
    nums = sorted(numbers, key=lambda x: (-x[1], -x[2]))
    out: List[List[Tuple[int, float, float]]] = []
    best_len = max_len + 1
    
    def backtrack(start: int, cur: List[Tuple[int, float, float]], s: float, used_conf: float):
        nonlocal best_len
        
        # Pruning
        if len(cur) > best_len:
            return
        if s > target + tolerance:
            return
        if len(cur) > max_len:
            return
        
        # Check if we have a match
        if abs(s - target) < tolerance:
            if len(cur) < best_len:
                best_len = len(cur)
                out.clear()
                out.append(list(cur))
            elif len(cur) == best_len:
                out.append(list(cur))
            return
        
        # Continue search
        for i in range(start, len(nums)):
            ocr_id, val, conf = nums[i]
            # Optimistic pruning: if even the largest remaining can't reach target (rough heuristic)
            if i < len(nums) - 1 and s + val < target - tolerance:
                # not a perfect bound but keeps search manageable
                pass
            
            cur.append((ocr_id, val, conf))
            # Calculate combined confidence (product of confidences for independent events)
            new_conf = used_conf * conf if used_conf > 0 else conf
            backtrack(i + 1, cur, s + val, new_conf)
            cur.pop()
    
    backtrack(0, [], 0.0, 0.0)
    
    if not out:
        return []
    
    # Sort combos by confidence and composition
    out.sort(key=lambda combo: (
        len(combo),  # Prefer shorter combos
        -np.prod([c[2] for c in combo]),  # Higher combined confidence
        [-c[1] for c in combo]  # Larger amounts first
    ))
    return out


def greedy_match_master_to_ocr(master_df: pd.DataFrame,
                               ocr_boxes: List[OCRBox],
                               tolerance: float,
                               max_len: int,
                               deduplicate: bool) -> List[MatchResult]:
    """
    Enhanced matching with confidence scoring and validation.
    """
    # Build candidate list from OCR with quality scores
    candidates: List[Tuple[int, float, float]] = []  # (ocr_id, amount, confidence)
    for b in ocr_boxes:
        best_amount = b.best_amount_with_context()
        if best_amount:
            # Combine OCR confidence with amount context confidence
            combined_conf = b.ocr_quality_score * best_amount.get('confidence', 0.8)
            candidates.append((b.ocr_id, best_amount['value'], combined_conf))
    
    used: set[int] = set()
    matches: List[MatchResult] = []
    
    # Ensure we only loop over rows with item amounts present
    working = master_df.copy()
    if "Item Amount" not in working.columns:
        working["Item Amount"] = None
    if "Receipt Flag" not in working.columns:
        working["Receipt Flag"] = "N"
    
    # Track matching statistics
    total_y_flag = 0
    matched_count = 0
    
    for idx, row in working.iterrows():
        rec_flag = str(row.get("Receipt Flag", "N")).strip().upper()
        amt = safe_float(row.get("Item Amount"))
        note = ""
        matched_ids: List[int] = []
        labels: List[str] = []
        match_confidence = 0.0
        match_details = {}
        
        if rec_flag == "Y" and amt is not None:
            total_y_flag += 1
            # Filter candidates; optionally remove used
            pool = [(oid, v, conf) for (oid, v, conf) in candidates 
                   if (not deduplicate or oid not in used)]
            
            combos = find_combinations(pool, amt, tolerance, max_len)
            
            if combos:
                best = combos[0]
                matched_ids = [oid for (oid, _, _) in best]
                match_confidence = float(np.prod([conf for (_, _, conf) in best]))
                
                # Mark used if deduplicate
                if deduplicate:
                    for oid in matched_ids:
                        used.add(oid)
                
                # Prepare human-readable labels (per-master-row indexing 1-based)
                if len(best) == 1:
                    labels = ["1"]
                    note = f"Matched 1 amount: {best[0][1]:.2f} (conf: {match_confidence:.2f})"
                else:
                    labels = [f"1{chr(65+i)}" for i in range(len(best))]
                    amounts_str = ", ".join([f"{v:.2f}" for (_, v, _) in best])
                    note = f"Matched combo: {amounts_str} (conf: {match_confidence:.2f})"
                
                match_details = {
                    'combo_length': len(best),
                    'amounts': [v for (_, v, _) in best],
                    'confidences': [conf for (_, _, conf) in best]
                }
                matched_count += 1
            else:
                note = f"No combination sums to {amt:.2f} (within ±{tolerance})"
                # Suggest closest match for debugging
                if pool:
                    amounts = [v for (_, v, _) in pool]
                    closest = min(amounts, key=lambda x: abs(x - amt)) if amounts else None
                    if closest:
                        note += f" - closest single amount: {closest:.2f}"
        else:
            note = "Skipped (no receipt flag or no amount)"
        
        matches.append(MatchResult(
            master_index=idx,
            master_amount=amt if amt is not None else float("nan"),
            master_note=note,
            matched_ocr_ids=matched_ids,
            labels=labels,
            match_confidence=match_confidence,
            match_details=match_details
        ))
    
    # Log matching statistics
    if total_y_flag > 0:
        log(f"Matching stats: {matched_count}/{total_y_flag} receipts matched ({matched_count/total_y_flag*100:.1f}%)")
    
    return matches


def validate_matches(matches: List[MatchResult], ocr_boxes: List[OCRBox]) -> Dict[str, Any]:
    """Validate matching results and return quality metrics"""
    total_master = len(matches)
    matched = sum(1 for m in matches if m.matched_ocr_ids)
    unmatched = total_master - matched
    
    # Check for OCR boxes that should have been matched
    used_ocr_ids = set()
    for m in matches:
        used_ocr_ids.update(m.matched_ocr_ids)
    
    orphaned_ocr = [
        b for b in ocr_boxes 
        if b.ocr_id not in used_ocr_ids and b.best_amount() is not None
    ]
    
    # Calculate confidence distribution
    confidences = [m.match_confidence for m in matches if m.matched_ocr_ids]
    avg_confidence = float(np.mean(confidences)) if confidences else 0.0
    
    return {
        'match_rate': matched / total_master if total_master > 0 else 0.0,
        'unmatched_master': unmatched,
        'orphaned_ocr_count': len(orphaned_ocr),
        'avg_confidence': avg_confidence,
        'potential_issues': [
            {
                'ocr_id': b.ocr_id,
                'text': b.text[:50],
                'amount': b.best_amount(),
                'file': b.src_file
            }
            for b in orphaned_ocr[:5]  # First 5 potential issues
        ]
    }


# -------------------------------
# ========== ANNOTATION =========
# -------------------------------
def annotate_on_raster(raster_path: str,
                       ocr_boxes: Dict[int, OCRBox],
                       labels_for_ids: Dict[int, str],
                       out_path: str):
    """
    Draw boxes around matched OCR boxes and place label text above.
    Enhanced with confidence indicators.
    """
    img = cv2.imread(raster_path)
    if img is None or img.size == 0:
        log(f"Failed to open raster for annotation: {raster_path}", logging.WARNING)
        return
    
    for oid, label in labels_for_ids.items():
        b = ocr_boxes.get(oid)
        if not b:
            continue
        
        pts = poly_to_int_pts(b.bbox)
        
        # Choose color based on confidence
        if b.ocr_quality_score > 0.8:
            color = (0, 255, 0)  # Green for high confidence
        elif b.ocr_quality_score > 0.5:
            color = (0, 255, 255)  # Yellow for medium confidence
        else:
            color = (0, 0, 255)  # Red for low confidence
        
        cv2.polylines(img, [pts], isClosed=True, color=color, thickness=2)
        x, y = pts[0][0]
        
        # Add label with confidence indicator
        display_label = f"{label} ({b.ocr_quality_score:.2f})"
        cv2.putText(img, display_label, (int(x), int(y) - 10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2, cv2.LINE_AA)
        
        # Add amount if available
        best_amount = b.best_amount()
        if best_amount:
            cv2.putText(img, f"${best_amount:.2f}", (int(x), int(y) - 30),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 1, cv2.LINE_AA)
    
    cv2.imwrite(out_path, img)


# -------------------------------
# ========= MAIN DRIVER =========
# -------------------------------
def process_project(project_path: str, project_name: str, ocr_engine: OCREngine):
    log(f"\n{'='*60}")
    log(f"Processing project: {project_name}")
    log(f"{'='*60}")
    
    loader = Loader(CFG, ocr_engine)
    
    # 1) Find master
    master_file, master_type = loader.find_master_file(project_path)
    master_df = pd.DataFrame()
    if master_file:
        log(f"Master detected: {master_file} ({master_type})")
        if master_type == "excel":
            master_df = loader.read_master_excel(master_file)
        elif master_type == "pdf":
            master_df = loader.read_master_pdf_to_df(master_file)
            if CFG.SAVE_CONVERTED_PDF_MASTER_AS_EXCEL and not master_df.empty:
                out_master_xlsx = os.path.join(project_path, "converted_expense.xlsx")
                try:
                    master_df.to_excel(out_master_xlsx, index=False)
                    log(f"Converted PDF master to Excel: {out_master_xlsx}")
                except Exception as e:
                    log(f"Failed to save converted master: {e}", logging.WARNING)
    else:
        log("No master file found.", logging.WARNING)
    
    # Normalize master
    if not master_df.empty:
        master_df = normalize_master_df(master_df)
    else:
        master_df = pd.DataFrame(columns=["Item Amount", "Receipt Flag"])
    
    # 2) OCR all other files with parallel processing
    ocr_boxes: List[OCRBox] = []
    
    # Collect files to process
    files_to_process = []
    for fn in os.listdir(project_path):
        fp = os.path.join(project_path, fn)
        if not os.path.isfile(fp):
            continue
        if master_file and os.path.abspath(fp) == os.path.abspath(master_file):
            continue
        _, ext = os.path.splitext(fn.lower())
        if (ext in CFG.IMAGE_EXTS or ext in CFG.PDF_EXTS or ext in CFG.EXCEL_EXTS):
            files_to_process.append((fp, ext, project_name))
    
    log(f"Found {len(files_to_process)} files to process")
    
    # Process in batches with progress bar
    with tqdm(total=len(files_to_process), desc=f"OCR Processing - {project_name}") as pbar:
        with ThreadPoolExecutor(max_workers=CFG.MAX_WORKERS) as executor:
            batches = []
            for i in range(0, len(files_to_process), CFG.BATCH_SIZE):
                batch = files_to_process[i:i+CFG.BATCH_SIZE]
                batches.append(batch)
            
            futures = []
            future_to_batch_size = {}
            for batch in batches:
                future = executor.submit(loader.process_file_batch, batch, project_name)
                futures.append(future)
                future_to_batch_size[future] = len(batch)
            
            for future in as_completed(futures):
                try:
                    batch_results = future.result(timeout=300)  # 5 minute timeout per batch
                    ocr_boxes.extend(batch_results)
                    pbar.update(future_to_batch_size[future])
                except Exception as e:
                    log(f"Batch processing failed: {e}", logging.ERROR)
                    pbar.update(future_to_batch_size[future])
    
    log(f"OCR complete: {len(ocr_boxes)} text boxes detected")
    
    # 3) Build OCR DataFrame
    ocr_records = []
    for b in ocr_boxes:
        best_amt = b.best_amount_with_context()
        ocr_records.append({
            "OCR ID": b.ocr_id,
            "File": b.src_file,
            "Source Type": b.src_type,
            "Page/Image": b.page_label,
            "PDF Page": b.page_num,
            "Text": b.text[:100] + "..." if len(b.text) > 100 else b.text,
            "Confidence": f"{b.conf:.2f}",
            "Quality Score": f"{b.ocr_quality_score:.2f}",
            "BBox": json.dumps(b.bbox),
            "Raster W": b.raster_w,
            "Raster H": b.raster_h,
            "Amount Candidates": json.dumps([a['value'] for a in b.amount_candidates]),
            "Best Amount": b.best_amount(),
            "Amount Context": best_amt['context'] if best_amt else "",
            "Raster Path": b.raster_path or ""
        })
    df_ocr = pd.DataFrame(ocr_records)
    
    # 4) Match
    log("Performing amount matching...")
    matches = greedy_match_master_to_ocr(
        master_df, ocr_boxes,
        tolerance=CFG.AMOUNT_TOLERANCE,
        max_len=CFG.COMBO_MAX_LEN,
        deduplicate=CFG.DEDUPLICATE_MATCHES
    )
    
    # 5) Validate matches
    validation_results = validate_matches(matches, ocr_boxes)
    log(f"Match validation: {validation_results['match_rate']*100:.1f}% match rate")
    if validation_results['potential_issues']:
        log(f"Potential issues: {len(validation_results['potential_issues'])} orphaned OCR boxes", logging.WARNING)
    
    # 6) Build output tables
    ocr_lookup: Dict[int, OCRBox] = {b.ocr_id: b for b in ocr_boxes}
    summary_rows = []
    
    for m in matches:
        linked_boxes = [ocr_lookup[oid] for oid in m.matched_ocr_ids if oid in ocr_lookup]
        linked_files = [f"{b.src_file} [{b.page_label}]" for b in linked_boxes]
        linked_amounts = [b.best_amount() for b in linked_boxes]
        
        summary_rows.append({
            "Master Row": m.master_index,
            "Item Amount": m.master_amount,
            "Receipt Flag": str(master_df.iloc[m.master_index].get("Receipt Flag", "")) if m.master_index in master_df.index else "",
            "Matched OCR IDs": m.matched_ocr_ids,
            "Matched Files/Pages": "; ".join(linked_files),
            "Matched Amounts": linked_amounts,
            "Match Confidence": f"{m.match_confidence:.2f}" if m.matched_ocr_ids else "",
            "Reconciliation Notes": m.master_note
        })
    
    df_summary = pd.DataFrame(summary_rows)
    
    # 7) Write Excel
    out_xlsx = os.path.join(project_path, "reconciliation_result.xlsx")
    try:
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            if not master_df.empty:
                master_df.to_excel(writer, index=False, sheet_name="Master (Normalized)")
            if not df_ocr.empty:
                df_ocr.to_excel(writer, index=False, sheet_name="OCR Details")
            df_summary.to_excel(writer, index=False, sheet_name="Reconciliation Summary")
            
            # Add validation sheet
            validation_df = pd.DataFrame([{
                'Metric': k,
                'Value': v if not isinstance(v, list) else json.dumps(v)
            } for k, v in validation_results.items()])
            validation_df.to_excel(writer, index=False, sheet_name="Validation")
        
        log(f"Wrote reconciliation Excel: {out_xlsx}")
    except Exception as e:
        log(f"Failed to write result Excel: {e}", logging.ERROR)
    
    # 8) Annotate images
    if CFG.WRITE_ANNOTATED_IMAGES and ocr_boxes:
        labels_by_raster: Dict[str, Dict[int, str]] = {}
        for m_idx, m in enumerate(matches, start=1):
            if not m.matched_ocr_ids:
                continue
            if len(m.matched_ocr_ids) == 1:
                seq_labels = [str(m_idx)]
            else:
                seq_labels = [f"{m_idx}{chr(65+i)}" for i in range(len(m.matched_ocr_ids))]
            
            for oid, label in zip(m.matched_ocr_ids, seq_labels):
                b = ocr_lookup.get(oid)
                if not b or not b.raster_path:
                    continue
                labels_by_raster.setdefault(b.raster_path, {})[oid] = label
        
        # Create annotated images
        for raster_path, labels_map in labels_by_raster.items():
            base_dir = os.path.dirname(raster_path)
            base_name = os.path.basename(raster_path)
            out_img = os.path.join(base_dir, f"annotated_{base_name}")
            try:
                annotate_on_raster(raster_path, ocr_lookup, labels_map, out_img)
                log(f"Wrote annotated image: {out_img}")
            except Exception as e:
                log(f"Failed to annotate {raster_path}: {e}", logging.WARNING)
    
    # 9) Cleanup
    loader.cleanup_temp_files()
    
    log(f"\n{'='*60}")
    log(f"Completed: {project_name}")
    log(f"{'='*60}")


def main():
    """Main entry point"""
    print("=" * 60)
    print("Expense Reconciliation System")
    print("=" * 60)
    
    # Basic param checks
    if not os.path.isdir(CFG.BASE_FOLDER):
        print(f"ERROR: BASE_FOLDER does not exist: {CFG.BASE_FOLDER}")
        sys.exit(1)
    
    print(f"Base folder: {CFG.BASE_FOLDER}")
    print(f"OCR Languages: {CFG.OCR_LANGS}")
    print(f"Max workers: {CFG.MAX_WORKERS}")
    print("=" * 60)
    
    # Initialize OCR
    try:
        print("Initializing OCR engine...")
        ocr_engine = OCREngine(CFG.OCR_LANGS, gpu=False)
        print("OCR engine initialized successfully")
    except Exception as e:
        print(f"ERROR: Failed to initialize EasyOCR: {e}")
        print("\nTroubleshooting tips:")
        print("1. Install easyocr: pip install easyocr")
        print("2. Check internet connection (first run downloads models)")
        print("3. Try with GPU=False if you don't have CUDA")
        sys.exit(1)
    
    # Find and process projects
    projects = [d for d in os.listdir(CFG.BASE_FOLDER) 
                if os.path.isdir(os.path.join(CFG.BASE_FOLDER, d))]
    
    if not projects:
        print("No project folders found in base directory.")
        sys.exit(0)
    
    print(f"\nFound {len(projects)} project(s) to process:")
    for i, project in enumerate(projects, 1):
        print(f"  {i}. {project}")
    
    print("\nStarting processing...")
    
    # Process each project
    success_count = 0
    for project in projects:
        proj_path = os.path.join(CFG.BASE_FOLDER, project)
        try:
            process_project(proj_path, project, ocr_engine)
            success_count += 1
        except Exception as e:
            traceback.print_exc()
            print(f"ERROR: Unhandled error in project: {project}")
    
    print("\n" + "=" * 60)
    print(f"Processing complete! {success_count}/{len(projects)} projects successful")
    print(f"Check '{CFG.LOG_FILE}' for detailed logs")
    print("=" * 60)


if __name__ == "__main__":

    main()
