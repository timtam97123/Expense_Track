import easyocr
import pdfplumber
import os
import numpy as np
import pandas as pd
import re
import cv2
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as XLImage
from PIL import Image as PILImage
import io
import logging
from typing import List, Tuple, Optional, Any, Dict
from dataclasses import dataclass
from pathlib import Path

# -------------------------------
# =========== CONFIG ============
# -------------------------------
@dataclass
class Config:
    BASE_FOLDER: str = r"C:\Users\Desktop\EXPENSE"   #paste your file path here.
    MASTER_KEYWORDS: Tuple[str, ...] = ("exp", "expense", "report")
    IMAGE_EXTS: Tuple[str, ...] = (".png", ".jpg", ".jpeg")
    EXCEL_EXTS: Tuple[str, ...] = (".xlsx", ".xlsm")
    PDF_EXTS: Tuple[str, ...] = (".pdf",)
    OCR_LANGS: Tuple[str, ...] = ("ch_tra", "en")
    OCR_MIN_CONF: float = 0.40
    PDF_RENDER_DPI: int = 300
    AMOUNT_TOLERANCE: float = 0.01
    COMBO_MAX_LEN: int = 3
    CANDIDATE_AMOUNT_MIN: float = 0.1
    DEDUPLICATE_MATCHES: bool = True
    WRITE_ANNOTATED_IMAGES: bool = True
    SAVE_CONVERTED_PDF_MASTER_AS_EXCEL: bool = True
    LOG_FILE: str = "reconciliation.log"

CFG = Config()

# -------------------------------
# ======== LOGGING SETUP ========
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CFG.LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize OCR reader (once, globally)
reader = easyocr.Reader(list(CFG.OCR_LANGS))

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

# Cleaning currency / punctuation around numbers
_AMOUNT_CLEAN_RE = re.compile(
    r"(HK\$|\$|\u20ac|\u00a3|\u00a5|\(|\)|小寫|,|\u200b|\u00a0)",
    re.IGNORECASE
)

# --- Patterns for "noise" contexts (non-amount numbers) ---
DATE_RE = re.compile(
    r"\b("
    r"\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4}"
    r"|"
    r"\d{4}[\/\-\.]\d{1,2}[\/\-\.]\d{1,2}"
    r")\b"
)
TIME_RE = re.compile(r"\b\d{1,2}:\d{2}\b")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{6,}\d")
ORDER_RE = re.compile(r"\b(?:ORD|INV|INVOICE|NO\.?|REF|TKT|TICKET)\s*\d+", re.IGNORECASE)

NON_AMOUNT_KEYWORDS = [
    "TEL", "PHONE", "FAX", "INVOICE", "INV", "ORDER", "ORD", "REF", "NO.",
    "TICKET", "會員", "客戶號", "電話", "傳真"
]

def is_noise_context(text: str) -> bool:
    """
    Heuristic to decide if this OCR text block is likely NOT an amount line:
    - Contains date/phone/order-like patterns
    - Contains obvious non-amount keywords
    """
    if not text:
        return False
    t = text.upper()

    if DATE_RE.search(text):
        return True
    if TIME_RE.search(text):
        return True
    if PHONE_RE.search(text):
        return True
    if ORDER_RE.search(text):
        return True

    if any(kw in t for kw in NON_AMOUNT_KEYWORDS):
        return True

    return False

def looks_like_amount(val: float, text: str) -> bool:
    """
    Heuristic to decide if a numeric value is a plausible monetary amount.
    Avoids large IDs, codes, etc.
    """
    if val < CFG.CANDIDATE_AMOUNT_MIN:
        return False

    # Filter out obviously huge numbers (likely IDs/ticket numbers)
    if val > 200000:
        return False

    # If there is no decimal point AND value is large, likely an ID (e.g. 12345678)
    if "." not in text and val > 99999:
        return False

    # Basic upper bound for typical receipts (tune as needed)
    if val > 50000:
        return False

    return True

def extract_amounts(text: str) -> List[float]:
    """
    Extract numeric amounts from text, with noise filtering:
    - Ignore OCR lines that look like dates, phone numbers, order/ID lines, etc.
    - Only keep numbers that look like realistic amounts.
    """
    if not text:
        return []

    # If the text context looks like date/ID/phone/order/etc, ignore entirely
    if is_noise_context(text):
        return []

    cleaned = _AMOUNT_CLEAN_RE.sub("", text)
    cleaned = re.sub(r"\s*\.\s*", ".", cleaned)
    cleaned = cleaned.replace("．", ".")

    matches = re.findall(r"-?\d+(?:\.\d+)?", cleaned)
    out = []
    for m in matches:
        try:
            val = abs(float(m))
            if looks_like_amount(val, text):
                out.append(val)
        except Exception:
            continue
    return out

def poly_to_int_pts(bbox: List[List[float]]) -> np.ndarray:
    """Convert bbox to integer points for OpenCV"""
    pts = np.array(bbox, dtype=np.float32)
    pts = pts.reshape((-1, 1, 2)).astype(np.int32)
    return pts

# -------------------------------
# ======= MASTER FILE HANDLING =======
# -------------------------------
def is_master_file(filename: str) -> bool:
    """Check if file is a master expense report"""
    name = filename.lower()
    return any(kw in name for kw in CFG.MASTER_KEYWORDS)

def find_master_file(project_path: str) -> Tuple[Optional[str], Optional[str]]:
    """Find master file in project folder"""
    excel_master = None
    pdf_master = None
    for filename in os.listdir(project_path):
        filepath = os.path.join(project_path, filename)
        if not os.path.isfile(filepath):
            continue
        ext = os.path.splitext(filename)[1].lower()
        if is_master_file(filename):
            if ext in CFG.EXCEL_EXTS:
                excel_master = filepath
            elif ext in CFG.PDF_EXTS:
                pdf_master = filepath
    if excel_master:
        return excel_master, "excel"
    elif pdf_master:
        return pdf_master, "pdf"
    return None, None

def get_master_table_excel(filepath: str) -> pd.DataFrame:
    """Read master Excel file"""
    return pd.read_excel(filepath)

def get_master_table_pdf(filepath: str) -> pd.DataFrame:
    """Extract tables from PDF master"""
    tables = []
    with pdfplumber.open(filepath) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                tables.append(pd.DataFrame(table))
    if tables:
        return pd.concat(tables, ignore_index=True)
    return pd.DataFrame()

def convert_pdf_to_excel(pdf_path: str, output_path: str) -> Optional[str]:
    """Convert PDF tables to Excel"""
    df = get_master_table_pdf(pdf_path)
    if not df.empty:
        df.to_excel(output_path, index=False)
        logger.info(f"Converted PDF master to Excel: {output_path}")
        return output_path
    else:
        logger.warning("No tables found in PDF master.")
        return None

def normalize_master_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize master dataframe to find amount and receipt columns
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Try to find amount column (usually Unnamed: 2 or similar)
    amount_col = None
    for col in df.columns:
        col_str = str(col).lower()
        if "unnamed" in col_str and any(num in col_str for num in ["2", "3"]):
            amount_col = col
            break
        elif "amount" in col_str or "amt" in col_str:
            amount_col = col
            break
    
    # Try to find receipt flag column (usually Unnamed: 10 or similar)
    receipt_col = None
    for col in df.columns:
        col_str = str(col).lower()
        if "unnamed" in col_str and "10" in col_str:
            receipt_col = col
            break
        elif "receipt" in col_str or "identification" in col_str:
            receipt_col = col
            break
    
    # Rename if found
    rename_dict = {}
    if amount_col:
        rename_dict[amount_col] = "Item Amount"
    if receipt_col:
        rename_dict[receipt_col] = "Receipt Identifications"
    
    if rename_dict:
        df = df.rename(columns=rename_dict)
    
    return df

# -------------------------------
# ======= OCR PROCESSING =======
# -------------------------------
def process_image(filepath: str, filename: str, page: str = "Image") -> List[list]:
    """Process a single image file"""
    ext = os.path.splitext(filepath)[1].lower()
    if ext not in CFG.IMAGE_EXTS:
        return []

    np_img = cv2.imread(filepath)
    if np_img is None or np_img.size == 0:
        logger.warning(f"Failed to load image: {filepath}")
        return []

    np_img = cv2.cvtColor(np_img, cv2.COLOR_BGR2RGB)

    try:
        results = reader.readtext(np_img)
        results = [r for r in results if r[2] >= CFG.OCR_MIN_CONF]
    except Exception as e:
        logger.warning(f"OCR failed for {filepath}: {e}")
        return []

    return [[filename, page, text, prob, bbox] for bbox, text, prob in results]

def process_pdf(filepath: str, filename: str) -> List[list]:
    """Process a PDF file"""
    rows = []
    try:
        with pdfplumber.open(filepath) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                pil_image = page.to_image(resolution=CFG.PDF_RENDER_DPI).original
                np_image = np.array(pil_image)
                results = reader.readtext(np_image)
                results = [r for r in results if r[2] >= CFG.OCR_MIN_CONF]
                rows.extend([[filename, f"Page {page_num}", text, prob, bbox]
                            for bbox, text, prob in results])
    except Exception as e:
        logger.error(f"Failed to process PDF {filepath}: {e}")
    return rows

def process_excel(filepath: str, filename: str, folder: str) -> List[list]:
    """Process embedded images in Excel"""
    rows = []
    try:
        wb = load_workbook(filepath)
        for ws in wb.worksheets:
            if hasattr(ws, "_images") and ws._images:
                for idx, img in enumerate(ws._images, start=1):
                    if isinstance(img, XLImage):
                        try:
                            img_bytes = img._data()
                            pil_img = PILImage.open(io.BytesIO(img_bytes))
                            np_img = np.array(pil_img)
                            results = reader.readtext(np_img)
                            results = [r for r in results if r[2] >= CFG.OCR_MIN_CONF]
                            rows.extend([[filename, f"{ws.title} - Embedded Image {idx}", text, prob, bbox]
                                        for bbox, text, prob in results])
                        except Exception as e:
                            logger.warning(f"Failed to process image in {filename}, sheet {ws.title}: {e}")
    except Exception as e:
        logger.warning(f"Failed to read Excel {filepath}: {e}")
    return rows

# -------------------------------
# ======= MATCHING LOGIC =======
# -------------------------------
def find_combinations(numbers: List[float], target: float, 
                     tolerance: float = 0.01, max_len: int = 3) -> List[List[float]]:
    """Find combinations that sum to target"""
    results = []
    numbers = sorted(numbers, reverse=True)
    
    def backtrack(start: int, current_set: List[float], current_sum: float):
        if current_sum > target + tolerance:
            return
        if len(current_set) > max_len:
            return
        if abs(current_sum - target) < tolerance:
            results.append(list(current_set))
            return
        for i in range(start, len(numbers)):
            current_set.append(numbers[i])
            backtrack(i + 1, current_set, current_sum + numbers[i])
            current_set.pop()
    
    backtrack(0, [], 0)
    
    if not results:
        return []
    
    results.sort(key=lambda combo: (len(combo), [-x for x in combo]))
    return [results[0]]  # Return best match

# -------------------------------
# ======= ANNOTATION (UPDATED) =======
# -------------------------------
def annotate_image(
    image_path: str,
    ocr_results: List[list],
    matched_ocr_label_map: Dict[int, str],
    output_path: str,
    file_name: str
):
    """
    Annotate image using ONLY the item labels (1, 2, 3, 1A, 1B...).
    matched_ocr_label_map: {ocr_row_index in all_results -> label string}
    """
    img = cv2.imread(image_path)
    if img is None:
        return

    any_drawn = False

    for ocr_idx, label in matched_ocr_label_map.items():
        if ocr_idx >= len(ocr_results):
            continue

        filename, page, text, prob, bbox = ocr_results[ocr_idx]
        if filename != file_name:
            continue

        # Choose color based on OCR confidence
        if prob > 0.8:
            color = (0, 255, 0)  # Green - high confidence
        elif prob > 0.5:
            color = (0, 255, 255)  # Yellow - medium confidence
        else:
            color = (0, 0, 255)  # Red - low confidence

        pts = poly_to_int_pts(bbox)
        cv2.polylines(img, [pts], isClosed=True, color=color, thickness=2)

        x, y = pts[0][0]
        # ONLY draw the item label (no amount)
        cv2.putText(
            img,
            label,
            (int(x), int(y) - 10),
            cv2.FONT_HERSHEY_SIMPLEX,
            1.0,
            color,
            2,
            cv2.LINE_AA
        )

        any_drawn = True

    if any_drawn:
        cv2.imwrite(output_path, img)
        logger.debug(f"Annotated items in {output_path}")
    else:
        logger.debug(f"No labels drawn for {file_name}, skipping save")

# -------------------------------
# ======= MAIN PROCESSING =======
# -------------------------------
def process_project(project_path: str, project_name: str):
    """Process a single project folder"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing project: {project_name}")
    logger.info(f"{'='*60}")
    
    all_results = []
    
    # 1. Find master file
    master_file, master_type = find_master_file(project_path)
    master_df = pd.DataFrame()
    
    if master_file:
        logger.info(f"Master detected: {os.path.basename(master_file)} ({master_type})")
        if master_type == "excel":
            master_df = get_master_table_excel(master_file)
        elif master_type == "pdf":
            converted_path = os.path.join(project_path, "converted_expense.xlsx")
            converted_excel = convert_pdf_to_excel(master_file, converted_path)
            if converted_excel:
                master_df = get_master_table_excel(converted_excel)
    
    # 2. Process all other files
    files_processed = 0
    file_list = []
    for filename in os.listdir(project_path):
        filepath = os.path.join(project_path, filename)
        if not os.path.isfile(filepath):
            continue
        if master_file and os.path.abspath(filepath) == os.path.abspath(master_file):
            continue
            
        ext = os.path.splitext(filename)[1].lower()
        file_list.append((filepath, filename, ext))
    
    for i, (filepath, filename, ext) in enumerate(file_list, 1):
        logger.info(f"Processing {i}/{len(file_list)}: {filename}")
        
        if ext in CFG.IMAGE_EXTS:
            results = process_image(filepath, filename)
            all_results.extend(results)
            files_processed += 1
        elif ext in CFG.PDF_EXTS:
            results = process_pdf(filepath, filename)
            all_results.extend(results)
            files_processed += 1
        elif ext in CFG.EXCEL_EXTS:
            results = process_excel(filepath, filename, project_path)
            all_results.extend(results)
            files_processed += 1
    
    logger.info(f"Processed {files_processed} files, found {len(all_results)} text boxes")
    
    # 3. Create OCR DataFrame
    if not all_results:
        logger.warning("No OCR results found")
        return
    
    df_ocr = pd.DataFrame(all_results, columns=["File", "Page/Image", "Text", "Confidence", "BBox"])
    
    df_ocr["Amount"] = df_ocr["Text"].apply(
        lambda t: extract_amounts(t)[0] if extract_amounts(t) else None
    )
    
    # 4. Combine with master
    if not master_df.empty:
        master_df = normalize_master_df(master_df)
        
        master_rows = []
        for idx, row in master_df.iterrows():
            master_rows.append({
                "File": "MASTER",
                "Page/Image": "Expense Report",
                "Text": "",
                "Confidence": "",
                "BBox": "",
                "Amount": safe_float(row.get("Item Amount", None)),
                "Item Amount": safe_float(row.get("Item Amount", None)),
                "Receipt Identifications": row.get("Receipt Identifications", ""),
                "Original Index": idx,
                "OCR Index": None
            })
        
        ocr_rows = []
        for idx, row in df_ocr.iterrows():
            ocr_rows.append({
                "File": row["File"],
                "Page/Image": row["Page/Image"],
                "Text": row["Text"],
                "Confidence": row["Confidence"],
                "BBox": row["BBox"],
                "Amount": row["Amount"],
                "Item Amount": None,
                "Receipt Identifications": None,
                "Original Index": None,
                "OCR Index": idx   # link back to df_ocr / all_results
            })
        
        combined = pd.concat([pd.DataFrame(master_rows), pd.DataFrame(ocr_rows)], ignore_index=True)
    else:
        combined = df_ocr.copy()
        combined["Item Amount"] = None
        combined["Receipt Identifications"] = None
        combined["Original Index"] = None
        combined["OCR Index"] = combined.index
    
    # 5. Perform reconciliation + build item labels
    reconciliation_notes = []
    matched_combined_indices = set()      # indices into 'combined'
    matched_ocr_indices = set()           # indices into df_ocr / all_results
    matched_ocr_label_map: Dict[int, str] = {}  # {ocr_index -> label like "1", "1A"}
    item_counter = 0                      # item number (1,2,3,... based on master order)
    
    for idx, row in combined.iterrows():
        if pd.isna(row.get("Original Index")):
            reconciliation_notes.append("")
            continue
            
        valH = str(row.get("Item Amount", "")).strip()
        receipt_flag = str(row.get("Receipt Identifications", "")).strip().upper()
        
        note = ""
        try:
            numH = float(valH.replace(",", "")) if valH not in ["", "nan"] else None
        except ValueError:
            numH = None
        
        if receipt_flag == "Y" and numH is not None:
            # Collect available amounts from unmatched OCR rows
            available_amounts = []
            for ocr_idx, ocr_row in combined.iterrows():
                if pd.isna(ocr_row.get("Original Index")):
                    if ocr_idx not in matched_combined_indices:
                        amt = ocr_row.get("Amount")
                        if amt is not None and not pd.isna(amt):
                            available_amounts.append(amt)
            
            combos = find_combinations(available_amounts, numH, CFG.AMOUNT_TOLERANCE, CFG.COMBO_MAX_LEN)
            
            if combos:
                best_combo = combos[0]
                item_counter += 1  # new item number for this master row
                note = f"Match: {best_combo}"
                
                # Mark these OCR rows as matched and create labels like 1, 1A, 1B...
                multi = (len(best_combo) > 1)
                for j, val in enumerate(best_combo):
                    # Determine label for this particular receipt line
                    if not multi:
                        label = str(item_counter)           # e.g. "1"
                    else:
                        label = f"{item_counter}{chr(65 + j)}"  # e.g. "1A", "1B"
                    
                    # Find first unmatched OCR row with this amount
                    for ocr_idx, ocr_row in combined.iterrows():
                        if (pd.isna(ocr_row.get("Original Index")) and 
                            ocr_idx not in matched_combined_indices and
                            abs((ocr_row.get("Amount") or 0) - val) < 0.01):
                            
                            matched_combined_indices.add(ocr_idx)
                            
                            ocr_index = ocr_row.get("OCR Index")
                            if ocr_index is not None and not pd.isna(ocr_index):
                                ocr_index = int(ocr_index)
                                matched_ocr_indices.add(ocr_index)
                                matched_ocr_label_map[ocr_index] = label
                            break
            else:
                note = f"No match for {numH:.2f}"
        elif receipt_flag == "Y":
            note = "Missing numeric values"
        elif receipt_flag == "N":
            note = "Skipped"
        else:
            note = "No receipt flag"
        
        reconciliation_notes.append(note)
    
    combined["Reconciliation Notes"] = reconciliation_notes
    
    # 6. Save results
    output_path = os.path.join(project_path, "result.xlsx")
    try:
        combined.to_excel(output_path, index=False)
        logger.info(f"Saved results to: {output_path}")
    except Exception as e:
        logger.error(f"Failed to save Excel: {e}")
    
    # 7. Annotate images - ONLY FOR MATCHED OCR ITEMS with labels
    if CFG.WRITE_ANNOTATED_IMAGES and matched_ocr_label_map:
        logger.info(f"Annotating {len(matched_ocr_label_map)} matched OCR items...")
        annotated_count = 0
        
        # Determine which files have matches
        files_to_annotate = set()
        for idx in matched_ocr_label_map.keys():
            if idx < len(all_results):
                filename = all_results[idx][0]
                files_to_annotate.add(filename)
        
        logger.info(f"Found matches in {len(files_to_annotate)} files")
        
        for filename in files_to_annotate:
            filepath = os.path.join(project_path, filename)
            ext = os.path.splitext(filename)[1].lower()
            
            # Subset label map for this file
            file_label_map: Dict[int, str] = {
                i: lbl for i, lbl in matched_ocr_label_map.items()
                if i < len(all_results) and all_results[i][0] == filename
            }
            
            if not file_label_map:
                continue
            
            if ext in CFG.IMAGE_EXTS:
                output_img = os.path.join(project_path, f"annotated_{filename}")
                annotate_image(filepath, all_results, file_label_map, output_img, filename)
                if os.path.exists(output_img):
                    annotated_count += 1
                    logger.info(f"Annotated: {filename}")
                    
            elif ext in CFG.PDF_EXTS:
                try:
                    with pdfplumber.open(filepath) as pdf:
                        for page_num, page in enumerate(pdf.pages, start=1):
                            page_label = f"Page {page_num}"
                            page_label_map: Dict[int, str] = {
                                i: lbl for i, lbl in file_label_map.items()
                                if all_results[i][1] == page_label
                            }
                            if not page_label_map:
                                continue
                            
                            pil_image = page.to_image(resolution=CFG.PDF_RENDER_DPI).original
                            np_image = np.array(pil_image)
                            temp_path = os.path.join(project_path, f"temp_{filename}_page{page_num}.png")
                            cv2.imwrite(temp_path, cv2.cvtColor(np_image, cv2.COLOR_RGB2BGR))
                            output_img = os.path.join(project_path, f"annotated_{filename}_page{page_num}.png")
                            annotate_image(temp_path, all_results, page_label_map, output_img, filename)
                            if os.path.exists(temp_path):
                                os.remove(temp_path)
                            annotated_count += 1
                            logger.info(f"Annotated: {filename} page {page_num}")
                except Exception as e:
                    logger.warning(f"Failed to annotate PDF {filename}: {e}")
        
        logger.info(f"Created {annotated_count} annotated images")
    else:
        logger.info("No matches found - skipping annotation")

def main():
    """Main entry point"""
    print("=" * 60)
    print("Expense Reconciliation System")
    print("=" * 60)
    
    if not os.path.isdir(CFG.BASE_FOLDER):
        print(f"ERROR: Base folder not found: {CFG.BASE_FOLDER}")
        return
    
    print(f"Base folder: {CFG.BASE_FOLDER}")
    print(f"OCR Languages: {CFG.OCR_LANGS}")
    print("=" * 60)
    
    projects = [d for d in os.listdir(CFG.BASE_FOLDER) 
                if os.path.isdir(os.path.join(CFG.BASE_FOLDER, d))]
    
    if not projects:
        print("No project folders found.")
        return
    
    print(f"Found {len(projects)} project(s):")
    for i, project in enumerate(projects, 1):
        print(f"  {i}. {project}")
    
    print("\nStarting processing...")
    
    success_count = 0
    for project in projects:
        try:
            project_path = os.path.join(CFG.BASE_FOLDER, project)
            process_project(project_path, project)
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to process {project}: {e}", exc_info=True)
    
    print("\n" + "=" * 60)
    print(f"Processing complete! {success_count}/{len(projects)} successful")
    print(f"Check '{CFG.LOG_FILE}' for details")
    print("=" * 60)

if __name__ == "__main__":
    main()
``
