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


# Initialize OCR reader
reader = easyocr.Reader(['ch_tra','en'])
base_folder = PASTE YOUR FILE PATH HERE 

keywords = ["exp", "expense", "report"]

def is_master_file(filename):
    name = filename.lower()
    return any(kw in name for kw in keywords)

def convert_pdf_to_excel(pdf_path, output_path):
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                tables.append(pd.DataFrame(table))
    if tables:
        df = pd.concat(tables, ignore_index=True)
        df.to_excel(output_path, index=False)
        print(f"Converted PDF master to Excel: {output_path}")
        return output_path
    else:
        print("No tables found in PDF master.")
        return None


def find_master_file(project_path):
    excel_master = None
    pdf_master = None
    for filename in os.listdir(project_path):
        filepath = os.path.join(project_path, filename)
        ext = os.path.splitext(filename)[1].lower()
        if is_master_file(filename):
            if ext in [".xlsx", ".xlsm"]:
                excel_master = filepath
            elif ext == ".pdf":
                pdf_master = filepath
    if excel_master:
        return excel_master, "excel"
    elif pdf_master:
        return pdf_master, "pdf"
    else:
        return None, None
def process_pdf(filepath, filename):
    rows = []
    with pdfplumber.open(filepath) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            pil_image = page.to_image(resolution=300).original
            np_image = np.array(pil_image)
            results = reader.readtext(np_image)
            rows.extend([[filename, f"Page {page_num}", text, prob, bbox] 
                         for bbox, text, prob in results])
    return rows

def process_image(filepath, filename, page="Image"):
    # Let EasyOCR read directly from path string
    results = reader.readtext(filepath)
    return [[filename, page, text, prob, bbox] for bbox, text, prob in results]

def process_image(filepath, filename, page="Image"):
    ext = os.path.splitext(filepath)[1].lower()
    if ext not in [".png", ".jpg", ".jpeg"]:
        print(f"⚠️ Skipping non-image file: {filepath}")
        return []

    np_img = cv2.imread(filepath)
    if np_img is None or np_img.size == 0:
        print(f"⚠️ Failed to load image: {filepath}")
        return []

    np_img = cv2.cvtColor(np_img, cv2.COLOR_BGR2RGB)

    try:
        results = reader.readtext(np_img)
    except Exception as e:
        print(f"⚠️ OCR failed for {filepath}: {e}")
        return []

    return [[filename, page, text, prob, bbox] for bbox, text, prob in results]


def process_excel(filepath, filename, folder):
    rows = []
    wb = load_workbook(filepath)

    for ws in wb.worksheets:
        if hasattr(ws, "_images") and ws._images:
            for idx, img in enumerate(ws._images, start=1):
                if isinstance(img, XLImage):
                    try:
                        img_bytes = img._data()
                        pil_img = PILImage.open(io.BytesIO(img_bytes))

                        # Build safe filename
                        safe_sheet = ws.title.replace(" ", "_")
                        pdf_path = os.path.join(folder, f"{filename}_{safe_sheet}_img{idx}.pdf")

                        # Save image directly as PDF
                        pil_img.save(pdf_path, "PDF")
                        print(f"✅ Saved image as PDF: {pdf_path}")

                        # OCR the image (still needs a raster format, so convert to PNG in memory)
                        np_img = np.array(pil_img)
                        results = reader.readtext(np_img)
                        rows.extend([[filename, f"{ws.title} - Embedded Image {idx}", text, prob, bbox]
                                     for bbox, text, prob in results])
                    except Exception as e:
                        print(f"⚠️ Failed to process image in {filename}, sheet {ws.title}: {e}")
    return rows





def extract_amounts(text):
    cleaned = text.replace(",", "")
    cleaned = re.sub(r"(HK\$|\$|\u20ac|\u00a3|\u00a5|\(|\)|小寫)", "", cleaned)
    cleaned = re.sub(r"\s*\.\s*", ".", cleaned)
    cleaned = cleaned.replace("．", ".").replace("\u200b", "").replace("\u00a0", "")
    matches = re.findall(r"-?\d+(?:\.\d+)?", cleaned)
    results = []
    for m in matches:
        try:
            # Convert to float and normalize to positive
            results.append(abs(float(m)))
        except:
            continue
    return results


def get_master_table_excel(filepath):
    return pd.read_excel(filepath)

def get_master_table_pdf(filepath):
    tables = []
    with pdfplumber.open(filepath) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                tables.append(pd.DataFrame(table))
    if tables:
        return pd.concat(tables, ignore_index=True)
    return pd.DataFrame()

def find_combinations(numbers, target, tolerance=0.01, max_len=3):
    results = []
    numbers = sorted(numbers, reverse=True)
    def backtrack(start, current_set, current_sum):
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
    return [results[0]]

def annotate_image(image_path, ocr_results, labels_map, output_path, file_name):
    img = cv2.imread(image_path)
    if img is None:
        return

    for row_idx, row in enumerate(ocr_results):
        filename, page, text, prob, bbox = row
        key = (filename, page, row_idx)
        if key in labels_map and filename == file_name:
            label = labels_map[key]

            # Use bounding box from OCR (column C)
            pts = np.array(bbox, dtype=np.int32).reshape((-1, 1, 2))

            # Draw bounding box in black
            cv2.polylines(img, [pts], isClosed=True, color=(0, 0, 0), thickness=2)

            # Place label above the box
            x, y = pts[0][0]
            cv2.putText(img, label, (int(x), int(y) - 10),
                        cv2.FONT_HERSHEY_SIMPLEX, 1.5, (0, 0, 0), 3, cv2.LINE_AA)

    cv2.imwrite(output_path, img)

# Main loop
for project in os.listdir(base_folder):
    project_path = os.path.join(base_folder, project)
    if os.path.isdir(project_path):
        print(f"\nProcessing project: {project}")
        all_results = []

        master_file, master_type = find_master_file(project_path)
        master_df = pd.DataFrame()
        if master_file:
            if master_type == "excel":
                master_df = get_master_table_excel(master_file)
            elif master_type == "pdf":
                converted_path = os.path.join(project_path, "converted_expense.xlsx")
                converted_excel = convert_pdf_to_excel(master_file, converted_path)
                if converted_excel:
                  master_df = get_master_table_excel(converted_excel)

        for filename in os.listdir(project_path):
            filepath = os.path.join(project_path, filename)
            ext = os.path.splitext(filename)[1].lower()
            if master_file and filepath == master_file:
                continue
            if ext in [".png", ".jpg", ".jpeg"]:
                all_results.extend(process_image(filepath, filename))
            elif ext == ".pdf":
                all_results.extend(process_pdf(filepath, filename))
            elif ext in [".xlsx", ".xlsm"]:
                all_results.extend(process_excel(filepath, filename, project_path))

        if all_results:
            df_ocr = pd.DataFrame(all_results, columns=["File","Page/Image","Text","Confidence","BBox"])
            df_ocr["Amount"] = df_ocr["Text"].apply(
                lambda t: extract_amounts(t)[0] if extract_amounts(t) else None
            )

            if not master_df.empty:
                combined = pd.concat([df_ocr, master_df], axis=1)
            else:
                combined = df_ocr

            rename_map = {}
            if "Unnamed: 2" in combined.columns:
                rename_map["Unnamed: 2"] = "Item Amount"
            if "Unnamed: 10" in combined.columns:
                rename_map["Unnamed: 10"] = "Receipt Identifications"
            combined = combined.rename(columns=rename_map)

            reconciliation_notes = []
            labels_map = {}
            item_counter = 0

            for idx, row in combined.iterrows():
                valH = str(row.get("Item Amount", "")).strip()
                receipt_flag = str(row.get("Receipt Identifications", "")).strip().upper()

                note = ""
                try:
                    numH = float(valH.replace(",", "")) if valH not in ["", "nan"] else None
                except ValueError:
                    numH = None

                if receipt_flag == "Y":
                    item_counter += 1
                    if numH is not None:
                        candidates = combined["Amount"].dropna()
                        try:
                            candidates = candidates.astype(float).tolist()
                        except:
                            candidates = []
                        combos = find_combinations(candidates, numH)
                        if combos:
                            best_combo = combos[0]
                            note = f"Match via E: {best_combo}"
                            for j, val in enumerate(best_combo):
                                suffix = chr(65+j) if len(best_combo) > 1 else ""
                                label = f"{item_counter}{suffix}"
                                match_rows = combined[combined["Amount"] == val]
                                if not match_rows.empty:
                                    r = match_rows.iloc[0]
                                    labels_map[(r["File"], r["Page/Image"], r.name)] = label
                        else:
                            note = f"No combination sums to {numH}"
                    else:
                        note = "Missing numeric values"
                elif receipt_flag == "N":
                    note = "Skipped"
                else:
                    note = "No receipt flag"

                reconciliation_notes.append(note)

            combined["Reconciliation Notes"] = reconciliation_notes

            print("\n--- Reconciliation Preview ---")
            print(combined[["File","Page/Image","Amount","Item Amount","Receipt Identifications","Reconciliation Notes"]].head(20))
            output_path = os.path.join(project_path, "result.xlsx")
            combined.to_excel(output_path, index=False)
            print(f"Combined OCR + Master with reconciliation saved to {output_path}")

            # Annotate images and PDFs
            for filename in os.listdir(project_path):
                filepath = os.path.join(project_path, filename)
                ext = os.path.splitext(filename)[1].lower()

                # Handle raster images
                if ext in [".png", ".jpg", ".jpeg"]:
                    output_img = os.path.join(project_path, f"annotated_{filename}")
                    annotate_image(filepath, all_results, labels_map, output_img, filename)

                # Handle PDFs by rendering each page to image and annotating
                elif ext == ".pdf":
                    with pdfplumber.open(filepath) as pdf:
                        for page_num, page in enumerate(pdf.pages, start=1):
                            pil_image = page.to_image(resolution=300).original
                            np_image = np.array(pil_image)
                            temp_path = os.path.join(project_path, f"{filename}_page{page_num}.png")
                            cv2.imwrite(temp_path, cv2.cvtColor(np_image, cv2.COLOR_RGB2BGR))
                            output_img = os.path.join(project_path, f"annotated_{filename}_page{page_num}.png")
                            annotate_image(temp_path, all_results, labels_map, output_img, filename)

            print(f"Annotated images saved in {project_path}")
