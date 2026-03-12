# Expense Reconciliation System

This script automates the process of matching expense receipts (images, PDFs, and images embedded in Excel files) with master expense reports using Optical Character Recognition (OCR). It is designed for accounting and finance departments to streamline expense verification.

## 📋 How It Works

The script automatically:
1. **Scans** each subfolder for a master expense file (containing item amounts)
2. **OCR-scans** all receipts in the folder (images, PDFs, Excel embedded images)
3. **Matches** receipt amounts to master list items using intelligent combination logic
4. **Annotates** receipts showing exactly which amount matched which expense item

**Smart Features:**
- Built-in combination logic (e.g., a $50 expense can be matched with two $25 receipts)
- Automatically filters out noise (dates, phone numbers, invoice numbers, etc.)
- Tracks where OCR found numbers in receipts and annotates them
- Color-codes annotations by confidence (green = high, yellow = medium, red = low)

## 🗂️ Folder Structure

Create a main folder for expense verification, then add subfolders for each project or period:

BASE_FOLDER/
├── FILE1/
│ ├── expense_report.xlsx (master file - must contain "exp" in filename)
│ ├── receipt_1.jpg
│ ├── receipt_2.pdf
│ └── statement_with_images.xlsx
├── FILE2/
│ ├── master_expense.pdf (master file - PDF with tables)
│ └── receipts/
├── FILE3/
│ ├── exp_report.xlsx (master file)
│ ├── receipt_a.png
│ └── receipt_b.jpeg
└── ...


**Each subfolder MUST contain:**
- **One master file** (Excel or PDF) with "exp", "expense", or "report" in the filename
- **Receipt files** (images, PDFs, or Excel files with embedded images)
-  Important Notes
Master files must contain "exp", "expense", or "report" in the filename

The script automatically identifies amount columns (usually "Unnamed: 2" in Excel exports)

Receipt flags with "Y" trigger matching attempts

The script filters out common noise:

Dates (2024-01-15, 15/01/2024)

Phone numbers

Invoice/order numbers (INV-12345, ORD-789)

Tax IDs and reference numbers

## 🚀 Getting Started

### Prerequisites

Install the required packages,

Adjust other settings as needed (optional):

BASE_FOLDER = r"C:\Your\Path\Here"

OCR_MIN_CONF = 0.40           # Minimum confidence for OCR (0-1)
AMOUNT_TOLERANCE = 0.01       # Tolerance for amount matching (±$0.01)
COMBO_MAX_LEN = 3              # Maximum number of receipts per expense
DEDUPLICATE_MATCHES = True     # Prevent reusing the same receipt for multiple expenses

This script was developed with assistance from AI coding tools to enhance reliability and features.



## 📄 License

MIT License - feel free to use and modify for your needs.





