# Expense_Track
This script automates the process of matching expense receipts including images , PDFs , images attached in Excel files , with master expense report (from 3rd parties) using OCR. It is designed for accounting / finance departments to streamline expense verification. 

Create a folder for Expense verification(and drag the SUBfolders into the folder) 
Each SUBfolder shold have a master file (concluding the item amounts) and the receipts images for verification. 
The script contains a built-in logic for calculation (A $50-worth of item amount can be verified with two $25 receipts) 
The sciprt will automatically track the location of where OCR found the number in those receipts, and annotate them when the match between the receipt amount and the master list is found 



1.Pip install all the imports (if you haven't already)
2.Make sure to correct the path for BASE_FOLDER (Create a file for getting all the expense files in) 
3.Make sure you have all the receipts and the master file (provided by yourself or 3rd party) for verification 
4.Make sure the files are in jpeg, pdf, png and Excels only.

