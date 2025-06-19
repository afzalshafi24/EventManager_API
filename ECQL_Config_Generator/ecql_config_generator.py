import pandas as pd
import sys
from config import COLUMNS_TO_SAVE, BATCHFILE_TEMPLATE, BATCHFILE_TEMPLATE_REPLACEMENT_KEY, POST_REQUEST_SCRIPT

def batch_script_maker(row_data):
    #Tool to make a batch script 
    
    batch_script = row_data['#AppName']
    scid = row_data['#SCID']
    metric_name = row_data['#METRIC_NAME']
    uri = row_data['#URI']
    source = 'ECQL'

    new_key = f"{POST_REQUEST_SCRIPT} -scid {scid} -metric_name '{metric_name}' -source '{source}' -uri '{uri}'"
    print(new_key)
    #Read in the batch file template
    # Read the content of the batch file
    with open(BATCHFILE_TEMPLATE, 'r') as f:
        template_data = f.read()

    #Replace Template Text with Actual Text
    updated_data = template_data.replace(BATCHFILE_TEMPLATE_REPLACEMENT_KEY, new_key)

    #Write new text to new batch script
    with open(batch_script, 'w') as f:
        f.write(updated_data)



def excel_to_txt(input_excel_file):
    # Read the Excel file
    df = pd.read_excel(input_excel_file)

    # Check if the DataFrame has 11 columns
    if df.shape[1] != 14:
        raise ValueError("The input Excel file must have exactly 11 columns.")

    #Extract out unique scids
    unique_scids = df['#SCID'].unique()

    for scid in unique_scids:
        output_txt_file = f'geo{scid}_ecql_cfg.txt'
        df_scid = df[df['#SCID'] == scid]

        # Write the DataFrame to a semicolon-separated text file in Unix LF format
        with open(output_txt_file, mode='w', newline='\n', encoding='utf-8') as txt_file:
            df_scid[COLUMNS_TO_SAVE].to_csv(output_txt_file, sep=';', index=False, header=True)

        #Create the batch script
        for idx, row_data in df_scid.iterrows():
            batch_script_maker(row_data)

if __name__ == "__main__":
    input_excel = sys.argv[1]
    
    excel_to_txt(input_excel)
   