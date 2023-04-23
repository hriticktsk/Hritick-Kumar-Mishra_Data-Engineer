#!/usr/bin/env python
# coding: utf-8

# In[4]:


import csv
import xml.etree.ElementTree as ET


def xml_to_csv(xml_file_path, csv_file_path):
    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        
        # Write the header row
        header_row = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp',
                      'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']
        writer.writerow(header_row)
        
        # Iterate over the XML file using an event-driven parser
        for event, elem in ET.iterparse(xml_file_path, events=('start', 'end')):
            if event == 'start':
                # Initialize the row data
                row_data = []
                
                # Parse the data from the XML element
                if elem.tag == 'FinInstrm':
                    FinInstrmGnlAttrbts_Id = elem.find('Id').text
                    FinInstrmGnlAttrbts_FullNm = elem.find('FullNm').text
                    FinInstrmGnlAttrbts_ClssfctnTp = elem.find('ClssfctnTp').text
                    FinInstrmGnlAttrbts_CmmdtyDerivInd = elem.find('CmmdtyDerivInd').text
                    FinInstrmGnlAttrbts_NtnlCcy = elem.find('NtnlCcy').text
                    Issr = elem.find('Issr').text
                    
                    # Add the data to the row
                    row_data.append(FinInstrmGnlAttrbts_Id)
                    row_data.append(FinInstrmGnlAttrbts_FullNm)
                    row_data.append(FinInstrmGnlAttrbts_ClssfctnTp)
                    row_data.append(FinInstrmGnlAttrbts_CmmdtyDerivInd)
                    row_data.append(FinInstrmGnlAttrbts_NtnlCcy)
                    row_data.append(Issr)
                    
                    # Write the row to the CSV file
                    writer.writerow(row_data)
                    
                    # Clear the XML element to free up memory
                    elem.clear()
                    
                # Print a progress update every 10000 elements
                if elem.tag == 'FinInstrm' and int(elem.attrib['count']) % 10000 == 0:
                    print(f'Processed {elem.attrib["count"]} elements...')
    
    print(f'Successfully converted {xml_file_path} to {csv_file_path}!')

# Example usage
xml_file_path = 'E:\DLTINS_20210117_01of01.xml'
csv_file_path = 'E:\largefile.csv'
xml_to_csv(xml_file_path, csv_file_path)


# In[ ]:


import boto3

def lambda_handler(event, context):
    # Replace with your S3 bucket name and CSV file name
    bucket_name = 'my-bucket'
    file_name = 'my-file.csv'
    
    # Replace with the local file path of the CSV file 
    local_file_path = 'E:\largefile.csv'
    
    # Create an S3 client and upload the file
    s3 = boto3.client('s3')
    with open(local_file_path, 'rb') as f:
        s3.upload_fileobj(f, bucket_name, file_name)
    
    return {
        'statusCode': 200,
        'body': 'CSV file uploaded to S3'
    }

