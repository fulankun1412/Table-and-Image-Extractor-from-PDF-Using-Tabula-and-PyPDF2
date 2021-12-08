import PyPDF2
import os
from PIL import Image
import pandas as pd
import tabula as tb
import os
import pandas as pd
import PIL
import mysql.connector as sql

#### Extracting Data Table From PDF file
### Declare Variables
dirr = "C:/Users/afkaa/Google Drive/Multimatics/Data/UINSA/"
table_dir = "table_output"
list_pdf = []
list_table = []

### Create Folder if not exist
if os.path.isdir(table_dir) == False:
    os.mkdir(table_dir) 

### Find pdf file in designated Directory
for file in os.listdir(dirr):
    if file.startswith("Daftar Hadir UINSA") and file.endswith(".pdf"):
        print("Found File : " + file)
        list_pdf.append(os.path.join(dirr, file))

### Extract Table Data from listed Pdf File
for pdf in list_pdf:
    print("Extracting Table Data From", pdf)
    dfs = tb.read_pdf(pdf, pages='all')
    ## Convert to Pandas
    df = pd.concat(dfs)
    df = df.drop(columns=['Added Time', 'Referrer Name', 'Task Owner'])
    list_table.append(df)

    
### Create Separate File or Combine the whole table
## Separate File
num_table = 0
for table in list_table:
    ### Save to CSV
    table.to_csv(os.path.join(table_dir,"table-" + str(num_table) + ".csv"), index=False)
    print("Created CSV file Named : " + "table-" + str(num_table) + ".csv")
    num_table += 1

## Combine Table
#df_full = pd.concat(list_table)

### Save to CSV
#df_full.to_csv("combine.csv",index=False)
#print(df_full.dtypes)

### Declare Variable
img_out = "img_out"
table_image_dir = "tabel_image_output"
csv_input = []
row_input = 0
file_num = 0

### Indexing Table File
for file in os.listdir(os.path.join(os.getcwd(), table_dir)):
    if file.startswith("table") and file.endswith(".csv"):
        print("Found CSV file for input: " + file)
        csv_input.append(file)

### Check if Table Image Output Directory Created and Create it if False
if os.path.isdir(table_image_dir) == False:
    os.mkdir(table_image_dir)
        
### Check if Image Output Directory Created and Create it if False
if os.path.isdir(img_out) == False:
    os.mkdir(img_out)        
        
### Extracting Image from PDF by CSV list
for index in range(len(csv_input)):
    ## Read and Convert Column to Str
    df = pd.read_csv(csv_input[index])
    df = df.astype({"Tanda Tangan Jelas": str})
    
    pdf_input = PyPDF2.PdfFileReader(open(list_pdf[index], "rb"))
    
    print("Processing File " + csv_input[index] + " and " + list_pdf[index])
    
    n_pages = pdf_input.getNumPages()
    for page in range(n_pages):
        pages = pdf_input.getPage(page)
        xObject = pages['/Resources']['/XObject'].getObject()

        for obj in xObject:
            if xObject[obj]['/Subtype'] == '/Image':
                size = (xObject[obj]['/Width'], xObject[obj]['/Height'])
                data = xObject[obj].getData()
                if xObject[obj]['/ColorSpace'] == '/DeviceRGB':
                    mode = "RGB"
                else:
                    mode = "P"

                if xObject[obj]['/Filter'] == '/FlateDecode':
                    img = Image.frombytes(mode, size, data)
                    saving_name = img_out + '/' + "pdf-" + str(file_num) + "-" + obj[1:]  + ".png"
                    img.save(saving_name)
                    df.at[row_input, "Tanda Tangan Jelas"] = saving_name
                elif xObject[obj]['/Filter'] == '/DCTDecode':
                    img = open(img_out + '/' + "pdf-" + str(file_num) + "-" +  obj[1:] + ".jpg", "wb")
                    img.write(data)
                    img.close()
                elif xObject[obj]['/Filter'] == '/JPXDecode':
                    img = open(img_out + '/' + "pdf-" + str(file_num) + "-" +  obj[1:] + ".jp2", "wb")
                    img.write(data)
                    img.close()
            
                row_input += 1
    
    df.to_csv(os.path.join(table_image_dir,"hasil-" + str(file_num) + ".csv"),index=False)
    print("Saving CSV Index Image to : " + "hasil-" + str(file_num) + ".csv")
    row_input = 0
    file_num += 1

#### Percobaan Import Ke MySQL
def convertToBinaryData(filename):
    ## Convert digital data to binary format
    try:
        with open(filename, 'rb') as file:
            binaryData = file.read()
        return binaryData
    except:
        return "zero"

def insertToMySQL(dataOneRow):
    try:
        connection = sql.connect(host='localhost', 
                                database='mahasiswa', 
                                user='lanang_afkaar', 
                            password='1q2w3e4r5t')
        cursor = connection.cursor()
        sql_insert = """INSERT INTO UINSA
                        (NIM, NamaLengkap, ProgramStudi, 
                        KelasPelatihan, PelatihanHariKe, TandaTanganJelas)
                        VALUES (%s, %s, %s, %s, %s, %s)"""
        result  = cursor.execute(sql_insert, dataOneRow)
        connection.commit()
        return 1
    
    except sql.Error as error:
        print("Failed inserting BLOB data into MySQL table {}".format(error))
        return 0

## Convert data into tuple format
list_file = []
count = 0

for file in os.listdir(os.getcwd()):
    if file.startswith("hasil") and file.endswith(".csv"):
        print("Found File processing :" + file)
        list_file.append(file)
        
for file in list_file:
    df_mahasiswa = pd.read_csv(file)
    print("Processing Upload for :" + file)
    for index, row in df_mahasiswa.iterrows():
        NIM = row['NIM']
        NamaLengkap = row['Nama Lengkap']
        ProgramStudi = row['Program Studi']
        KelasPelatihan = row['Kelas Pelatihan']
        PelatihanHariKe = row['Pelatihan Hari Ke-']
        TandaTanganJelas = convertToBinaryData(row['Tanda Tangan Jelas'])
        tuple_data = (NIM, NamaLengkap, ProgramStudi, KelasPelatihan, PelatihanHariKe, TandaTanganJelas)
        counting = insertToMySQL(tuple_data)
        count = count + counting

print("{} Data and Tanda Tangan have been inserted successfully as a OLE into UINSA".format(count))