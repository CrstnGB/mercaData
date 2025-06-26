# 1. ENVIRONMENT SETUP

## Libraries import
import pdfplumber
import pandas as pd
from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv

# 2. DATA PROCESSING


def process_invoice(invoice_path: str):

    #Opening the file
    with pdfplumber.open(invoice_path) as pdf:
        for i, pagina in enumerate(pdf.pages):
            # Extracting text
            invoice_text = pagina.extract_text()
            #print(InvoiceTexto)
    
    ## Text import and processing
    
    # Split the plain text into lines
    invoice_lst = invoice_text.split('\n')
    # Find a word to identify the header
    header_idx = [idx for idx, element in enumerate(invoice_lst) if 'Descripción' in element][0]
    # Find a word to identify the end
    end_idx = [idx for idx, element in enumerate(invoice_lst) if 'TOTAL' in element][0]
    # Extracting the part of the invoice r elated to the purchase
    products_lst = invoice_lst[header_idx + 1:end_idx]
    products_lst_split = [row.split() for row in products_lst]
    
    # list for storing the data
    dataForDf = []
    # function for check if a str is float
    def isfloat(text):
      try:
          if ',' not in text:
            return False
          float(text.replace(',', '.'))
          return True
      except ValueError:
          return False
    
    # function to populate the data dict
    def populate_dict(qty, weight, product, price, unit_measure):
      dictRow = {
            'qty': qty,
            'weight': weight,
            'product': product,
            'price': price,
            'unit_measure': unit_measure
            }
      return dictRow
    
    for i, row in enumerate(products_lst_split):
      dictRow = {}
      # Selection of the interesting elements of the row
      qty = row[0]
      price = row[-1]
      # Check if the qty is a number
      if not qty.isnumeric():
        continue
      # Check if the price is actualy a float
      if isfloat(price):
        qty = int(qty)
        weight = None
        unit_measure = 'unit'
        if isfloat(row[-2]):
          lastProdIdx = -3
        else:
          lastProdIdx = -2
        product = ' '.join(row[1:lastProdIdx + 1])
        price = float(price.replace(',', '.'))
        dictRow = populate_dict(qty, weight, product, price, unit_measure)
      else:
        qty = int(qty)
        weight = products_lst_split[i + 1][0]
        weight = float(weight.replace(',', '.'))
        product = ' '.join(row[1:])
        price = products_lst_split[i + 1][-1]
        price = float(price.replace(',', '.'))
        unit_measure = products_lst_split[i + 1][1]
        dictRow = populate_dict(qty, weight, product, price, unit_measure)
    
      # Finally, append the dict to the data list
      dataForDf.append(dictRow)
    
    # Creation of the dataframe
    df_invoice = pd.DataFrame(dataForDf)
    # Adding unit_price calculated
    df_invoice['unit_price'] = round(df_invoice['price'] / df_invoice['qty'], 2)
    # Conversion of floats
    df_invoice['price'] = df_invoice['price'].apply(float)
    df_invoice['unit_price'] = df_invoice['unit_price'].apply(float)
    # Showing a sample
    df_invoice.head()
    
    ## Getting general invoice variables
    # Adress
    address = invoice_lst[1]
    # Postal Code
    postal_code = invoice_lst[2].split()[0]
    # city
    city = ' '.join(invoice_lst[2].split()[1:])
    # Phone number
    phone = invoice_lst[3].split()[-1]
    # Date
    invoice_dateText = ' '.join(invoice_lst[4].split()[:2])
    invoice_date = datetime.strptime(invoice_dateText, "%d/%m/%Y %H:%M")
    # OP
    op_number = int(invoice_lst[4].split()[-1].split(':')[-1])
    # Invoice ID
    invoice_number = invoice_lst[5].split()[-1]
    # card_number
    card_number_last_digits = int(invoice_lst[-6].split()[-1])
    # N.C
    nc_number = invoice_lst[-5].split()[1]
    # AUT
    auth_code = invoice_lst[-5].split()[3]
    # AID
    aid = invoice_lst[-4].split()[1]
    # ARC
    arc_code = invoice_lst[-4].split()[3]
    # Card_type
    card_type = invoice_lst[-2].split()[-1]
    
    '''
    print(f'address: {address}')
    print(f'postal_code: {postal_code}')
    print(f'city: {city}')
    print(f'phone: {phone}')
    print(f'invoice_date: {invoice_date}')
    print(f'op_number: {op_number}')
    print(f'invoice_number: {invoice_number}')
    print(f'nc_number: {nc_number}')
    print(f'auth_code: {auth_code}')
    print(f'aid: {aid}')
    print(f'arc_code: {arc_code}')
    print(f'card_type: {card_type}')
    '''
    
    # 3.EXPORTING DATA TO THE BBDD
    
    ## Connect to BBDD POSTGRESQL
    
    # Cargar las variables del archivo .env
    load_dotenv()
    
    # Obtener las variables
    host = os.getenv("DB_HOST")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT")
    
    # Conectivity Params
    connection = psycopg2.connect(
        host=host,
        database=dbname,
        user=user,
        password=password,
        port=port
    )
    
    # Create a cursor for consults
    cursor = connection.cursor()
    
    try:
    
        # Execute a test consult
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("Conectado a PostgreSQL:", version)
    
        ## Updating tables
        print('Actualizando tablas...')
    
        ### PRODUCTS
    
        # Create the SQL consult
        consult = 'SELECT * FROM products;'
    
        # Create the df directly from the consult
        df_products = pd.read_sql_query(consult, connection)
    
        # List of unique products
        unique_products = set(df_products['name'])
    
        # Check what product of the invoice already exists on the table
        df_invoice['product_on_bbdd'] = df_invoice['product'].map(lambda x: x in unique_products)
    
        # Filter by those products that are not in the table yet creating a set
        new_products = set(df_invoice['product'][df_invoice['product_on_bbdd'] == False])
    
        # Filter by those products that are in the table already creating a set
        existing_products = set(df_invoice['product'][df_invoice['product_on_bbdd'] == True])
    
        # Inserting new products into the bbdd (if there is any)
        if len(new_products) > 0:
            for product in new_products:
                consult = '''
            INSERT INTO products (name, unit_price, unit_measure)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO NOTHING;
            '''
                unit_price = float(df_invoice['unit_price'][df_invoice['product'] == product].values[0])
                unit_measure = df_invoice['unit_measure'][df_invoice['product'] == product].values[0]
                cursor.execute(consult, (product, unit_price, unit_measure))
    
        # Updating existing products into the bbdd (if there is any)
        if len(existing_products) > 0:
            for product in existing_products:
                consult = '''
            UPDATE products
            SET unit_price = %s, unit_measure = %s
            WHERE name = %s;
            '''
                unit_price = float(df_invoice['unit_price'][df_invoice['product'] == product].values[0])
                unit_measure = df_invoice['unit_measure'][df_invoice['product'] == product].values[0]
                cursor.execute(consult, (unit_price, unit_measure, product))
    
        ### STORES
    
        consult = '''
        INSERT INTO Stores (name, address, postal_code, city, phone)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (address) DO NOTHING
        RETURNING id;
        '''
    
        cursor.execute(consult, ('MERCADONA, S.A.', address, postal_code, city, phone))
    
        # The id is got for future tables
        output = cursor.fetchone()
        if output:
            store_id = output[0]
        else:
            # The user already exists, the id must be searched
            cursor.execute('SELECT id FROM Stores WHERE address = %s', (address,))
            store_id = cursor.fetchone()[0]
    
        ### USERS
    
        consult = '''
        INSERT INTO Users (name, surname1, surname2, nif, email)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (nif) DO NOTHING
        RETURNING id;
        '''
        cursor.execute(consult, ('Cristian', 'Guerrero', 'Balber', '76086500Q', 'cristian.guerrerobalber@gmail.com'))
    
        # The id is got for future tables
        output = cursor.fetchone()
        if output:
            user_id = output[0]
        else:
            # The user already exists, the id must be searched
            cursor.execute('SELECT id FROM Users WHERE nif = %s', ('76086500Q',))
            user_id = cursor.fetchone()[0]
    
        ### INVOICES
    
        consult = '''
        INSERT INTO Invoices (invoice_number, op_number, nc_number, aid, auth_code,
        arc_code, invoice_date, store_id, user_id, card_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (invoice_number) DO NOTHING
        RETURNING id;
        '''
    
        cursor.execute(consult, (invoice_number, op_number, nc_number, aid, auth_code,
                                 arc_code, invoice_date, store_id, user_id, card_type))
    
        # The id is got for future tables
        output = cursor.fetchone()
        if output:
            invoice_id = output[0]
            invoice_exists = False
        else:
            # The invoice already exists, the id won´t be needed
            invoice_exists = True
    
    
        ### INVOICE_ITEMS
        if not invoice_exists:
            for i, row in df_invoice.iterrows():
                product = row['product']
                quantity = row['qty']
                snapshot_unit_price = row['unit_price']
                cursor.execute('SELECT id FROM products WHERE name = %s', (product,))
                product_id = cursor.fetchone()[0]
                total_price = row['price']
                weight = row['weight']
    
                consult = '''
                INSERT INTO invoice_items (invoice_id, product_id, quantity, weight, snapshot_unit_price, total_price)
                VALUES (%s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(consult, (invoice_id, product_id, quantity, weight, snapshot_unit_price, total_price))
    
        ### COMMIT
        # Commit all changes
        connection.commit()
    
        return {"message": "Procesado correctamente"}
    
    except Exception as e:
        connection.rollbakc()  # Undo the changes if error
        return {"error: ", e}
    
    finally:
        cursor.close()
        connection.close()



