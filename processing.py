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
    def populate_dict(qty, product, price, unit_measure):
        dictRow = {
            'qty': qty,
            'product': product,
            'price': price,
            'unit_measure': unit_measure
        }
        return dictRow

    '''
    Analysing the Mercadona invoices, the logic to populate the purchase table is as follows:
    - Check if the last element is a float, it means, it has a price
        - if so, check if the first element is a float (is is a product weighted).
            - if not, is is an integer. Everything is ok. Populate.
            - if so, populate that weight as qty of the previous element and populate with the info of previous element.
    '''
    for i, row in enumerate(products_lst_split):
        # Selection of the interesting elements of the row
        first_elem = row[0]
        last_elem = row[-1]
        # Check if the last element is a float
        if isfloat(last_elem):  # The last element has price
            if not isfloat(first_elem):  # The first element is not a weight, so it is a integer qty
                qty = float(first_elem)  # Althoug it is an integer, it will be treated as float
                price = float(last_elem.replace(',', '.'))
                unit_measure = '€/unit'
                # Check if the P. Unit is populated or not to know how to join the name of the product
                if isfloat(row[-2]):
                    lastProdIdx = -3
                else:
                    lastProdIdx = -2
                product = ' '.join(row[1:lastProdIdx + 1])
            else:  # The first element is a float, so it represents a weight of the product of the former line
                qty = float(first_elem.replace(',', '.'))
                price = float(last_elem.replace(',', '.'))
                unit_measure = f'€/{row[1]}'
                # For this scenario, the name of the product is in the former line
                product = products_lst_split[i - 1]
                '''
                When the product bought is, for example, a fruit, the is a qty before the product, which it is not 
                interesting as it is always a 1 (the interesting qty is the weight). However, other products like fish,
                have no that item on the description. Let´s process that removen the qty when apply.
                '''
                if product[0] == '1':
                    product.remove('1')
                # Now, join the list
                product = ' '.join(product)
            # Anycase, append the data to the dict
            dictRow = populate_dict(qty, product, price, unit_measure)
            # Finally, append the dict to the data list
            dataForDf.append(dictRow)
        else:
            pass

    # Creation of the dataframe
    df_invoice = pd.DataFrame(dataForDf)
    # Adding unit_price calculated
    df_invoice['unit_price'] = round(df_invoice['price'] / df_invoice['qty'], 2)
    # Removing the parking product if needed
    df_invoice = df_invoice[df_invoice['product'] != 'PARKING']

    ## Getting general invoice variables
    def obtain_idx(variable):
        '''
        Obtain a idx of an element in the invoice_lst given a chain of characters that must be into that element.
        :param variable:
        :return:
        '''
        ### Creation of a dict with keywords to be search to locate the correct index of some variables that might change of position

        idx_keywords = {
            'phone': 'TELÉFONO:',
            'op_number': 'OP:',
            'invoice_number': 'FACTURA SIMPLIFICADA',
            'nc_number': 'N.C',
            'auth_code': 'AUT:',
            'aid': 'AID:',
            'arc_code': 'ARC:',
            'card_number_last_digits': 'TARJ.',
            'card_type': 'Importe:',
            'entry_time': 'ENTRADA ',
            'exit_time': 'SALIDA '
        }
        keyword = idx_keywords[variable]
        # Filter the whole list by the element with the keyword and obtain the element
        element = list(filter(lambda x: keyword in x, invoice_lst))[0]
        # Obtain the idx of that element
        idx = invoice_lst.index(element)
        return idx

    # Adress
    address = invoice_lst[1]
    # Postal Code
    postal_code = invoice_lst[2].split()[0]
    # city
    city = ' '.join(invoice_lst[2].split()[1:])
    # Phone number
    phone = invoice_lst[obtain_idx('phone')].split()[-1]
    # Date
    invoice_dateText = ' '.join(invoice_lst[4].split()[:2])
    invoice_date = datetime.strptime(invoice_dateText, "%d/%m/%Y %H:%M")
    # OP
    op_number = int(invoice_lst[obtain_idx('op_number')].split()[-1].split(':')[-1])
    # Invoice ID
    invoice_number = invoice_lst[obtain_idx('invoice_number')].split()[-1]
    # card_number
    card_number_last_digits = int(invoice_lst[obtain_idx('card_number_last_digits')].split()[-1])
    # N.C
    nc_number = invoice_lst[obtain_idx('nc_number')].split()[1]
    # AUT
    auth_code = invoice_lst[obtain_idx('auth_code')].split()[3]
    # AID
    aid = invoice_lst[obtain_idx('aid')].split()[1]
    # ARC
    arc_code = invoice_lst[obtain_idx('arc_code')].split()[3]
    # Card_type
    card_type = invoice_lst[obtain_idx('card_type')].split()[-1]
    # parking_used, entry_time and exit_time
    parking_used = True if '1 PARKING 0,00' in invoice_lst else False
    if parking_used:
        entry_time_text = invoice_lst[obtain_idx('entry_time')].split()[1]
        entry_time = datetime.strptime(entry_time_text, '%H:%M').time()
        exit_time_text = invoice_lst[obtain_idx('exit_time')].split()[3]
        exit_time = datetime.strptime(exit_time_text, '%H:%M').time()
    else:
        entry_time = None
        exit_time = None


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
        arc_code, card_number_last_digits, invoice_date, store_id, user_id, card_type,
        parking_used, entry_time, exit_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (invoice_number) DO NOTHING
        RETURNING id;
        '''

        cursor.execute(consult, (invoice_number, op_number, nc_number, aid, auth_code,
                                 arc_code, card_number_last_digits, invoice_date, store_id, user_id, card_type,
                                 parking_used, entry_time, exit_time))

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

                consult = '''
                INSERT INTO invoice_items (invoice_id, product_id, quantity, snapshot_unit_price, total_price)
                VALUES (%s, %s, %s, %s, %s)
                '''
                cursor.execute(consult, (invoice_id, product_id, quantity, snapshot_unit_price, total_price))
        else:
            raise ValueError("La factura enviada ya existe en el sistema")
        ### COMMIT
        # Commit all changes
        connection.commit()
        return {"message": "Procesado correctamente"}

    except Exception as e:
        connection.rollback()  # Undo the changes if error
        return {"error: ", e}

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    result = process_invoice(r'2.inputs/20250614 Mercadona 163,16 €.pdf')
    print(result)
