from connexion import connect


def load(data):
    client = connect.mongo_connect()
    # Select the database and collection
    print(client.list_database_names())

    db = client['etl']
    # TODO: this will be a parameter args.collection
    collection = db['NEWS']
    # Insert the data into the collection
    # TODO: check that the document is not empty
    if data:
        collection.insert_many(data)
        print('Data loaded successfully')
    else:
        print('Empty data')
    client.close()
