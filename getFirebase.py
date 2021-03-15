import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd

cred = credentials.Certificate("kevFirebase.json")
firebase_admin.initialize_app(cred, {'projectId': "sil-interviews"})

def iterate(collection_name, batch_size=50000, cursor=None):
    firestoreClient = firestore.client()
    firebaseQuery = firestoreClient.collection(collection_name).limit(batch_size)
    if cursor:
        query = firebaseQuery.start_after(cursor)

    for doc in firebaseQuery.stream():
        yield doc

    else:
        if 'doc' in locals():
            yield from iterate(collection_name, batch_size, doc)

docsList = []
docsGenerator = iterate(collection_name="beneficiaries")

for doc in docsGenerator:
    print(len(docsList))
    docsList.append(doc.to_dict())

df = pd.DataFrame(docsList)
print(df.head())
