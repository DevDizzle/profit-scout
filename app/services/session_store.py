# session_store.py
from google.cloud import firestore

db = firestore.Client()

def load_session(user_id: str):
    doc = db.collection("sessions").document(user_id).get()
    return doc.to_dict() or {"messages": []}

def save_message(user_id: str, message: dict):
    session_ref = db.collection("sessions").document(user_id)
    session_ref.update({
        "messages": firestore.ArrayUnion([message])
    })
