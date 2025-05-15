# firebase_auth.py
import os, json
import firebase_admin
from firebase_admin import auth, credentials
from fastapi import Request, Depends, HTTPException, status

# Initialize Firebase Admin SDK using a service‑account JSON in env var
cred = credentials.Certificate(
    json.loads(os.getenv("FIREBASE_CREDENTIALS_JSON"))
)
firebase_admin.initialize_app(cred)

async def verify_token(request: Request):
    """
    FastAPI dependency: verify incoming Firebase ID token in the Authorization header.
    Attaches decoded claims to request.state.user.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
        )
    id_token = auth_header.split("Bearer ")[1]
    try:
        decoded_token = auth.verify_id_token(id_token)
        request.state.user = decoded_token
        return decoded_token
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token verification failed: {e}",
        )
