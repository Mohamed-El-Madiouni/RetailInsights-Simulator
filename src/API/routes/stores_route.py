from fastapi import APIRouter
from typing import List
from pydantic import BaseModel
import json

router = APIRouter()


# Modèle Pydantic pour la réponse des magasins
class StoreResponse(BaseModel):
    id: str
    name: str
    location: str
    capacity: int
    opening_hour: str
    closing_hour: str


# Charger les données des magasins depuis le fichier JSON
def load_stores():
    """Charge les magasins depuis le fichier JSON 'stores.json'."""
    try:
        with open('data/stores.json', 'r', encoding='utf-8') as f:
            stores = json.load(f)
        return stores
    except FileNotFoundError:
        print("Le fichier des magasins n'existe pas.")
        return []


@router.get("", response_model=List[StoreResponse])
async def get_stores():
    """
    Route GET pour récupérer la liste des magasins depuis le fichier 'stores.json'.
    """
    # Charger les données des magasins
    try:
        stores = load_stores()
    except FileNotFoundError:
        return {"error": "Stores data file not found."}

    # Formater et retourner la réponse
    return stores
