from fastapi import APIRouter
from typing import List
from pydantic import BaseModel
import json

router = APIRouter()


# Modèle Pydantic pour la réponse des produits
class ProductResponse(BaseModel):
    id: str
    name: str
    category: str
    price: float
    cost: float


# Charger les données des produits depuis le fichier JSON
def load_products():
    """Charge les produits depuis le fichier JSON 'products.json'."""
    try:
        with open('../../data_api/products.json', 'r', encoding='utf-8') as f:
            products = json.load(f)
        return products
    except FileNotFoundError:
        print("Le fichier des produits n'existe pas.")
        return []


@router.get("", response_model=List[ProductResponse])
async def get_products():
    """
    Route GET pour récupérer la liste des produits depuis le fichier 'products.json'.
    """
    # Charger les données des produits
    try:
        products = load_products()
    except FileNotFoundError:
        return {"error": "Products data file not found."}

    # Formater et retourner la réponse
    return products
