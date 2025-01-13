import json
from typing import List

from fastapi import APIRouter
from pydantic import BaseModel

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
    """
    Charge les produits depuis le fichier JSON 'products.json'.

    Returns:
        list: Liste des produits chargés depuis le fichier.
        []: Si le fichier est introuvable ou vide.

    Raises:
        FileNotFoundError: Si le fichier 'products.json' est introuvable.
    """
    try:
        with open("data_api/products.json", "r", encoding="utf-8") as f:
            products = json.load(f)
        return products
    except FileNotFoundError:
        print("Le fichier des produits n'existe pas.")
        return []


@router.get("", response_model=List[ProductResponse])
async def get_products():
    """
    Route GET pour récupérer la liste des produits depuis le fichier 'products.json'.

    Returns:
        List[ProductResponse]: Liste des produits si le fichier est chargé avec succès.
        dict: Message d'erreur si le fichier 'products.json' est introuvable.
    """
    # Charger les données des produits
    try:
        products = load_products()
    except FileNotFoundError:
        return {"error": "Products data file not found."}

    # Retourner la liste des produits tels que définis dans le fichier JSON
    return products
