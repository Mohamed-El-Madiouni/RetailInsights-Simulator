import json
from typing import List

from fastapi import APIRouter
from pydantic import BaseModel
from src.api.routes.logger_routes import logger

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
        logger.info("Products data successfully loaded.")
        return products
    except FileNotFoundError:
        logger.error("Products data file not found.")
        return []


@router.get("", response_model=List[ProductResponse])
async def get_products():
    """
    Route GET pour récupérer la liste des produits depuis le fichier 'products.json'.

    Returns:
        List[ProductResponse]: Liste des produits si le fichier est chargé avec succès.
        dict: Message d'erreur si le fichier 'products.json' est introuvable.
    """
    logger.info("GET /products called.")

    # Charger les données des produits
    try:
        products = load_products()
    except FileNotFoundError:
        logger.error("Error loading products data.")
        return [{"error": "Products data file not found."}]

    if not products:  # si le fichier est vide
        logger.warning("No products found in products.json.")
        return [{"error": "No products available."}]

    logger.info(f"{len(products)} products retrieved successfully.")

    # Retourner la liste des produits tels que définis dans le fichier JSON
    return products
