import json
from typing import List, Union

from fastapi import APIRouter
from pydantic import BaseModel
from src.api.routes.logger_routes import logger

router = APIRouter()


# Modèle Pydantic pour la réponse des magasins
class StoreDataResponse(BaseModel):
    id: str
    name: str
    location: str
    capacity: int
    opening_hour: str
    closing_hour: str


class ErrorResponse(BaseModel):
    error: str


StoreResponse = Union[StoreDataResponse, ErrorResponse]


# Charger les données des magasins depuis le fichier JSON
def load_stores():
    """
    Charge les magasins depuis le fichier JSON 'stores.json'.

    Returns:
        list: Liste des magasins chargés depuis le fichier.
        []: Si le fichier n'existe pas ou est vide.

    Raises:
        FileNotFoundError: Si le fichier 'stores.json' est introuvable.
    """
    try:
        with open("data_api/stores.json", "r", encoding="utf-8") as f:
            stores = json.load(f)
        logger.info("Stores data successfully loaded.")
        return stores
    except FileNotFoundError:
        logger.error("Stores data file not found.")
        return []


@router.get("", response_model=List[StoreResponse])
async def get_stores():
    """
    Route GET pour récupérer la liste des magasins depuis le fichier 'stores.json'.

    Returns:
        List[StoreResponse]: Liste des magasins si le fichier est chargé avec succès.
    """
    logger.info("GET /stores called.")

    # Charger les données des magasins et gérer les cas où le fichier est introuvable
    try:
        stores = load_stores()
    except FileNotFoundError:
        logger.error("Error loading stores data.")
        return [{"error": "Stores data file not found."}]

    if not stores:
        logger.warning("No stores found in stores.json.")
        return [{"error": "No stores available."}]

    logger.info(f"Retrieved {len(stores)} stores.")

    # Formater et retourner la réponse
    return stores
