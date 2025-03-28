import json
from typing import List

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from src.api.routes.logger_routes import logger

router = APIRouter()


# Modèle Pydantic pour la réponse des clients
class ClientResponse(BaseModel):
    id: str
    name: str
    age: int
    gender: str
    loyalty_card: bool
    city: str


# Charger les données des clients depuis le fichier JSON
def load_clients():
    """
    Charge les clients depuis le fichier JSON 'clients.json'.

    Returns:
        list: Liste des clients chargés depuis le fichier.
        []: Si le fichier n'existe pas ou est vide.

    Raises:
        FileNotFoundError: Si le fichier 'clients.json' est introuvable.
    """
    try:
        with open("data_api/clients.json", "r", encoding="utf-8") as f:
            clients = json.load(f)
        return clients
    except FileNotFoundError:
        logger.error("Clients data file not found.")
        return []


@router.get("", response_model=List[ClientResponse])
async def get_clients(city: str):
    """
    Route GET pour récupérer la liste des clients dans une ville donnée.

    Args:
        city (str): Nom de la ville pour filtrer les clients.

    Returns:
        JSONResponse: Liste des clients correspondant à la ville, ou un message d'erreur si aucun client n'est trouvé.
    """
    logger.info(f"GET /clients called with city={city}")
    # Charger les données des clients
    try:
        clients = load_clients()
    except FileNotFoundError:
        logger.error("Error loading clients data.")
        return JSONResponse(
            content={"error": "Clients data file not found."},
            status_code=404,
        )

    # Filtrer les clients pour inclure uniquement ceux de la ville spécifiée (insensible à la casse)
    filtered_clients = [
        client for client in clients if client["city"].lower() == city.lower()
    ]

    # Si aucun client n'est trouvé pour la ville spécifiée
    if not filtered_clients:
        logger.warning(f"No clients found in city: {city}")
        return JSONResponse(
            content={"error": f"No clients found in city: {city}"},
            status_code=404,
        )

    logger.info(f"Clients retrieved successfully for city={city}")
    # Retourner la liste des clients filtrés
    return filtered_clients
