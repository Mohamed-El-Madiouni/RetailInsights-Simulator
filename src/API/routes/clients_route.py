from fastapi import APIRouter
from typing import List, Union
from pydantic import BaseModel
import json
from fastapi.responses import JSONResponse


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
    """Charge les clients depuis le fichier JSON 'clients.json'."""
    try:
        with open('../../data_api/clients.json', 'r', encoding='utf-8') as f:
            clients = json.load(f)
        return clients
    except FileNotFoundError:
        print("Le fichier des clients n'existe pas.")
        return []


@router.get("", response_model=List[ClientResponse])
async def get_clients(city: str):
    """
    Route GET pour récupérer la liste des clients dans une ville donnée.
    L'argument 'city' est requis pour filtrer les clients par ville.
    """
    # Charger les données des clients
    try:
        clients = load_clients()
    except FileNotFoundError:
        return JSONResponse(
            content={"error": "Clients data file not found."},
            status_code=404,
        )

    # Filtrer les clients par la ville
    filtered_clients = [
        client for client in clients if client['city'].lower() == city.lower()
    ]

    # Si aucun client n'est trouvé pour la ville spécifiée
    if not filtered_clients:
        return JSONResponse(
            content={"error": f"No clients found in city: {city}"},
            status_code=404,  # Changer ici le code de statut si nécessaire
        )

    # Retourner la liste des clients filtrés
    return filtered_clients
