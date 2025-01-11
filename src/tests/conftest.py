"""
Fixture Pytest pour nettoyer le répertoire `data_test` après l'exécution de tous les tests.

Cette fixture est définie avec `autouse=True`, ce qui signifie qu'elle sera automatiquement utilisée
sans avoir besoin de l'inclure explicitement dans les tests. Elle garantit que tous les fichiers ou
dossiers créés dans le répertoire `data_test` sont supprimés une fois la session de tests terminée.
"""
import os
import shutil

import pytest


# Cette fixture s'exécutera après tous les tests
@pytest.fixture(autouse=True, scope="session")
def cleanup_test_data(request):
    """
     Fixture Pytest pour nettoyer le répertoire `data_test` après l'exécution de tous les tests.

     Cette fixture garantit que tous les fichiers ou dossiers créés dans le répertoire `data_test`
     sont supprimés une fois la session de tests terminée.
     """
    def clean():
        test_data_dir = os.path.join("data_test")
        if os.path.exists(test_data_dir):
            shutil.rmtree(test_data_dir)

    # Enregistre la fonction de nettoyage pour qu'elle s'exécute à la fin
    request.addfinalizer(clean)
