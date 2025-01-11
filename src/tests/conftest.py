import os
import shutil

import pytest


# Cette fixture s'exécutera après tous les tests
@pytest.fixture(autouse=True, scope="session")
def cleanup_test_data(request):
    def clean():
        test_data_dir = os.path.join("data_test")
        if os.path.exists(test_data_dir):
            shutil.rmtree(test_data_dir)

    # Enregistre la fonction de nettoyage pour qu'elle s'exécute à la fin
    request.addfinalizer(clean)
