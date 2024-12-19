import pandas as pd
import os


# Lire tous les fichiers Parquet dans un répertoire
def read_parquet_files(folder_path):
    """
    Lit tous les fichiers Parquet dans un répertoire et ses sous-dossiers.
    Concatène les fichiers Parquet d'un même sous-dossier en un seul DataFrame.

    :param folder_path: Chemin du répertoire contenant les fichiers/dossiers Parquet.
    """
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)

        # Si le fichier est un fichier Parquet
        if file.endswith(".parquet"):
            print(f"Lecture du fichier Parquet : {file_path}")
            df = pd.read_parquet(file_path)
            print(len(df))

        # Si c'est un dossier (et non un fichier Parquet)
        elif os.path.isdir(file_path):
            print(f"Lecture du sous-dossier : {file_path}")

            # Concatène tous les fichiers Parquet du sous-dossier
            data_frames = []
            for sub_file in os.listdir(file_path):
                sub_file_path = os.path.join(file_path, sub_file)
                if sub_file.endswith(".parquet"):
                    print(f"Lecture du fichier Parquet : {sub_file_path}")
                    data_frames.append(pd.read_parquet(sub_file_path))

            # Concatène les DataFrames si la liste n'est pas vide
            if data_frames:
                concatenated_df = pd.concat(data_frames, ignore_index=True)
                print(f"DataFrame concaténé pour le dossier {file} :")
                print(len(concatenated_df))


# Exemple d'utilisation
if __name__ == "__main__":
    folder_path = "data"
    print("")
    read_parquet_files(folder_path)
