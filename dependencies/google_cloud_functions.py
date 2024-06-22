from google.cloud import secretmanager
from google.cloud import storage
import logging

def get_last_secret(project_id,secret_name) -> str:
    """Obtener la ultima version de un sercreto de 
    Google Secret Manager
    Args:
        secret_name: str 
            Nombre del secreto

    Returns:
        secret: str
            Valor del secreto
    """

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    return response.payload.data.decode('UTF-8')

def upload_blob(project_id, bucket_name, blob_filename, blob_file):
    """Subir un archivo a Cloud Storage
    Args:
        bucket_name: str 
            Nombre del bucket
        blob_filename: str 
            Nombre del archivo
        blob_file: Bytes
            Contenido del archivo 

    Returns:
    
    """
    storage_client = storage.Client(project = project_id)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_filename)
    blob.upload_from_string(blob_file.encode('utf-8'))
    logging.info(f"{blob_filename} was uploaded to {bucket_name}")



def delete_path_blob(project_id, bucket_name, blob_path):
    """Eliminar todos los archivos contenidos un directorio
    de un Bucket en Cloud Storage
    Args:
        bucket_name: str 
            Nombre del bucket
        blob_path: str 
            Directorio contenido en el Bucket

    Returns:
    
    """

    storage_client = storage.Client(project = project_id)

    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix = blob_path)
    for blob in blobs:
        logging.warning(f"{blob.name} is being deleted to {bucket_name}")

        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request to delete is aborted if the object's
        # generation number does not match your precondition.
        blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
        generation_match_precondition = blob.generation

        blob.delete(if_generation_match=generation_match_precondition)