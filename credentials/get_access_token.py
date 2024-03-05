import google.auth
from google.auth.transport.requests import Request

def get_access_token(credentials_path):
  """
  Obtiene un token de acceso del archivo de credenciales especificado.

  Args:
      credentials_path: Ruta al archivo de credenciales JSON.

  Returns:
      Un string que contiene el token de acceso, o None si hay un error.
  """

  try:
    credentials, _ = google.auth.load_credentials_from_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())
    # Access token using a different approach
    access_token = credentials.token
    return access_token
  except Exception as e:
    print(f"Error: {e}")
    return None

if __name__ == "__main__":
  credentials_path = "eminent-cycle-415715-3ef9bde04901.json"
  access_token = get_access_token(credentials_path)

  if access_token:
    print(f"Token de acceso: {access_token}")
    print("Ejecución exitosa del script.")
  else:
    print("Error durante la ejecución del script.")
