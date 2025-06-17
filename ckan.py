import requests as req
from tqdm import tqdm
from datetime import datetime

CKAN_URL = "http://svjc-des-ckan.ttec.es:84/catalogo"
API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJvRDFyS0JZSThkN1Y0YVotYjh0VTFNdkI5bzNOZ3R5RDdBTXAxZEVqdENRIiwiaWF0IjoxNzM5OTU4NzkxfQ.ErylKZFVJgGr6VsZ-eywA4wz1zKwzbhErraKw4Rae4Q"
API_BASE_URL = "http://svjc-des-bne.ttec.es:3000/api"

def actualizar_CKAN(datasets):
    """
    Actualiza los conjuntos de datos en el catálogo CKAN.
    
    Args:
        datasets (dict): Diccionario con los nombres de los conjuntos de datos a actualizar
    """
    try:
        session = req.Session()
        session.headers.update({
            'Authorization': API_KEY,
            'Content-Type': 'application/json'
        })
        
        for dataset_name, mrc_file in datasets.items():
            dataset_id = dataset_name[:3]  
            
            print(f"Procesando dataset: {dataset_id}")
            
            # Verificar si el dataset ya existe en CKAN
            check_url = f"{CKAN_URL}/api/3/action/package_show"
            try:
                response = session.get(f"{check_url}?id={dataset_id}", verify=False)
                dataset_exists = response.status_code == 200
                if dataset_exists:
                    existing_dataset = response.json().get('result', {})
            except Exception as e:
                print(f"Error al verificar existencia del dataset {dataset_id}: {e}")
                dataset_exists = False
                existing_dataset = {}
            
            # Definir los recursos como URLs para diferentes formatos
            current_date = datetime.now().strftime("%Y-%m-%d")
            resources = []
            
            # Mantener recursos existentes que no son de estos formatos específicos
            if dataset_exists:
                for resource in existing_dataset.get('resources', []):
                    # Si el recurso no es uno de los que vamos a crear, lo mantenemos
                    if not any(fmt in resource.get('name', '').lower() for fmt in ['json', 'csv', 'xml']):
                        resources.append(resource)
            
            # Añadir recursos para diferentes formatos
            formats = [
                {"name": "JSON", "extension": "json", "mimetype": "application/json"},
                {"name": "CSV", "extension": "csv", "mimetype": "text/csv"},
            ]
            
            for fmt in formats:
                url = f"{API_BASE_URL}/{dataset_id}.{fmt['extension']}"
                
                # Comprobar si el recurso ya existe (por URL)
                existing_resource = None
                if dataset_exists:
                    for resource in existing_dataset.get('resources', []):
                        if resource.get('url') == url:
                            existing_resource = resource
                            break
                
                # Si existe, mantener el ID para actualizarlo en lugar de crear uno nuevo
                resource_data = {
                    'name': f"Datos en formato {fmt['name']}",
                    'description': f"Descarga del dataset en formato {fmt['name']}",
                    'url': url,
                    'format': fmt['name'],
                    'mimetype': fmt['mimetype'],
                    'last_modified': current_date,
                    'resource_type': 'api'
                }
                
                if existing_resource:
                    resource_data['id'] = existing_resource.get('id')
                
                resources.append(resource_data)
            
            # Preparar los datos del JSON para la creación/actualización
            dataset_data = {
                'name': dataset_id,
                'title': f"Conjunto de datos: {mrc_file}",
                "title_translated": {
                    "en": f"MARC21 dataset: {dataset_id}",
                    "es": f"Conjunto de datos MARC21: {dataset_id}"
                },
                'notes': f"Datos actualizados desde registros MARC21 ({mrc_file})", 
                "notes_translated": {
                    "en": f"Data updated from MARC21 files ({mrc_file})",
                    "es": f"Datos actualizados desde registros MARC21 ({mrc_file})"
                },
                'owner_org': 'test-api',
                "author": "",
                "contact_email": "fsoria@tragsa.es",
                "dcat_type": "http://inspire.ec.europa.eu/metadata-codelist/ResourceType/dataset",
                "identifier": mrc_file,
                "language": "http://publications.europa.eu/resource/authority/language/SPA",
                "maintainer": "",
                "hvd_category": "",
                "resources": resources,
                 "spatial_coverage": [
                  {
                    "bbox": {
                      "type": "Polygon",
                      "coordinates": [
                        [
                          [
                            -18.16,
                            27.64
                          ],
                          [
                            4.32,
                            27.64
                          ],
                          [
                            4.32,
                            43.79
                          ],
                          [
                            -18.16,
                            43.79
                          ],
                          [
                            -18.16,
                            27.64
                          ]
                        ]
                      ]
                    },
                    "centroid": {
                      "type": "Point",
                      "coordinates": [
                        -6.92,
                        35.715
                      ]
                    },
                    "text": "España",
                    "uri": "http://datos.gob.es/recurso/sector-publico/territorio/Pais/España"
                  }
                ],
         
            }
            
            if dataset_exists and 'id' in existing_dataset:
                dataset_data['id'] = existing_dataset['id']
            
            # Crear o actualizar el dataset
            if dataset_exists:
                update_url = f"{CKAN_URL}/api/3/action/package_update"
                action = "actualizando"
            else:
                update_url = f"{CKAN_URL}/api/3/action/package_create"
                action = "creando"
                
            try:
                with tqdm(total=1, desc=f"{action.capitalize()} dataset en CKAN", unit="dataset") as pbar:
                    response = session.post(update_url, json=dataset_data, verify=False)
                    response.raise_for_status()
                    resultado = response.json()
                    pbar.update(1)
                
                print(f"Dataset {dataset_id} {action} exitosamente en CKAN")
            except Exception as e:
                print(f"Error {action} dataset {dataset_id} en CKAN: {e}")
                continue
                
    except Exception as e:
        print(f"Error general al actualizar CKAN: {e}")
        
    return
