#!/usr/bin/env python3
"""
Script específico para Los Angeles - Captura datos por hora de hoy
Basado en captura_realtime_open_aq_x_meteo.py que SÍ funcionó para LA
Genera CSV con formato correcto para el modelo de predicción
"""

import pandas as pd
from datetime import datetime, timedelta, timezone
import os
import time
import urllib.request
import urllib.parse
import json
from dotenv import load_dotenv
import argparse

# Cargar variables de entorno
load_dotenv()
API_KEY_OPENAQ = os.getenv("API_KEY_OPENAQ")

if not API_KEY_OPENAQ:
    raise ValueError("No se encontró la API Key. Asegúrate de crear un archivo .env con API_KEY_OPENAQ.")

# Configuración específica para LA (basada en el script exitoso)
LA_CONFIG = {
    "lat": 34.05, 
    "lon": -118.24, 
    "tz_api": "America/Los_Angeles",
    "name": "Centro_LA"
}

# Formato CSV correcto como especificaste
CSV_COLUMNS = [
    'timestamp', 'temperature_2m', 'relativehumidity_2m', 'precipitation', 'pressure_msl',
    'windspeed_10m', 'winddirection_10m', 'boundary_layer_height', 'shortwave_radiation_sum',
    'hour_of_day', 'day_of_week', 'month_of_year', 'is_weekend',
    'co', 'no', 'no2', 'nox', 'o3', 'pm25', 'so2',
    'pm25_lag_3h', 'pm25_lag_6h', 'pm25_lag_12h', 'pm25_lag_24h'
]

# Mapeo de parámetros de OpenAQ (del script exitoso)
PARAMETER_MAPPING = {
    'co ppm': 'co',
    'no ppm': 'no',
    'no2 ppm': 'no2', 
    'nox ppm': 'nox',
    'o3 ppm': 'o3',
    'pm25 µg/m³': 'pm25',
    'so2 ppm': 'so2'
}

def obtener_sensores_ubicacion_la(location_id, api_key):
    """Obtiene la lista de sensores para una ubicación específica en LA."""
    headers = {"X-API-Key": api_key}
    sensors_url = f"https://api.openaq.org/v3/locations/{location_id}/sensors"
    
    try:
        req = urllib.request.Request(sensors_url, headers=headers)
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                if data.get('results'):
                    return data['results']
    except Exception as e:
        print(f"...⚠️ Error obteniendo sensores LA: {e}")
    
    return []

def obtener_mediciones_sensor_historicas_la(sensor_id, datetime_from, datetime_to, api_key):
    """Obtiene mediciones históricas de un sensor específico para LA."""
    headers = {"X-API-Key": api_key}
    
    start_time = datetime_from.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime_to.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    measurements_url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
    params = {
        'datetime_from': start_time,
        'datetime_to': end_time,
        'limit': 100
    }
    
    try:
        full_url = f"{measurements_url}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(full_url, headers=headers)
        
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                return data.get('results', [])
            else:
                print(f"...❌ Error HTTP {response.status} para sensor {sensor_id}")
    except Exception as e:
        print(f"...⚠️ Error obteniendo mediciones del sensor {sensor_id}: {e}")
    
    return []

def obtener_pm25_historico_la(location_id, target_timestamp, api_key):
    """Obtiene valores de PM2.5 históricos para LA usando el método exitoso."""
    print(f"...🔍 Obteniendo datos históricos PM2.5 para LA")
    
    lag_hours = [3, 6, 12, 24]
    lag_values = {'pm25_lag_3h': None, 'pm25_lag_6h': None, 'pm25_lag_12h': None, 'pm25_lag_24h': None}
    lag_columns = ['pm25_lag_3h', 'pm25_lag_6h', 'pm25_lag_12h', 'pm25_lag_24h']
    
    # Obtener sensores de la ubicación
    sensores = obtener_sensores_ubicacion_la(location_id, api_key)
    if not sensores:
        print(f"...❌ No se pudieron obtener sensores para LA")
        return lag_values
    
    # Encontrar sensor de PM2.5
    sensor_pm25 = None
    for sensor in sensores:
        if 'pm25' in sensor.get('name', '').lower():
            sensor_pm25 = sensor
            print(f"...✅ Sensor PM2.5 encontrado LA: {sensor['name']} (ID: {sensor['id']})")
            break
    
    if not sensor_pm25:
        print(f"...❌ No se encontró sensor PM2.5 en LA")
        return lag_values
    
    # Obtener mediciones históricas para cada lag
    for i, hours_back in enumerate(lag_hours):
        lag_column = lag_columns[i]
        
        target_time = target_timestamp - timedelta(hours=hours_back)
        datetime_from = target_time - timedelta(minutes=30)
        datetime_to = target_time + timedelta(minutes=30)
        
        print(f"...📅 Buscando PM2.5 LA hace {hours_back}h: {target_time.strftime('%Y-%m-%d %H:%M')}")
        
        mediciones = obtener_mediciones_sensor_historicas_la(
            sensor_pm25['id'], datetime_from, datetime_to, api_key
        )
        
        if mediciones:
            mejor_medicion = None
            menor_diferencia = timedelta(hours=1)
            
            for medicion in mediciones:
                try:
                    medicion_time_str = medicion['period']['datetimeTo']['local']
                    medicion_time = datetime.fromisoformat(medicion_time_str.replace('Z', '+00:00'))
                    
                    diferencia = abs(medicion_time - target_time)
                    if diferencia < menor_diferencia:
                        menor_diferencia = diferencia
                        mejor_medicion = medicion
                except Exception as e:
                    continue
            
            if mejor_medicion and menor_diferencia <= timedelta(minutes=30):
                pm25_value = mejor_medicion.get('value')
                lag_values[lag_column] = pm25_value
                print(f"...✅ {lag_column}: {pm25_value} (diff: {menor_diferencia})")
            else:
                print(f"...❌ {lag_column}: No se encontró medición cercana")
        else:
            print(f"...❌ {lag_column}: Sin mediciones en el rango de tiempo")
    
    return lag_values

def get_openaq_data_la(lat, lon, api_key):
    """Obtiene datos de calidad del aire para LA (método exitoso)."""
    print("...Iniciando petición OpenAQ para LA...")
    air_quality_data = {}
    location_id = None
    
    try:
        base_url = "https://api.openaq.org/v3/locations"
        params = {
            "coordinates": f"{lat},{lon}",
            "radius": 10000,
            "limit": 5
        }
        headers = {"X-API-Key": api_key}
        
        locations_url = f"{base_url}?{urllib.parse.urlencode(params)}"
        print(f"...Buscando ubicaciones LA: {locations_url}")
        
        req = urllib.request.Request(locations_url, headers=headers)
        
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                locations_data = json.loads(response.read().decode("utf-8"))
                
                if locations_data.get('results'):
                    print(f"...Encontradas {len(locations_data['results'])} ubicaciones LA")
                    
                    # Buscar la ubicación con más parámetros disponibles
                    best_location = None
                    max_params = 0
                    
                    for location in locations_data['results']:
                        sensors = location.get('sensors', [])
                        sensor_params = [s['name'] for s in sensors]
                        target_params_count = sum(1 for param in sensor_params if param in PARAMETER_MAPPING)
                        
                        print(f"...Ubicación LA: {location['name']} - Parámetros: {target_params_count}")
                        
                        if target_params_count > max_params:
                            max_params = target_params_count
                            best_location = location
                    
                    if best_location:
                        location_id = best_location['id']
                        print(f"...Usando ubicación LA: {best_location['name']} (ID: {location_id})")
                        
                        # Crear mapeo de sensores
                        sensor_mapping = {}
                        for sensor in best_location.get('sensors', []):
                            sensor_id = sensor['id']
                            sensor_name = sensor['name']
                            sensor_mapping[sensor_id] = sensor_name
                        
                        # Obtener mediciones más recientes
                        measurements_url = f"https://api.openaq.org/v3/locations/{location_id}/latest"
                        print(f"...Obteniendo mediciones LA: {measurements_url}")
                        
                        req = urllib.request.Request(measurements_url, headers=headers)
                        
                        with urllib.request.urlopen(req) as response:
                            if response.status == 200:
                                measurements_data = json.loads(response.read().decode("utf-8"))
                                
                                if measurements_data.get('results'):
                                    print(f"...Procesando {len(measurements_data['results'])} mediciones LA")
                                    
                                    for measurement in measurements_data['results']:
                                        sensor_id = measurement.get('sensorsId')
                                        value = measurement.get('value')
                                        
                                        if sensor_id in sensor_mapping:
                                            param_name = sensor_mapping[sensor_id]
                                            
                                            if param_name in PARAMETER_MAPPING:
                                                mapped_param = PARAMETER_MAPPING[param_name]
                                                air_quality_data[mapped_param] = value
                                                print(f"...✓ LA {mapped_param}: {value}")
                                
                                print(f"...Datos calidad aire LA: {len(air_quality_data)} parámetros")

    except Exception as e:
        print(f"--- ERROR en OpenAQ LA: {e} ---")

    return air_quality_data, location_id

def get_openmeteo_data_la(lat, lon, timezone, start_date, end_date):
    """Obtiene datos climáticos de Open-Meteo para LA."""
    base_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon, "timezone": timezone,
        "start_date": start_date, "end_date": end_date,
        "hourly": ["temperature_2m", "relativehumidity_2m", "precipitation", "pressure_msl", 
                   "windspeed_10m", "winddirection_10m", "boundary_layer_height"],
        "daily": ["shortwave_radiation_sum"]
    }
    hourly_params = "&".join([f"hourly={var}" for var in params['hourly']])
    daily_params = "&".join([f"daily={var}" for var in params['daily']])
    del params['hourly'], params['daily']
    
    full_url = f"{base_url}?{urllib.parse.urlencode(params)}&{hourly_params}&{daily_params}"
    
    with urllib.request.urlopen(full_url) as response:
        if response.status != 200:
            raise Exception(f"API Open-Meteo falló con código {response.status}")
        return json.loads(response.read().decode("utf-8"))

def capturar_hora_actual_la():
    """Captura datos de LA para la hora actual solamente."""
    print("🌍 CAPTURANDO DATOS HORA ACTUAL - LOS ANGELES")
    print("="*60)
    
    # Obtener hora actual
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    current_hour = now_utc.hour
    
    print(f"⏰ Hora UTC actual: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Capturando datos para hora: {current_hour:02d}:00")
    
    all_rows = []
    
    # Procesar solo la hora actual
    hour = current_hour
    print(f"\n📅 PROCESANDO HORA ACTUAL: {hour:02d}:00")
    
    try:
            # Crear timestamp para esta hora
            target_timestamp = datetime.combine(today, datetime.min.time().replace(hour=hour))
            target_timestamp = target_timestamp.replace(tzinfo=timezone.utc)
            
            # Ajustar a zona horaria de LA
            target_timestamp_la = target_timestamp - timedelta(hours=8)  # UTC-8 para LA
            
            print(f"Hora objetivo LA: {target_timestamp_la.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Obtener datos meteorológicos
            weather_data = get_openmeteo_data_la(
                LA_CONFIG["lat"], LA_CONFIG["lon"], LA_CONFIG["tz_api"], 
                today.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
            )
            
            # Obtener datos de calidad del aire
            air_quality_data, location_id = get_openaq_data_la(
                LA_CONFIG["lat"], LA_CONFIG["lon"], API_KEY_OPENAQ
            )
            
            # Procesar datos meteorológicos
            df_hourly = pd.DataFrame(weather_data['hourly'])
            df_hourly['time'] = pd.to_datetime(df_hourly['time'])
            
            # Buscar datos para esta hora específica
            target_timestamp_naive = target_timestamp_la.replace(tzinfo=None)
            matching_rows = df_hourly[df_hourly['time'].dt.hour == hour]
            
            if not matching_rows.empty:
                current_weather_row = matching_rows.iloc[0]
                radiation_sum = weather_data['daily']['shortwave_radiation_sum'][0]
                
                # Crear nueva fila con formato correcto
                new_row = pd.Series(index=CSV_COLUMNS, dtype='object')
                new_row['timestamp'] = target_timestamp_la
                
                # Añadir datos meteorológicos
                for col in current_weather_row.index:
                    if col in new_row.index:
                        new_row[col] = current_weather_row[col]
                new_row['shortwave_radiation_sum'] = radiation_sum
                
                # Añadir datos de calidad del aire
                for param, value in air_quality_data.items():
                    if param in new_row.index:
                        new_row[param] = value
                
                # Obtener datos históricos de PM2.5
                if location_id:
                    lag_data = obtener_pm25_historico_la(location_id, target_timestamp_la, API_KEY_OPENAQ)
                    for lag_col, lag_value in lag_data.items():
                        new_row[lag_col] = lag_value
                else:
                    for lag_col in ['pm25_lag_3h', 'pm25_lag_6h', 'pm25_lag_12h', 'pm25_lag_24h']:
                        new_row[lag_col] = None
                
                # Añadir características temporales
                new_row['hour_of_day'] = hour
                new_row['day_of_week'] = target_timestamp_la.weekday()
                new_row['month_of_year'] = target_timestamp_la.month
                new_row['is_weekend'] = target_timestamp_la.weekday() >= 5
                
                all_rows.append(new_row)
                print(f"✅ Hora {hour:02d}:00 procesada exitosamente")
            else:
                print(f"❌ No se encontraron datos meteorológicos para hora {hour:02d}:00")
                
    except Exception as e:
        print(f"❌ Error procesando hora {hour:02d}:00: {e}")
    
    # Guardar CSV (agregar a archivo existente o crear nuevo)
    if all_rows:
        df_new = pd.DataFrame(all_rows)
        filename = f"datos_realtime_Centro_LA.csv"
        
        # Verificar si el archivo ya existe
        if os.path.exists(filename):
            # Leer archivo existente y agregar nueva fila
            try:
                df_existing = pd.read_csv(filename)
                df_final = pd.concat([df_existing, df_new], ignore_index=True)
                print(f"📝 Agregando datos a archivo existente")
            except Exception as e:
                print(f"⚠️ Error leyendo archivo existente: {e}")
                df_final = df_new
        else:
            df_final = df_new
            print(f"📝 Creando nuevo archivo")
        
        df_final.to_csv(filename, index=False)
        
        print(f"\n🎉 DATOS CAPTURADOS PARA LA")
        print(f"📊 Hora procesada: {current_hour:02d}:00")
        print(f"💾 Archivo actualizado: {filename}")
        
        return filename
    else:
        print(f"\n❌ No se pudieron procesar datos para LA hora {current_hour:02d}:00")
        return None

def ejecutar_captura_continua_la():
    """Ejecuta la captura de datos cada hora de manera continua."""
    print("🔄 INICIANDO CAPTURA CONTINUA PARA LOS ANGELES")
    print("⏰ Capturará datos automáticamente cada hora")
    print("🛑 Presiona Ctrl+C para detener")
    print("="*60)
    
    ultima_hora_procesada = -1
    
    try:
        while True:
            now_utc = datetime.now(timezone.utc)
            hora_actual = now_utc.hour
            
            # Solo procesar si es una nueva hora
            if hora_actual != ultima_hora_procesada:
                print(f"\n🕐 Nueva hora detectada: {hora_actual:02d}:00 UTC")
                capturar_hora_actual_la()
                ultima_hora_procesada = hora_actual
                
                # Esperar hasta el siguiente minuto para evitar procesamiento múltiple
                print(f"⏳ Esperando próxima hora...")
            
            # Verificar cada 30 segundos
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n🛑 Captura detenida por el usuario")
        print("💾 Datos guardados hasta el momento")

def main():
    parser = argparse.ArgumentParser(description='Captura datos de Los Angeles por hora')
    parser.add_argument('--continuo', action='store_true', help='Modo continuo (captura cada hora automáticamente)')
    parser.add_argument('--una-vez', action='store_true', help='Capturar solo la hora actual y salir')
    
    args = parser.parse_args()
    
    print("🚀 SCRIPT ESPECÍFICO PARA LOS ANGELES")
    print("="*50)
    print(f"📍 API Key: {'✅' if API_KEY_OPENAQ else '❌'}")
    print(f"🔄 Modo continuo: {'✅' if args.continuo else '❌'}")
    print(f"1️⃣ Solo una vez: {'✅' if args.una_vez else '❌'}")
    
    if not API_KEY_OPENAQ:
        print("❌ ERROR: API_KEY_OPENAQ no configurada")
        return
    
    if args.continuo:
        ejecutar_captura_continua_la()
    elif args.una_vez:
        capturar_hora_actual_la()
    else:
        print("\n📋 OPCIONES DISPONIBLES:")
        print("   --continuo    : Captura automática cada hora")
        print("   --una-vez     : Captura solo la hora actual")
        print("\n💡 Ejemplo: python generar_datos_LA_24h.py --continuo")

if __name__ == "__main__":
    main()