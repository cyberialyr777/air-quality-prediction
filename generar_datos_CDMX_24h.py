#!/usr/bin/env python3
"""
Script específico para Ciudad de México - Captura datos por hora de hoy
Basado en datos_historicos_mexico_hourly.py que SÍ funcionó para CDMX
Genera CSV con formato correcto para el modelo de predicción
"""

import os
import requests
import json
import pandas as pd
from datetime import datetime, timedelta, date, timezone
from dotenv import load_dotenv
import argparse
import time

# Cargar variables de entorno
load_dotenv()
API_KEY = os.getenv("API_KEY_OPENAQ")

if not API_KEY:
    raise ValueError("No se encontró la API Key. Asegúrate de crear un archivo .env con API_KEY_OPENAQ.")

# Configuración específica para CDMX (basada en el método exitoso)
CDMX_CONFIG = {
    "location_id": 10534,  # CCA_CDMX que SÍ funcionó
    "name": "CCA_CDMX",
    "lat": 19.4326,
    "lon": -99.1332,
    "timezone": "America/Mexico_City"
}

# Formato CSV correcto como especificaste
CSV_COLUMNS = [
    'timestamp', 'temperature_2m', 'relativehumidity_2m', 'precipitation', 'pressure_msl',
    'windspeed_10m', 'winddirection_10m', 'boundary_layer_height', 'shortwave_radiation_sum',
    'hour_of_day', 'day_of_week', 'month_of_year', 'is_weekend',
    'co', 'no', 'no2', 'nox', 'o3', 'pm25', 'so2',
    'pm25_lag_3h', 'pm25_lag_6h', 'pm25_lag_12h', 'pm25_lag_24h'
]

base_url = "https://api.openaq.org/v3/"
headers = {"accept": "application/json", "X-API-Key": API_KEY}

def obtener_sensores_cdmx(location_id):
    """Obtiene sensores para CDMX usando el método exitoso."""
    sensors_url = f"{base_url}locations/{location_id}/sensors"
    print(f"📡 Obteniendo sensores CDMX desde: {sensors_url}")
    
    try:
        response = requests.get(sensors_url, headers=headers)
        response.raise_for_status()
        
        sensors_list = response.json()['results']
        if not sensors_list:
            print(f"❌ No se encontraron sensores para CDMX")
            return []
        
        print(f"✅ Se encontraron {len(sensors_list)} sensores CDMX")
        
        # Mostrar detalles de sensores y crear mapeo
        sensor_mapping = {}
        for sensor in sensors_list:
            sensor_id = sensor['id']
            sensor_name = sensor['name']
            sensor_mapping[sensor_name] = sensor_id
            print(f"   - {sensor_name} (ID: {sensor_id})")
        
        return sensor_mapping
        
    except Exception as e:
        print(f"❌ Error obteniendo sensores CDMX: {e}")
        return {}

def obtener_mediciones_cdmx_rango(sensor_id, datetime_from, datetime_to):
    """Obtiene mediciones para un sensor específico en CDMX."""
    measurements_url = f"{base_url}sensors/{sensor_id}/measurements"
    
    params = {
        'datetime_from': datetime_from,
        'datetime_to': datetime_to,
        'limit': 100
    }
    
    try:
        response = requests.get(measurements_url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            measurements = data.get('results', [])
            return measurements
        else:
            print(f"      ❌ Error HTTP {response.status_code} para sensor {sensor_id}")
            return []
            
    except Exception as e:
        print(f"      ❌ Error obteniendo mediciones sensor {sensor_id}: {e}")
        return []

def obtener_datos_openaq_cdmx_hora(sensor_mapping, target_hour):
    """Obtiene datos de OpenAQ para CDMX en una hora específica."""
    print(f"   🔍 Obteniendo datos OpenAQ CDMX para hora {target_hour}")
    
    # Crear rango de tiempo para esta hora
    datetime_from = target_hour.strftime('%Y-%m-%dT%H:%M:%SZ')
    datetime_to = (target_hour + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    air_quality_data = {}
    
    # Mapeo de parámetros
    parameter_mapping = {
        'co ppm': 'co',
        'no ppm': 'no',
        'no2 ppm': 'no2',
        'nox ppm': 'nox',
        'o3 ppm': 'o3',
        'pm25 µg/m³': 'pm25',
        'so2 ppm': 'so2'
    }
    
    # Obtener datos para cada parámetro disponible
    for sensor_name, sensor_id in sensor_mapping.items():
        if sensor_name in parameter_mapping:
            param_name = parameter_mapping[sensor_name]
            
            mediciones = obtener_mediciones_cdmx_rango(sensor_id, datetime_from, datetime_to)
            
            if mediciones:
                # Tomar la primera medición disponible
                value = mediciones[0]['value']
                air_quality_data[param_name] = value
                print(f"      ✅ {param_name}: {value}")
            else:
                air_quality_data[param_name] = None
                print(f"      ❌ {param_name}: No disponible")
    
    return air_quality_data

def obtener_pm25_lag_cdmx(sensor_mapping, target_hour):
    """Obtiene datos de lag PM2.5 para CDMX usando el método exitoso."""
    print(f"   🔍 Obteniendo PM2.5 lag CDMX para {target_hour}")
    
    lag_hours = [3, 6, 12, 24]
    lag_data = {}
    
    # Buscar sensor PM2.5
    pm25_sensor_id = None
    for sensor_name, sensor_id in sensor_mapping.items():
        if 'pm25' in sensor_name.lower():
            pm25_sensor_id = sensor_id
            print(f"      ✅ Usando sensor PM2.5: {sensor_name} (ID: {sensor_id})")
            break
    
    if not pm25_sensor_id:
        print(f"      ❌ No se encontró sensor PM2.5 en CDMX")
        for lag_hour in lag_hours:
            lag_data[f"pm25_lag_{lag_hour}h"] = None
        return lag_data
    
    # Obtener datos para cada lag
    for hours_back in lag_hours:
        lag_key = f"pm25_lag_{hours_back}h"
        
        # Calcular tiempo objetivo
        target_time = target_hour - timedelta(hours=hours_back)
        datetime_from = target_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        datetime_to = (target_time + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        print(f"      📅 Buscando {lag_key}: {target_time.strftime('%Y-%m-%d %H:%M')}")
        
        mediciones = obtener_mediciones_cdmx_rango(pm25_sensor_id, datetime_from, datetime_to)
        
        if mediciones:
            lag_value = mediciones[0]['value']
            lag_data[lag_key] = lag_value
            print(f"         ✅ {lag_key}: {lag_value}")
        else:
            lag_data[lag_key] = None
            print(f"         ❌ {lag_key}: No disponible")
        
        # Evitar rate limiting
        time.sleep(1)
    
    return lag_data

def get_openmeteo_data_cdmx(lat, lon, start_date, end_date):
    """Obtiene datos meteorológicos para CDMX."""
    base_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "timezone": "America/Mexico_City",
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "relativehumidity_2m", "precipitation", "pressure_msl", 
                   "windspeed_10m", "winddirection_10m", "boundary_layer_height"],
        "daily": ["shortwave_radiation_sum"]
    }
    
    # Construir URL con parámetros múltiples
    hourly_params = "&".join([f"hourly={var}" for var in params['hourly']])
    daily_params = "&".join([f"daily={var}" for var in params['daily']])
    del params['hourly'], params['daily']
    
    import urllib.parse
    full_url = f"{base_url}?{urllib.parse.urlencode(params)}&{hourly_params}&{daily_params}"
    
    import urllib.request
    with urllib.request.urlopen(full_url) as response:
        if response.status != 200:
            raise Exception(f"API Open-Meteo falló con código {response.status}")
        return json.loads(response.read().decode("utf-8"))

def capturar_hora_actual_cdmx():
    """Captura datos de CDMX para la hora actual solamente."""
    print("🌍 CAPTURANDO DATOS HORA ACTUAL - CIUDAD DE MÉXICO")
    print("="*60)
    
    # Obtener sensores disponibles
    sensor_mapping = obtener_sensores_cdmx(CDMX_CONFIG["location_id"])
    if not sensor_mapping:
        print("❌ No se pudieron obtener sensores para CDMX")
        return None
    
    # Obtener hora actual
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    current_hour = now_utc.hour
    
    print(f"⏰ Hora UTC actual: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Capturando datos para hora: {current_hour:02d}:00")
    
    # Obtener datos meteorológicos para todo el día
    print(f"\n🌤️ OBTENIENDO DATOS METEOROLÓGICOS CDMX...")
    weather_data = get_openmeteo_data_cdmx(
        CDMX_CONFIG["lat"], CDMX_CONFIG["lon"],
        today.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
    )
    
    df_hourly = pd.DataFrame(weather_data['hourly'])
    df_hourly['time'] = pd.to_datetime(df_hourly['time'])
    radiation_sum = weather_data['daily']['shortwave_radiation_sum'][0]
    
    all_rows = []
    
    # Procesar solo la hora actual
    hour = current_hour
    print(f"\n📅 PROCESANDO HORA ACTUAL CDMX: {hour:02d}:00")
    
    try:
            # Crear timestamp para esta hora en zona horaria CDMX
            target_timestamp = datetime.combine(today, datetime.min.time().replace(hour=hour))
            target_timestamp = target_timestamp.replace(tzinfo=timezone.utc)
            target_timestamp_cdmx = target_timestamp - timedelta(hours=6)  # UTC-6 para CDMX
            
            print(f"Hora objetivo CDMX: {target_timestamp_cdmx.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Buscar datos meteorológicos para esta hora
            matching_weather = df_hourly[df_hourly['time'].dt.hour == hour]
            
            if not matching_weather.empty:
                current_weather_row = matching_weather.iloc[0]
                
                # Crear nueva fila con formato correcto
                new_row = pd.Series(index=CSV_COLUMNS, dtype='object')
                new_row['timestamp'] = target_timestamp_cdmx
                
                # Añadir datos meteorológicos
                for col in current_weather_row.index:
                    if col in new_row.index:
                        new_row[col] = current_weather_row[col]
                new_row['shortwave_radiation_sum'] = radiation_sum
                
                # Obtener datos de calidad del aire para esta hora
                air_quality_data = obtener_datos_openaq_cdmx_hora(sensor_mapping, target_timestamp_cdmx)
                for param, value in air_quality_data.items():
                    if param in new_row.index:
                        new_row[param] = value
                
                # Obtener datos de lag PM2.5
                lag_data = obtener_pm25_lag_cdmx(sensor_mapping, target_timestamp_cdmx)
                for lag_col, lag_value in lag_data.items():
                    new_row[lag_col] = lag_value
                
                # Añadir características temporales
                new_row['hour_of_day'] = hour
                new_row['day_of_week'] = target_timestamp_cdmx.weekday()
                new_row['month_of_year'] = target_timestamp_cdmx.month
                new_row['is_weekend'] = target_timestamp_cdmx.weekday() >= 5
                
                all_rows.append(new_row)
                print(f"✅ Hora {hour:02d}:00 CDMX procesada exitosamente")
            else:
                print(f"❌ No se encontraron datos meteorológicos para hora {hour:02d}:00")
                
    except Exception as e:
        print(f"❌ Error procesando hora CDMX {hour:02d}:00: {e}")
    
    # Guardar CSV (agregar a archivo existente o crear nuevo)
    if all_rows:
        df_new = pd.DataFrame(all_rows)
        filename = f"datos_realtime_Centro_CDMX.csv"
        
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
        
        print(f"\n🎉 DATOS CAPTURADOS PARA CDMX")
        print(f"📊 Hora procesada: {current_hour:02d}:00")
        print(f"💾 Archivo actualizado: {filename}")
        
        return filename
    else:
        print(f"\n❌ No se pudieron procesar datos para CDMX hora {current_hour:02d}:00")
        return None

def ejecutar_captura_continua_cdmx():
    """Ejecuta la captura de datos cada hora de manera continua."""
    print("🔄 INICIANDO CAPTURA CONTINUA PARA CIUDAD DE MÉXICO")
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
                capturar_hora_actual_cdmx()
                ultima_hora_procesada = hora_actual
                
                # Esperar hasta el siguiente minuto para evitar procesamiento múltiple
                print(f"⏳ Esperando próxima hora...")
            
            # Verificar cada 30 segundos
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n🛑 Captura detenida por el usuario")
        print("💾 Datos guardados hasta el momento")

def main():
    parser = argparse.ArgumentParser(description='Captura datos de Ciudad de México por hora')
    parser.add_argument('--continuo', action='store_true', help='Modo continuo (captura cada hora automáticamente)')
    parser.add_argument('--una-vez', action='store_true', help='Capturar solo la hora actual y salir')
    
    args = parser.parse_args()
    
    print("🚀 SCRIPT ESPECÍFICO PARA CIUDAD DE MÉXICO")
    print("="*50)
    print(f"📍 API Key: {'✅' if API_KEY else '❌'}")
    print(f"🔄 Modo continuo: {'✅' if args.continuo else '❌'}")
    print(f"1️⃣ Solo una vez: {'✅' if args.una_vez else '❌'}")
    
    if not API_KEY:
        print("❌ ERROR: API_KEY_OPENAQ no configurada")
        return
    
    if args.continuo:
        ejecutar_captura_continua_cdmx()
    elif args.una_vez:
        capturar_hora_actual_cdmx()
    else:
        print("\n📋 OPCIONES DISPONIBLES:")
        print("   --continuo    : Captura automática cada hora")
        print("   --una-vez     : Captura solo la hora actual")
        print("\n💡 Ejemplo: python generar_datos_CDMX_24h.py --continuo")

if __name__ == "__main__":
    main()