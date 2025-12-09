
import streamlit as st
import pandas as pd
import requests
import folium
from streamlit_folium import st_folium
import plotly.express as px
from datetime import date

# --- CONFIGURATION ---
st.set_page_config(page_title="VéloMag Montpellier", page_icon="🚲", layout="wide")
API_URL = "http://127.0.0.1:8000"

# --- FONCTIONS ---
@st.cache_data(ttl=3600)
def get_predictions():
    try:
        response = requests.get(f"{API_URL}/predict")
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
        else:
            df['date'] = date.today()
        return df
    except Exception as e:
        st.error(f"Erreur API: {e}")
        return pd.DataFrame()

def get_color(daily_intensity):
    """Couleurs adaptées aux volumes JOURNALIERS (ex: 2000 passages/jour)"""
    if daily_intensity < 500: return "green"      # Calme
    elif daily_intensity < 1500: return "orange"  # Normal
    else: return "red"                            # Intense

# --- UI ---
st.title("🚲 Météo des Vélos - Montpellier")
st.markdown("Prévisions de trafic journalier (J+3)")

df = get_predictions()

if not df.empty:
    # --- FILTRES (Date uniquement) ---
    st.sidebar.header(" Sélection")
    unique_dates = sorted(df['date'].unique())
    selected_date = st.sidebar.selectbox(
        "Choisir la date", 
        unique_dates, 
        format_func=lambda d: d.strftime('%A %d %B %Y')
    )

    # Filtrage des données pour la journée entière
    df_day = df[df['date'] == selected_date]

    # --- KPI ---
    total_trafic_jour = df_day['predicted_intensity'].sum()
    temp_moy = df_day['temperature_2m'].mean()

    c1, c2, c3 = st.columns(3)
    c1.metric("Date sélectionnée", selected_date.strftime('%d/%m/%Y'))
    c2.metric("Trafic Total Ville", f"{int(total_trafic_jour)} vélos")
    c3.metric("Température Moyenne", f"{temp_moy:.1f}°C")

    # --- CARTE (Trafic cumulé par compteur sur la journée) ---
    st.subheader(f" Carte du volume journalier ({selected_date.strftime('%d/%m')})")
    
    # On agrège par compteur (somme des 24h)
    # .first() permet de garder lat/lon qui sont identiques pour chaque ligne du compteur
    df_map_agg = df_day.groupby('counter_id_encoded').agg({
        'predicted_intensity': 'sum',
        'lat': 'first',
        'lon': 'first'
    }).reset_index()

    m = folium.Map(location=[43.6107, 3.8767], zoom_start=13, tiles="CartoDB positron")

    for _, row in df_map_agg.iterrows():
        total_counter = int(row['predicted_intensity'])
        color = get_color(total_counter)
        
        popup_html = f"""
        <b>{row['counter_id_encoded']}</b><br>
        Volume Jour: {total_counter} vélos
        """
        
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=10 + (total_counter / 200), # Taille basée sur le volume jour
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=200),
            tooltip=f"{row['counter_id_encoded']}: {total_counter}"
        ).add_to(m)

    st_folium(m, width=None, height=500)

    # --- GRAPHIQUE (Profil horaire de la journée) ---
    st.subheader(" Profil horaire de la journée")
    
    df_chart = df_day.groupby('hour')['predicted_intensity'].sum().reset_index()
    
    fig = px.area(
        df_chart, x='hour', y='predicted_intensity',
        title="Evolution du trafic ville heure par heure",
        labels={'predicted_intensity': 'Vélos', 'hour': 'Heure'},
        color_discrete_sequence=['#0072B2']
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- DATA ---
    with st.expander("Données détaillées"):
        st.dataframe(df_day)

else:
    st.warning("Aucune donnée reçue de l'API.")
