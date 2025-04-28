from flask import Flask, request, jsonify
import os
import json
import requests
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account

app = Flask(__name__)

# Çevre değişkenlerini al
NOTION_TOKEN = os.environ.get('NOTION_TOKEN', '')
NOTION_DATABASE_ID = os.environ.get('NOTION_DATABASE_ID', '')
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS', '{}')
GOOGLE_SHEET_NAME = os.environ.get('GOOGLE_SHEET_NAME', '')

# Notion API headers
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_sheets_client():
    """Google Sheets API istemcisi oluşturur"""
    try:
        info = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        )
        return gspread.authorize(creds)
    except Exception as e:
        print(f"Sheets istemcisi oluşturulurken hata: {str(e)}")
        raise e

def get_notion_data():
    """Notion veritabanından veri çeker"""
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    
    response = requests.post(url, headers=NOTION_HEADERS)
    
    if response.status_code != 200:
        raise Exception(f"Notion API hatası: {response.status_code} - {response.text}")
    
    data = response.json()
    results = []
    
    for item in data.get('results', []):
        row_data = {}
        properties = item.get('properties', {})
        
        # Farklı veri tiplerini işle
        for prop_name, prop_data in properties.items():
            prop_type = prop_data.get('type', '')
            
            if prop_type == 'title':
                title_array = prop_data.get('title', [])
                row_data[prop_name] = title_array[0].get('plain_text', '') if title_array else ''
            elif prop_type == 'rich_text':
                text_array = prop_data.get('rich_text', [])
                row_data[prop_name] = text_array[0].get('plain_text', '') if text_array else ''
            elif prop_type == 'number':
                row_data[prop_name] = prop_data.get('number', 0)
            elif prop_type == 'select':
                select_data = prop_data.get('select', {})
                row_data[prop_name] = select_data.get('name', '') if select_data else ''
            elif prop_type == 'multi_select':
                multi_select = prop_data.get('multi_select', [])
                names = [item.get('name', '') for item in multi_select if item]
                row_data[prop_name] = ', '.join(names)
            elif prop_type == 'date':
                date_data = prop_data.get('date', {})
                row_data[prop_name] = date_data.get('start', '') if date_data else ''
            elif prop_type == 'checkbox':
                row_data[prop_name] = prop_data.get('checkbox', False)
            else:
                row_data[prop_name] = f"[{prop_type}]"
        
        # Kimlik ve düzenleme zamanı ekle
        row_data['notion_id'] = item.get('id', '')
        row_data['last_edited_time'] = item.get('last_edited_time', '')
        
        results.append(row_data)
    
    return results

def update_google_sheet(data):
    """Google Sheets'e veri yazar, sadece değişen kayıtları günceller"""
    try:
        client = get_sheets_client()
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        
        # Mevcut verileri al
        existing_data = []
        try:
            existing_data = sheet.get_all_records()
        except:
            # Sayfa boşsa veya başlıklar yoksa hata alınabilir
            pass
        
        # Mevcut kayıtları notion_id'ye göre mapla
        existing_map = {row.get('notion_id', ''): (idx + 2, row) for idx, row in enumerate(existing_data)}
        
        # Başlıkları topla
        all_keys = set()
        for row in data:
            all_keys.update(row.keys())
        
        # ID ve zaman alanlarını sona taşı
        headers = [key for key in all_keys if key not in ['notion_id', 'last_edited_time']]
        if 'notion_id' in all_keys:
            headers.append('notion_id')
        if 'last_edited_time' in all_keys:
            headers.append('last_edited_time')
        
        # Sayfa boşsa veya yeniden düzenlemek istiyorsak başlıkları ekle
        if not existing_data:
            sheet.clear()
            sheet.append_row(headers)
        
        # Yeni veya değiştirilmiş kayıtları güncelle
        updated_count = 0
        new_count = 0
        
        for row in data:
            notion_id = row.get('notion_id', '')
            
            if notion_id in existing_map:
                # Mevcut kayıt - son düzenleme zamanlarını karşılaştır
                idx, existing_row = existing_map[notion_id]
                
                # Notion'daki değişiklik Sheets'teki son güncellemeden sonraysa güncelle
                if row.get('last_edited_time', '') > existing_row.get('last_edited_time', ''):
                    # Satırı güncelle
                    cell_list = []
                    for col_idx, header in enumerate(headers, start=1):
                        cell_list.append(gspread.Cell(idx, col_idx, str(row.get(header, ''))))
                    
                    sheet.update_cells(cell_list)
                    updated_count += 1
            else:
                # Yeni kayıt - sona ekle
                values = [row.get(header, '') for header in headers]
                sheet.append_row(values)
                new_count += 1
        
        return {"updated": updated_count, "new": new_count, "total": len(data)}
    except Exception as e:
        print(f"Google Sheets güncellenirken hata: {str(e)}")
        raise e

@app.route('/')
def home():
    return "Notion-Sheets Senkronizasyon Servisi Aktif"

@app.route('/webhook', methods=['POST'])
def webhook():
    # Gelen webhook verilerini al
    data = request.json
    
    # Webhook doğrulama
    if 'challenge' in data:
        # Bu bir doğrulama isteği
        return jsonify({"challenge": data['challenge']})
    
    # Webhook verilerini loglama
    print("Webhook alındı:", data)
    
    try:
        # Veritabanı değişikliği webhook'u
        notion_data = get_notion_data()
        print(f"{len(notion_data)} Notion kaydı bulundu")
        
        # Google Sheets'e gönder
        update_google_sheet(notion_data)
        
        return jsonify({
            "status": "success", 
            "message": f"{len(notion_data)} kayıt işlendi"
        })
    except Exception as e:
        print(f"Hata: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/sync', methods=['GET'])
def manual_sync():
    """Manuel senkronizasyon için endpoint"""
    try:
        notion_data = get_notion_data()
        result = update_google_sheet(notion_data)
        return jsonify({
            "status": "success",
            "message": f"{result['total']} kayıt işlendi. {result['new']} yeni, {result['updated']} güncellendi."
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/test-notion', methods=['GET'])
def test_notion():
    """Notion bağlantısını test et"""
    try:
        notion_data = get_notion_data()
        return jsonify({
            "status": "success",
            "record_count": len(notion_data),
            "data": notion_data[:2]  # Sadece ilk 2 kaydı göster
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
