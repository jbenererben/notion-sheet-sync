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
GOOGLE_CREDENTIALS = os.environ.get('GOOGLE_CREDENTIALS', '{}')  # GOOGLE_CREDENTIALS_JSON yerine GOOGLE_CREDENTIALS 
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
        info = json.loads(GOOGLE_CREDENTIALS)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        )
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"Sheets istemcisi oluşturulurken hata: {str(e)}")
        raise Exception(f"Sheets istemcisi hatası: {str(e)}")

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
        
        # Çalışma sayfasını aç
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        
        try:
            # Mevcut verileri al
            existing_data = sheet.get_all_records()
            
            # Mevcut kayıtları notion_id'ye göre mapla
            existing_map = {row.get('notion_id', ''): (idx + 2, row) for idx, row in enumerate(existing_data)}
            
            # Başlıklar zaten varsa kullan, yoksa oluştur
            if existing_data:
                headers = sheet.row_values(1)
            else:
                # Takvim için gerekli alanları seç
                headers = ['Etkinlik Adı', 'Müşteri', 'Tarih', 'Yer', 'Durum',
                             'Etkinlik Türü', 'Kişi Sayısı', 'notion_id', 'last_edited_time']
                sheet.append_row(headers)
            
            # Yeni veya değiştirilmiş kayıtları güncelle
            updated_count = 0
            new_count = 0
            
            for row in data:
                notion_id = row.get('notion_id', '')
                
                # Takvim için gerekli alanları filtreleme
                filtered_row = {
                    'Etkinlik Adı': row.get('Etkinlik Adı', ''),
                    'Müşteri': row.get('Müşteri', ''),
                    'Tarih': row.get('Tarih', ''),
                    'Yer': row.get('Yer', ''),
                    'Durum': row.get('Durum', ''),
                    'Etkinlik Türü': row.get('Etkinlik Türü', ''),
                    'Kişi Sayısı': row.get('Kişi Sayısı', ''),
                    'NX Kodu': row.get('NX Kodu', ''),
                    'notion_id': notion_id,
                    'last_edited_time': row.get('last_edited_time', '')
                }
                
                if notion_id in existing_map:
                    # Mevcut kayıt - son düzenleme zamanlarını karşılaştır
                    idx, existing_row = existing_map[notion_id]
                    
                    # Notion'daki değişiklik Sheets'teki son güncellemeden sonraysa güncelle
                    if row.get('last_edited_time', '') > existing_row.get('last_edited_time', ''):
                        # Satırı güncelle
                        cell_list = []
                        for col_idx, header in enumerate(headers, start=1):
                            cell_list.append(gspread.Cell(idx, col_idx, str(filtered_row.get(header, ''))))
                        
                        sheet.update_cells(cell_list)
                        updated_count += 1
                else:
                    # Yeni kayıt - sona ekle
                    values = [str(filtered_row.get(header, '')) for header in headers]
                    sheet.append_row(values)
                    new_count += 1
            
             # Sıralama - Etkinlik Adı'na göre sırala
            if existing_data or new_count > 0:
                # Başlık satırını hariç tut ve verileri sırala
                all_data = sheet.get_all_records()
                
                # Etkinlik Adı sütununa göre sırala (varsa)
                if 'Etkinlik Adı' in headers:
                    sorted_data = sorted(all_data, key=lambda x: x.get('Etkinlik Adı', '').lower(), reverse=False)
                    
                    # Sıralanmış verileri geri yaz
                    sheet.update('A2', [
                        [str(row.get(header, '')) for header in headers] 
                        for row in sorted_data
                    ])
            
            return {"updated": updated_count, "new": new_count, "total": len(data)}
            
        except Exception as e:
            print(f"Sheets işlemi sırasında hata: {str(e)}")
            raise Exception(f"Sheets işlemi hatası: {str(e)}")
    except Exception as e:
        print(f"Google Sheets güncelleme hatası: {str(e)}")
        raise Exception(f"Google Sheets güncelleme hatası: {str(e)}")

@app.route('/')
def home():
    return "Notion-Sheets Senkronizasyon Servisi Aktif"

@app.route('/webhook', methods=['POST'])
def webhook():
    # Gelen webhook verilerini al
    data = request.json
    print("Webhook verisi:", data)
    
    # Webhook doğrulama
    if 'challenge' in data:
        challenge_token = data['challenge']
        print(f"Challenge token: {challenge_token}")
        # Bu bir doğrulama isteği
        return jsonify({"challenge": challenge_token})
    
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
        print("Sync endpoint çağrıldı.")
        
        # Notion'dan veri al
        notion_data = get_notion_data()
        print(f"Notion'dan {len(notion_data)} kayıt alındı.")
        
        # Google Sheets'e gönder
        result = update_google_sheet(notion_data)
        print("Güncelleme tamamlandı:", result)
        
        return jsonify({
            "status": "success",
            "message": f"{len(notion_data)} kayıt başarıyla Google Sheets'e aktarıldı."
        })
    except Exception as e:
        error_detail = str(e)
        print(f"Sync hatası: {error_detail}")
        return jsonify({"status": "error", "message": error_detail}), 500
        
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

# Google Sheets'ten veri çekmek için yeni bir fonksiyon
def get_sheets_data():
    """Google Sheets'ten veri çeker"""
    try:
        client = get_sheets_client()
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        
        # Tüm verileri al
        rows = sheet.get_all_records()
        return rows
    except Exception as e:
        print(f"Google Sheets'ten veri çekerken hata: {str(e)}")
        raise Exception(f"Google Sheets veri çekme hatası: {str(e)}")

# Notion'daki bir sayfayı güncellemek için yardımcı fonksiyon
def update_notion_page(page_id, properties):
    """Notion'da bir sayfayı günceller"""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    
    payload = {
        "properties": properties
    }
    
    response = requests.patch(url, headers=NOTION_HEADERS, json=payload)
    
    if response.status_code != 200:
        raise Exception(f"Notion sayfa güncelleme hatası: {response.status_code} - {response.text}")
    
    return response.json()

# Notion'da yeni bir sayfa oluşturmak için yardımcı fonksiyon
def create_notion_page(properties):
    """Notion'da yeni bir sayfa oluşturur"""
    url = "https://api.notion.com/v1/pages"
    
    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": properties
    }
    
    response = requests.post(url, headers=NOTION_HEADERS, json=payload)
    
    if response.status_code != 200:
        raise Exception(f"Notion sayfa oluşturma hatası: {response.status_code} - {response.text}")
    
    return response.json()

# Google Sheets'ten Notion'a veri aktaran ana fonksiyon
def update_notion_from_sheets():
    """Google Sheets'ten Notion'a veri aktarır"""
    try:
        # Google Sheets'ten verileri al
        sheets_data = get_sheets_data()
        print(f"Google Sheets'ten {len(sheets_data)} kayıt alındı.")
        
        # Notion'daki mevcut verileri al
        notion_data = get_notion_data()
        print(f"Notion'dan {len(notion_data)} kayıt alındı.")
        
        # Notion verilerini ID'ye göre mapla
        notion_map = {item.get('notion_id', ''): item for item in notion_data if item.get('notion_id', '')}
        
        # Güncelleme ve ekleme sayaçları
        updated_count = 0
        new_count = 0
        
        # Her Google Sheets satırı için
        for sheet_row in sheets_data:
            notion_id = sheet_row.get('notion_id', '')
            
            # Bu satırın last_edited_time'ı var mı kontrol et
            sheet_last_edited = sheet_row.get('last_edited_time', '')
            
            if notion_id and notion_id in notion_map:
                # Mevcut Notion sayfası - değişiklik var mı kontrol et
                notion_item = notion_map[notion_id]
                notion_last_edited = notion_item.get('last_edited_time', '')
                
                # Google Sheets'teki değişiklik Notion'daki son güncellemeden sonraysa güncelle
                if sheet_last_edited > notion_last_edited:
                    # Notion API için properties nesnesi oluştur
                    properties = build_notion_properties(sheet_row)
                    
                    # Notion sayfasını güncelle
                    update_notion_page(notion_id, properties)
                    updated_count += 1
            elif not notion_id:
                # Notion ID yoksa, bu yeni bir kayıt olabilir
                # Kontrol: Bu satır Google Sheets'te oluşturulmuş yeni bir kayıt mı?
                if all(key in sheet_row for key in ['Etkinlik Adı', 'Müşteri']):
                    # Etkinlik Adı ve Müşteri alanları varsa, bu muhtemelen manuel olarak eklenmiş geçerli bir kayıt
                    # Notion API için properties nesnesi oluştur
                    properties = build_notion_properties(sheet_row)
                    
                    # Notion'da yeni sayfa oluştur
                    response = create_notion_page(properties)
                    new_count += 1
        
        return {"updated": updated_count, "new": new_count, "total": len(sheets_data)}
    except Exception as e:
        print(f"Notion güncelleme hatası: {str(e)}")
        raise Exception(f"Notion güncelleme hatası: {str(e)}")

# Google Sheets verilerinden Notion properties nesnesi oluşturmak için yardımcı fonksiyon
def build_notion_properties(sheet_row):
    """Google Sheets satırından Notion properties nesnesi oluşturur"""
    properties = {}
    
    # Etkinlik Adı (title)
    if 'Etkinlik Adı' in sheet_row and sheet_row['Etkinlik Adı']:
        properties['Etkinlik Adı'] = {
            "title": [{"text": {"content": sheet_row['Etkinlik Adı']}}]
        }
    
    # Müşteri (select)
    if 'Müşteri' in sheet_row and sheet_row['Müşteri']:
        properties['Müşteri'] = {
            "select": {"name": sheet_row['Müşteri']}
        }
    
    # Etkinlik Türü (select)
    if 'Etkinlik Türü' in sheet_row and sheet_row['Etkinlik Türü']:
        properties['Etkinlik Türü'] = {
            "select": {"name": sheet_row['Etkinlik Türü']}
        }
    
    # Tarih (date)
    if 'Tarih' in sheet_row and sheet_row['Tarih']:
        properties['Tarih'] = {
            "date": {"start": sheet_row['Tarih']}
        }
    
    # Kurulum Tarihi (date)
    if 'Kurulum Tarihi' in sheet_row and sheet_row['Kurulum Tarihi']:
        properties['Kurulum Tarihi'] = {
            "date": {"start": sheet_row['Kurulum Tarihi']}
        }
    
    # Yer (rich_text)
    if 'Yer' in sheet_row and sheet_row['Yer']:
        properties['Yer'] = {
            "rich_text": [{"text": {"content": sheet_row['Yer']}}]
        }
    
    # Kişi Sayısı (number)
    if 'Kişi Sayısı' in sheet_row and sheet_row['Kişi Sayısı'] and str(sheet_row['Kişi Sayısı']).isdigit():
        properties['Kişi Sayısı'] = {
            "number": int(sheet_row['Kişi Sayısı'])
        }
    
    # NX Kodu (rich_text)
    if 'NX Kodu' in sheet_row and sheet_row['NX Kodu']:
        properties['NX Kodu'] = {
            "rich_text": [{"text": {"content": sheet_row['NX Kodu']}}]
        }
    
    # Durum (select)
    if 'Durum' in sheet_row and sheet_row['Durum']:
        properties['Durum'] = {
            "select": {"name": sheet_row['Durum']}
        }
    
    return properties

# Sheets'ten Notion'a manuel senkronizasyon endpoint'i
@app.route('/sync-to-notion', methods=['GET'])
def sync_to_notion():
    """Google Sheets'ten Notion'a manuel senkronizasyon"""
    try:
        print("Sheets'ten Notion'a senkronizasyon başladı")
        result = update_notion_from_sheets()
        print("Senkronizasyon tamamlandı:", result)
        
        return jsonify({
            "status": "success",
            "message": f"{result['total']} kayıt işlendi. {result['new']} yeni, {result['updated']} güncellendi."
        })
    except Exception as e:
        error_detail = str(e)
        print(f"Senkronizasyon hatası: {error_detail}")
        return jsonify({"status": "error", "message": error_detail}), 500

# İki yönlü senkronizasyon endpoint'i
@app.route('/sync-both', methods=['GET'])
def sync_both():
    """Notion ve Google Sheets arasında iki yönlü senkronizasyon"""
    try:
        # Önce Notion'dan Google Sheets'e
        notion_data = get_notion_data()
        sheets_result = update_google_sheet(notion_data)
        
        # Sonra Google Sheets'ten Notion'a
        notion_result = update_notion_from_sheets()
        
        return jsonify({
            "status": "success",
            "sheets_sync": f"{sheets_result['total']} kayıt işlendi. {sheets_result['new']} yeni, {sheets_result['updated']} güncellendi.",
            "notion_sync": f"{notion_result['total']} kayıt işlendi. {notion_result['new']} yeni, {notion_result['updated']} güncellendi."
        })
    except Exception as e:
        error_detail = str(e)
        print(f"İki yönlü senkronizasyon hatası: {error_detail}")
        return jsonify({"status": "error", "message": error_detail}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
