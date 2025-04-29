from flask import Flask, request, jsonify
import os
import json
import requests
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account

def get_last_sync_time():
    """Son senkronizasyon zamanını çevre değişkeninden okur"""
    return os.environ.get("LAST_SYNC_TIME", "")

def save_last_sync_time():
    """Şu anki zamanı son senkronizasyon zamanı olarak kaydeder"""
    current_time = datetime.now().isoformat()
    print(f"ÖNEMLİ: LAST_SYNC_TIME={current_time} değerini Render.com ortam değişkenlerine ekleyin")
    return current_time

def resolve_conflicts(notion_item, sheet_row):
    """İki sistemde aynı anda yapılan değişiklikleri çözümler"""
    notion_edited = notion_item.get('last_edited_time', '')
    sheet_edited = sheet_row.get('last_edited_time', '')
    
    # Eğer son düzenleme zamanları farklıysa, daha yeni olanı tercih et
    if notion_edited and sheet_edited:
        if notion_edited > sheet_edited:
            return "notion"  # Notion'daki değişiklik daha yeni
        else:
            return "sheet"   # Sheets'teki değişiklik daha yeni
    elif notion_edited:
        return "notion"
    elif sheet_edited:
        return "sheet"
    
    return "notion"  # Varsayılan olarak Notion'ı tercih et

def get_notion_data(filter_recent=False):
    """Notion veritabanından veri çeker, opsiyonel olarak son değişiklikleri filtreler"""
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    
    payload = {}
    if filter_recent:
        # Son 24 saatte değişmiş kayıtları al
        last_sync = get_last_sync_time()
        if last_sync:
            payload["filter"] = {
                "property": "last_edited_time",
                "date": {
                    "on_or_after": last_sync
                }
            }
    
    response = requests.post(url, headers=NOTION_HEADERS, json=payload)
    
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

def delete_from_sheets(notion_id):
    """Sheets'ten bir kaydı siler"""
    try:
        client = get_sheets_client()
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        
        # Tüm kayıtları al
        records = sheet.get_all_records()
        
        # Silinecek kaydın satır numarasını bul
        row_num = None
        for idx, record in enumerate(records):
            if record.get('notion_id') == notion_id:
                row_num = idx + 2  # +2: başlık satırı ve 0-indeksli olduğu için
                break
        
        # Eğer kayıt bulunduysa sil
        if row_num:
            sheet.delete_row(row_num)
            print(f"Sheets'ten silindi: {notion_id} (Satır: {row_num})")
            return True
        
        return False
    except Exception as e:
        print(f"Sheets'ten silme hatası: {str(e)}")
        return False

def delete_from_notion(notion_id):
    """Notion'dan bir sayfayı siler (arşivler)"""
    try:
        url = f"https://api.notion.com/v1/pages/{notion_id}"
        
        # Notion, silme yerine arşivleme kullanır
        payload = {"archived": True}
        
        response = requests.patch(url, headers=NOTION_HEADERS, json=payload)
        
        if response.status_code != 200:
            print(f"Notion silme hatası: {response.status_code} - {response.text}")
            return False
        
        print(f"Notion'dan silindi (arşivlendi): {notion_id}")
        return True
    except Exception as e:
        print(f"Notion'dan silme hatası: {str(e)}")
        return False

def handle_deleted_records():
    """Bir sistemde silinen kayıtları diğer sistemde de siler"""
    try:
        # Notion'daki tüm kayıtları al
        notion_data = get_notion_data()
        notion_ids = {item.get('notion_id', '') for item in notion_data if item.get('notion_id', '')}
        
        # Sheets'teki tüm kayıtları al
        sheets_data = get_sheets_data()
        sheets_notion_ids = {row.get('notion_id', '') for row in sheets_data if row.get('notion_id', '')}
        
        # Notion'da olmayan ama Sheets'te olan kayıtları bul (Notion'dan silinmiş)
        deleted_from_notion = sheets_notion_ids - notion_ids
        notion_deleted_count = 0
        for notion_id in deleted_from_notion:
            if notion_id:  # Boş ID'leri atla
                # Bu kaydı Sheets'ten sil
                if delete_from_sheets(notion_id):
                    notion_deleted_count += 1
        
        # Sheets'te olmayan ama Notion'da olan kayıtları bul (Sheets'ten silinmiş)
        deleted_from_sheets = notion_ids - sheets_notion_ids
        sheets_deleted_count = 0
        for notion_id in deleted_from_sheets:
            if notion_id:  # Boş ID'leri atla
                # Bu kaydı Notion'dan sil
                if delete_from_notion(notion_id):
                    sheets_deleted_count += 1
        
        return {"notion": notion_deleted_count, "sheets": sheets_deleted_count}
    except Exception as e:
        print(f"Silinen kayıtları işleme hatası: {str(e)}")
        return {"notion": 0, "sheets": 0}

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
        
        # Google Sheets istemcisini hazırla (yeni notion_id'leri geri aktarmak için)
        client = get_sheets_client()
        sheet = client.open(GOOGLE_SHEET_NAME).sheet1
        
        # Başlıkları al
        headers = sheet.row_values(1)
        notion_id_col = headers.index('notion_id') + 1 if 'notion_id' in headers else None
        
        # Her Google Sheets satırı için
        for idx, sheet_row in enumerate(sheets_data):
            row_num = idx + 2  # Sheets'te satır numarası (başlık satırı + 1)
            notion_id = sheet_row.get('notion_id', '')
            
            if notion_id and notion_id in notion_map:
                # Mevcut Notion sayfası - değişiklik var mı kontrol et
                notion_item = notion_map[notion_id]
                
                # Değişiklik kontrolü - her iki kaydı karşılaştır
                has_changes = False
                
                # Önemli alanları kontrol et
                for field in ['Etkinlik Adı', 'Müşteri', 'Tarih', 'Yer', 'Durum', 'Etkinlik Türü']:
                    if field in sheet_row and field in notion_item:
                        if str(sheet_row.get(field, '')) != str(notion_item.get(field, '')):
                            has_changes = True
                            print(f"Değişiklik tespit edildi - {field}: '{notion_item.get(field, '')}' -> '{sheet_row.get(field, '')}'")
                            break
                
                # Son düzenleme zamanı kontrolü
                sheet_last_edited = sheet_row.get('last_edited_time', '')
                notion_last_edited = notion_item.get('last_edited_time', '')
                time_based_update = not sheet_last_edited or not notion_last_edited or sheet_last_edited > notion_last_edited
                
                # Değişiklik varsa güncelle
                if has_changes or time_based_update:
                    # Notion API için properties nesnesi oluştur
                    properties = build_notion_properties(sheet_row)
                    
                    # Notion sayfasını güncelle
                    update_notion_page(notion_id, properties)
                    updated_count += 1
                    print(f"Kayıt güncellendi: {notion_id}")
            elif not notion_id:
                # Notion ID yoksa, bu yeni bir kayıt olabilir
                # Kontrol: Bu satır Google Sheets'te oluşturulmuş yeni bir kayıt mı?
                if 'Etkinlik Adı' in sheet_row and sheet_row['Etkinlik Adı']:
                    # Etkinlik Adı alanı varsa, bu muhtemelen manuel olarak eklenmiş geçerli bir kayıt
                    # Notion API için properties nesnesi oluştur
                    properties = build_notion_properties(sheet_row)
                    
                    # Notion'da yeni sayfa oluştur
                    response = create_notion_page(properties)
                    new_notion_id = response.get('id', '')
                    new_count += 1
                    print(f"Yeni kayıt oluşturuldu: {new_notion_id}")
                    
                    # Yeni notion_id'yi Google Sheets'e geri aktar
                    if notion_id_col and new_notion_id:
                        sheet.update_cell(row_num, notion_id_col, new_notion_id)
                        print(f"Yeni notion_id Google Sheets'e aktarıldı: {new_notion_id}")
                        
                        # last_edited_time sütunu varsa güncelle
                        last_edited_col = headers.index('last_edited_time') + 1 if 'last_edited_time' in headers else None
                        if last_edited_col:
                            current_time = datetime.now().isoformat()
                            sheet.update_cell(row_num, last_edited_col, current_time)
        
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
    
    # Müşteri (rich_text)
    if 'Müşteri' in sheet_row and sheet_row['Müşteri']:
        properties['Müşteri'] = {
            "rich_text": [{"text": {"content": sheet_row['Müşteri']}}]
        }
    
    # Etkinlik Türü (rich_text)
    if 'Etkinlik Türü' in sheet_row and sheet_row['Etkinlik Türü']:
        properties['Etkinlik Türü'] = {
            "rich_text": [{"text": {"content": sheet_row['Etkinlik Türü']}}]
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
    if 'Kişi Sayısı' in sheet_row and sheet_row['Kişi Sayısı'] and str(sheet_row['Kişi Sayısı']).replace('.', '').isdigit():
        properties['Kişi Sayısı'] = {
            "number": float(str(sheet_row['Kişi Sayısı']).replace(',', '.'))
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

@app.route('/sync-optimized', methods=['GET'])
def sync_optimized():
    """Optimize edilmiş iki yönlü senkronizasyon"""
    try:
        # Son senkronizasyon zamanını al
        last_sync = get_last_sync_time()
        print(f"Son senkronizasyon: {last_sync}")
        
        # 1. Notion'dan değişen verileri al
        notion_data = get_notion_data(filter_recent=True) if last_sync else get_notion_data()
        print(f"Notion'dan {len(notion_data)} kayıt alındı")
        
        # 2. Sheets'ten değişen verileri al
        sheets_data = get_sheets_data()
        print(f"Sheets'ten {len(sheets_data)} kayıt alındı")
        
        # 3. Notion'daki değişiklikleri Sheets'e aktar
        sheets_result = update_google_sheet(notion_data)
        
        # 4. Sheets'teki değişiklikleri Notion'a aktar
        notion_result = update_notion_from_sheets()
        
        # 5. Silinen kayıtları işle
        deleted_result = handle_deleted_records()
        
        # 6. Son senkronizasyon zamanını güncelle
        new_sync_time = save_last_sync_time()
        
        return jsonify({
            "status": "success",
            "last_sync": last_sync,
            "new_sync": new_sync_time,
            "sheets_sync": f"{sheets_result['total']} kayıt işlendi. {sheets_result['new']} yeni, {sheets_result['updated']} güncellendi.",
            "notion_sync": f"{notion_result['total']} kayıt işlendi. {notion_result['new']} yeni, {notion_result['updated']} güncellendi.",
            "deleted": f"{deleted_result['notion']} kayıt Notion'dan, {deleted_result['sheets']} kayıt Sheets'ten silindi."
        })
    except Exception as e:
        error_detail = str(e)
        print(f"Senkronizasyon hatası: {error_detail}")
        return jsonify({"status": "error", "message": error_detail}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
