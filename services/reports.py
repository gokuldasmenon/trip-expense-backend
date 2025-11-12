import os
import json
import qrcode
import tempfile
from io import BytesIO
from datetime import datetime
from fpdf import FPDF
from database import get_connection
import requests

# ============================================================
# 🧩 Unicode-safe PDF class
# ============================================================
class UnicodePDF(FPDF):
    def __init__(self):
        super().__init__()
        # Detect font dir (local vs Render)
        local_font_dir = os.path.join("D:\\Expensetracker", "flutter_frontend", "assets", "fonts")
        server_font_dir = "/opt/render/project/src/fonts"

        if os.path.exists(local_font_dir):
            font_dir = local_font_dir
        elif os.path.exists(server_font_dir):
            font_dir = server_font_dir
        else:
            font_dir = os.getcwd()

        print(f"🟢 Using font directory: {font_dir}")

        self._load_font(font_dir, "DejaVuSans.ttf", "")
        self._load_font(font_dir, "DejaVuSans-Bold.ttf", "B")
        self._load_font(font_dir, "DejaVuSans-Oblique.ttf", "I")

    def _load_font(self, font_dir, filename, style=""):
        font_path = os.path.join(font_dir, filename)
        if os.path.exists(font_path):
            self.add_font("DejaVu", style, font_path)
            print(f"✅ Loaded font: {font_path}")
        else:
            print(f"⚠️ Font not found: {font_path}")


# ============================================================
# 🧾 Generate Settlement PDF
# ============================================================
def generate_settlement_pdf(trip_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            COALESCE(t.name, CONCAT('Trip #', v.trip_id)) AS trip_name,
            v.total_expense,
            v.total_members,
            v.per_head_cost,
            v.family_summary,
            v.suggested_settlements,
            v.created_at
        FROM v_latest_stay_settlement_snapshot v
        LEFT JOIN trips t ON v.trip_id = t.id
        WHERE v.trip_id = %s
        ORDER BY v.created_at DESC
        LIMIT 1;
    """, (trip_id,))

    record = cursor.fetchone()
    cursor.close()
    conn.close()

    if not record:
        raise ValueError(f"No settlement snapshot found for trip {trip_id}")

    trip_name, total_expense, total_members, per_head_cost, family_summary, suggested, created_at = record
    trip_name = trip_name or f"Trip #{trip_id}"


    # PDF creation
    pdf = UnicodePDF()
    pdf.add_page()
    pdf.set_font("DejaVu", "B", 16)
    pdf.set_fill_color(230, 240, 255)
    pdf.cell(0, 10, f"🧾 Trip Settlement Report — {trip_name or 'Untitled Trip'}",
            ln=True, align="C", fill=True)

    pdf.set_font("DejaVu", "", 12)
    pdf.cell(0, 8, f"📅 Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M')}", ln=True)
    pdf.cell(0, 8, f"🕒 Settlement Date: {created_at.strftime('%Y-%m-%d %H:%M')}", ln=True)
    pdf.cell(0, 8, f"💰 Total Expense: ₹{total_expense} | 👥 Members: {total_members} | 💵 Per Head: ₹{per_head_cost}", ln=True)
    pdf.cell(0, 8, f"Total Members: {total_members}", ln=True)
    pdf.ln(8)

    # Family Summary
    pdf.set_font("DejaVu", "B", 12)
    pdf.cell(0, 10, "Family Settlement Summary", ln=True)
    pdf.set_font("DejaVu", "", 11)

    pdf.cell(60, 8, "Family", 1)
    pdf.cell(30, 8, "Spent", 1)
    pdf.cell(30, 8, "Due", 1)
    pdf.cell(30, 8, "Adjusted", 1)
    pdf.cell(30, 8, "Status", 1, ln=True)

    for f in family_summary:
        adj = f.get("adjusted_balance", 0)
        status = "Settled" if adj == 0 else ("To Receive" if adj > 0 else "To Pay")
        pdf.cell(60, 8, f.get("family_name", ""), 1)
        pdf.cell(30, 8, f"₹{f.get('total_spent', 0)}", 1)
        pdf.cell(30, 8, f"₹{f.get('due_amount', 0)}", 1)
        pdf.cell(30, 8, f"₹{adj}", 1)
        pdf.cell(30, 8, status, 1, ln=True)

    pdf.ln(10)
    pdf.set_font("DejaVu", "B", 13)
    pdf.cell(0, 10, "Suggested Settlements (Who Pays Whom)", ln=True)
    pdf.set_font("DejaVu", "", 11)

    if suggested:
        for s in suggested:
            pdf.cell(0, 8, f"{s['from']} → {s['to']} : ₹{s['amount']}", ln=True)
    else:
        pdf.cell(0, 8, "✅ All accounts settled!", ln=True)

    # QR Code
    pdf.ln(10)
    frontend_base_url = "https://trip-expense-backend.onrender.com/"
    qr_data = f"{frontend_base_url}/trip/{trip_id}"

    # ✅ Generate QR safely
    qr_img = qrcode.make(qr_data)
    temp_dir = tempfile.gettempdir()
    qr_path = os.path.join(temp_dir, f"trip_{trip_id}_qr.png")
    qr_img.save(qr_path)

    # ✅ Place QR clearly in the bottom-right corner
    y_position = pdf.get_y() + 5
    pdf.image(qr_path, x=pdf.w - 50, y=y_position, w=40)
              
    pdf.ln(45)
    pdf.set_font("DejaVu", "I", 9)
    pdf.cell(0, 10, f"Scan QR to view trip #{trip_id}", ln=True, align="R")

    # Save
    filename = f"Trip_{trip_id}_Settlement_Report.pdf"
    output_path = os.path.join(temp_dir, filename)
    pdf.output(output_path)
    print(f"✅ PDF generated successfully: {output_path}")
    return output_path


# ============================================================
# 📤 Share PDF on WhatsApp (Mock link generator)
# ============================================================
def send_whatsapp_message(pdf_url: str, trip_id: int, recipient: str = "<RECIPIENT_PHONE_NUMBER>"):
    """Send or simulate a WhatsApp message with a trip settlement link."""
    # --- WhatsApp Business API endpoint ---
    whatsapp_api_url = "https://graph.facebook.com/v17.0/YOUR_PHONE_ID/messages"

    # --- Message payload ---
    payload = {
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "text",
        "text": {"body": f"Trip #{trip_id} Settlement Report 📊\n{pdf_url}"}
    }

    headers = {
        "Authorization": "Bearer YOUR_WHATSAPP_ACCESS_TOKEN",
        "Content-Type": "application/json"
    }

    try:
        res = requests.post(whatsapp_api_url, json=payload, headers=headers)
        print(f"📤 WhatsApp API Response: {res.status_code} - {res.text}")

        if res.status_code == 200:
            return {"status": "sent", "response": res.json()}
        else:
            return {"status": "failed", "response": res.text}

    except Exception as e:
        print(f"❌ WhatsApp send failed: {e}")
        return {"status": "error", "error": str(e)}
def share_pdf_via_whatsapp(trip_id: int):
    """Generate a settlement PDF and send a WhatsApp message link."""
    pdf_path = generate_settlement_pdf(trip_id)

    # Step 1: Create a mock public URL (replace with your backend domain or S3 link)
    pdf_filename = os.path.basename(pdf_path)
    pdf_url = f"https://yourdomain.com/reports/{pdf_filename}"

    # Step 2: Send WhatsApp message (comment out to simulate only)
    result = send_whatsapp_message(pdf_url, trip_id)

    # Step 3: Return final payload
    return {
        "trip_id": trip_id,
        "pdf_path": pdf_path,
        "pdf_url": pdf_url,
        "whatsapp_status": result.get("status", "simulated"),
        "response": result.get("response", "message simulated for local test")
    }



