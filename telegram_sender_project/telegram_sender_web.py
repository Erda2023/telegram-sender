import pandas as pd
import requests
from flask import Flask, request, jsonify
import asyncio
from telethon import TelegramClient

# ========================== SETTINGS ==========================

# Your Telegram credentials
api_id = 29615453        # Replace with your API ID
api_hash = '2c41c286147313e7831c1c57f8e63414'   # Replace with your API Hash

# Your Google Sheet Export CSV link
CSV_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSzzmvvQS2kTHgt4eqIpODhDw_p9aIX0rpnePOxkcuoIUbZ8XG5Gjv5gMrL2CgVi9z6hi3jqiNUyPtI/pub?output=csv'   # ⚡ Example: 'https://docs.google.com/spreadsheets/d/xxxxxx/export?format=csv'

# ===============================================================

# Initialize Flask app
app = Flask(__name__)

# Initialize Telethon client
client = TelegramClient('session_name', api_id, api_hash)

@app.route('/send', methods=['POST'])
def send_to_telegram():
    try:
        asyncio.run(send_messages())
        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

async def send_messages():
    # Download the sheet
    df = pd.read_csv(CSV_URL)
    df.columns = [col.strip().lower() for col in df.columns]

    print("🧾 Columns found:", df.columns.tolist())
    print("📋 Loaded data:", df.head())

    # Start the Telegram client
    await client.start()

    for index, row in df.iterrows():
        send_flag = str(row.get('send', '')).strip().lower()
        print(f"➡️ Checking row {index}: Send = {send_flag}")

        if send_flag != 'true':
            print(f"⏩ Skipping row {index}")
            continue

        try:
            username = row['username']
            chat_id = str(row['chat id']).strip()
            adv_name = row['adv name']
            sheet_link = row['sheet link']
            period_tab = row['period tab']
            balance_date = row['balance date']
            balance_amount = row['balance amount']

            # Read advertiser tab
            sheet_id = sheet_link.split("/d/")[1].split("/")[0]
            tab_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={period_tab}"
            adv_df = pd.read_csv(tab_url)
            adv_df.columns = [col.strip().lower() for col in adv_df.columns]

            # Find columns
            geo_col = next((col for col in adv_df.columns if 'geo' in col), 'geo')
            leads_col = next((col for col in adv_df.columns if 'cg leads' in col), 'cg leads')
            ftd_col = next((col for col in adv_df.columns if 'ftd' in col.lower()), 'ftds')
            crg_col = next((col for col in adv_df.columns if 'cg' == col.strip().lower()), 'cg')
            cpa_col = next((col for col in adv_df.columns if 'cpa' in col), 'cpa')
            total_cost_col = next((col for col in adv_df.columns if 'total cost' in col), 'total cost')
            notes_col = next((col for col in adv_df.columns if 'note' in col), 'notes')

            adv_df_filtered = adv_df[
                adv_df[geo_col].notna() &
                adv_df[leads_col].notna() &
                (adv_df[leads_col] != 0)
            ]

            if adv_df_filtered.empty:
                print(f"⚠️ No rows with valid GEO and CG Leads found for {adv_name}.")
                continue

            # Start composing message
            message = f"Hi {username},\n\nSharing last week’s {adv_name} report — feel free to take a look:\n\n"

            total_week_cost = 0

            for i, adv_row in adv_df_filtered.iterrows():
                geo = adv_row.get(geo_col, 'Unknown')
                cg_leads = adv_row.get(leads_col, '')
                ftds = adv_row.get(ftd_col, '')
                cg = adv_row.get(crg_col, '')
                cpa = adv_row.get(cpa_col, '')
                total_cost = adv_row.get(total_cost_col, 0)
                notes = adv_row.get(notes_col, '')

                if pd.isna(cg_leads) or float(cg_leads) == 0:
                    continue

                try:
                    cpa = float(str(cpa).replace('$', '').replace(',', ''))
                    total_cost = float(str(total_cost).replace('$', '').replace(',', ''))
                except:
                    cpa = cpa
                    total_cost = 0

                # Build GEO block
                message += (
                    f"🔹 GEO: {geo}\n"
                )
                if not pd.isna(ftds) and ftds != '':
                    message += f"• FTDs: {int(ftds) if float(ftds).is_integer() else ftds}\n"
                message += (
                    f"• CG Leads: {int(cg_leads) if float(cg_leads).is_integer() else cg_leads}\n"
                    f"• CG: {cg}\n"
                    f"• CPA: ${int(cpa) if isinstance(cpa, float) and cpa.is_integer() else cpa}\n"
                    f"• Total Cost: ${int(total_cost) if isinstance(total_cost, float) and total_cost.is_integer() else f'{total_cost:.2f}'}\n"
                )
                if not pd.isna(notes) and str(notes).strip().lower() != 'nan':
                    message += f"• Notes: {notes}\n"
                message += "\n"

                total_week_cost += total_cost

            # Add cost summary
            message += f"💰 Cost for the week {period_tab}: ${int(total_week_cost) if total_week_cost.is_integer() else f'{total_week_cost:.2f}'}\n\n"
            message += "Please review and confirm the calculation provided."

            # Send FIRST message
            await client.send_message(int(chat_id), message)

            # Send SECOND message (wallets)
            wallets_message = (
                f"Balance until {balance_date}: ${balance_amount}\n\n"
                "When you have a moment, please help us with the payment + Top Up\n\n"
                "USDT TRC20 (+2%)\n"
                "TYHNjMiUzCkAfqTDEUzwn2tJSvc5VtNyir\n\n"
                "USDT ERC20 (+2%)\n"
                "0x7F856e17da3c79301b345F7c97983f9D30821684"
            )
            await client.send_message(int(chat_id), wallets_message)

            print(f"✅ Messages sent successfully to group {chat_id} tagging {username}")

        except Exception as e:
            print(f"❌ Error sending to {username}: {e}")

    await client.disconnect()

# ========== START ==========

if __name__ == '__main__':
    app.run(port=5000)

