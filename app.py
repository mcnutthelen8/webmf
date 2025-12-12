from flask import Flask, redirect, url_for, jsonify, render_template, request, Response
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timedelta
import pytz
import random
import requests
from datetime import datetime, time, timezone
from collections import Counter


app = Flask(__name__)

# MongoDB connection URI
mongo_uri = "mongodb+srv://redgta36:J6n7Hoz2ribHmMmx@moneyfarm.wwzcs.mongodb.net/?retryWrites=true&w=majority&appName=moneyfarm"

# Connect to the MongoDB cluster
client = MongoClient(mongo_uri)

# Specify the database and collection
db = client['MoneyFarmV10']  # Replace with your database name


@app.route("/get-farm-activity")
def get_farm_activity():
    doc = db["maindb"].find_one({"type": "farm_activity"}) or {}
    records = doc.get("records", [])
    records = records[::-1]
    
    return jsonify(records)

@app.route("/get-analytics")
def get_analytics():
    """Get analytics data for the new widgets"""
    doc = db["maindb"].find_one({"type": "farm_activity"}) or {}
    records = doc.get("records", [])
    
    # Failed Sites Analysis - sites that don't have exactly "3 | 4"
    failed_sites = 0
    for record in records:
        sites = record.get("sites", "")
        if sites != "3 | 4":
            failed_sites += 1
    
    # Duplicate Analysis with detailed breakdown
    ips = [record.get("ip") for record in records if record.get("ip")]
    fingerprints = [record.get("fingerprint") for record in records if record.get("fingerprint")]
    
    # Count duplicates and get details
    ip_counts = Counter(ips)
    fingerprint_counts = Counter(fingerprints)
    
    duplicate_ips = {ip: count for ip, count in ip_counts.items() if count > 1}
    duplicate_fingerprints = {fp: count for fp, count in fingerprint_counts.items() if count > 1}
    
    # Connection Type Analysis from actual data
    durations = [record.get("duration") for record in records if record.get("duration")]
    duration_counts = Counter(durations)
    
    connection_types = {
        "Business": duration_counts.get("Business", 0),
        "Residential": duration_counts.get("Residential", 0),
        "Wireless": duration_counts.get("Wireless", 0)
    }
    
    # Country Usage Analysis
    countries = [record.get("country") for record in records if record.get("country")]
    country_counts = dict(Counter(countries))
    
    return jsonify({
        "failed_sites": failed_sites,
        "duplicate_ips": duplicate_ips,
        "duplicate_fingerprints": duplicate_fingerprints,
        "connection_types": connection_types,
        "country_usage": country_counts
    })

def fetch_stats_and_farm():
    doc = db["maindb"].find_one({"type": "main"}) or {}
    stats = {
        "views_td": int(doc.get("views_td", 0)),
        "views_at": int(doc.get("views_at", 0)),
        "views_td_conf": int(doc.get("views_td_conf", 0)),
        "views_at_conf": int(doc.get("views_at_conf", 0)),
        "earn_td": float(doc.get("earn_td", 0.0)),
        "earn_at": float(doc.get("earn_at", 0.0)),
        "earn_td_conf": float(doc.get("earn_td_conf", 0.0)),
        "earn_at_conf": float(doc.get("earn_at_conf", 0.0)),
        "cpm": doc.get("cpm", ""),
        "lastconf_update": doc.get("lastconf_update", "")
    }

    farm_doc = db["maindb"].find_one({"type": "farmstat"}) or {}
    farmstats = []
    for i in range(1, 7):
        farmstats.append({
            "farm": f"Farm{i}",
            "lastres": farm_doc.get(f"lastres_farm{i}", ""),
            "views_td": farm_doc.get(f"views_td_farm{i}", 0),
            "views_at": farm_doc.get(f"views_at_farm{i}", 0)
        })

    return stats, farmstats


@app.route('/get-stats')
def get_stats():
    stats, farmstats = fetch_stats_and_farm()
    return jsonify({"stats": stats, "farmstats": farmstats})

@app.route('/farm-action', methods=['POST'])
def farm_action():
    """Handle farm control actions"""
    data = request.json
    farm_id = data.get('farm_id')
    action = data.get('action')
    
    # Mock implementation - replace with actual farm control logic
    response_msg = f"{action} action executed for {farm_id}"
    
    return jsonify({
        "success": True,
        "message": response_msg,
        "farm_id": farm_id,
        "action": action
    })





@app.route('/update-main-db', methods=['POST'])
def update_main_db():
    try:
        # Check if the request is JSON (from analytics page)
        if request.is_json:
            data = request.json
            update_data = {}
            
            # Handle analytics update
            for key in ['earn_at_conf', 'earn_at', 'views_at', 'views_at_conf', 'cpm', 'lastconf_update']:
                if key in data:
                    update_data[key] = data[key]
            
            if update_data:
                db["maindb"].update_one(
                    {"type": "main"},
                    {"$set": update_data},
                    upsert=True
                )
                return jsonify({"status": "success", "message": "Dashboard updated successfully!"})
            
            return jsonify({"status": "failed", "message": "No valid data provided"}), 400
            
        # Handle form data (from database page)
        time_period = request.form.get('time')
        data_type = request.form.get('type')
        value = request.form.get('value')

        # Convert numeric values to float if possible
        try:
            value_float = float(value)
        except:
            value_float = None  # keep string for cpm

        main_data = db["maindb"].find_one({"type": "main"})
        if not main_data:
            return jsonify({"status": "failed", "message": "Main data not found"}), 404

        # Determine fields to update
        field_map = {
            "views": {"today": "views_td", "alltime": "views_at"},
            "earn":  {"today": "earn_td",  "alltime": "earn_at"},
            "cpm":   {"today": "cpm_td",  "alltime": "cpm_at"}  # make sure cpm_at exists
        }

        field_to_update = field_map.get(data_type, {}).get(time_period)
        if not field_to_update:
            return jsonify({"status": "failed", "message": "Invalid selection"}), 400

        update_data = {field_to_update: value}
        # For views/earn, also update _conf field and lastconf_update
        if data_type in ["views", "earn"]:
            conf_field = field_to_update + "_conf"
            # Use numeric value if possible, else skip
            if value_float is not None:
                update_data[conf_field] = value_float
            update_data["lastconf_update"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        db["maindb"].update_one(
            {"type": "main"},
            {"$set": update_data}
        )

        return jsonify({"status": "success", "message": "Updated successfully!"})

    except Exception as e:
        return jsonify({"status": "failed", "message": f"Error: {str(e)}"}), 500

def add_or_update_link(record, site_key):
    """
    Add or update a link record in the 'url_list' collection.
    If the id already exists, update only the given site key.
    If not, insert a new record with that id.
    """
    query = {"type": "url_list", "records.id": record["id"]}
    doc = db["maindb"]
    # Try to update existing record
    update = {"$set": {f"records.$.{site_key}": record[site_key]}}
    result = doc.update_one(query, update)

    if result.matched_count == 0:
        # No existing record found, so insert a new one
        insert_query = {"type": "url_list"}
        insert_update = {"$push": {"records": record}}
        doc.update_one(insert_query, insert_update, upsert=True)

@app.route("/upload-links", methods=["POST"])
def upload_links():
    data = request.json
    records = data.get("records", [])
    site_key = data.get("site_key")  # e.g. "site1", "site2", etc.

    if not records:
        return jsonify({"status": "failed", "message": "Missing input links"}), 400
    if not site_key:
        return jsonify({"status": "failed", "message": "Missing site key"}), 400
    try:
        for record in records:
            add_or_update_link(record, site_key)

        return jsonify({"status": "success", "message": f"{len(records)} links processed"})
    except Exception as e:
        return jsonify({"status": "failed", "message": str(e)}), 500

@app.route('/update-shortlink-db', methods=['POST'])
def update_shortlink_db():
    try:
        date = request.form.get('date')           # only used if single record
        site = request.form.get('site')           # site1, site2, site3, site4
        metric = request.form.get('metric')       # all, views, earning, cpm
        value = request.form.get('value')         # could be single or multi-line

        if not all([site, metric, value]):
            return jsonify({"status": "failed", "message": "Missing required fields"}), 400

        # Find or create document
        shortlink_doc = db["maindb"].find_one({"type": "shortlink_statics"})
        if not shortlink_doc:
            shortlink_doc = {"type": "shortlink_statics", "site1": [], "site2": [], "site3": [], "site4": []}
            db["maindb"].insert_one(shortlink_doc)

        site_records = shortlink_doc.get(site, [])

        def upsert_record(record_date, views, earning, cpm):
            nonlocal site_records
            found = False
            for rec in site_records:
                if rec.get("Date") == record_date:
                    rec["Views"] = views
                    rec["Earning"] = earning
                    rec["CPM"] = cpm
                    found = True
                    break
            if not found:
                site_records.append({
                    "Date": record_date,
                    "Views": views,
                    "Earning": earning,
                    "CPM": cpm
                })

        # ==========================
        # Handle bulk input for "All"
        # ==========================
        if metric == "all":
                lines = [l.strip() for l in value.splitlines() if l.strip()]

                if site == "site1":
                    # Format: Date \t Views \t CPM \t Earning \t Ref
                    for line in lines:
                        parts = [p.strip().replace("$", "") for p in line.split("\t") if p.strip()]
                        if len(parts) < 4:
                            return jsonify({"status": "failed", "message": f"Invalid format for site1 line: {line}"}), 400
                        rec_date, views, cpm, earning = parts[0], int(parts[1]), float(parts[2]), float(parts[3])
                        upsert_record(rec_date, views, earning, cpm)

                elif site == "site2":
                    # Format: Date \t Views \t CPM \t Earning \t Ref
                    for line in lines:
                        parts = [p.strip().replace("$", "") for p in line.split("\t") if p.strip()]
                        if len(parts) < 4:
                            return jsonify({"status": "failed", "message": f"Invalid format for site2 line: {line}"}), 400
                        rec_date, views, earning, cpm = parts[0], int(parts[1]), float(parts[2]), float(parts[3])
                        upsert_record(rec_date, views, earning, cpm)

                elif site in ["site3", "site4"]:
                    # Format: Date \t Views \t Earning \t Ref
                    for line in lines:
                        parts = [p.strip().replace("$", "") for p in line.split("\t") if p.strip()]
                        if len(parts) < 4:
                            return jsonify({"status": "failed", "message": f"Invalid format for {site} line: {line}"}), 400
                        rec_date, views, earning, cpm = parts[0], int(parts[1]), float(parts[2]), float(parts[3])
                        upsert_record(rec_date, views, earning, 0.0)

                else:
                    return jsonify({"status": "failed", "message": "Unknown site"}), 400

        else:
            # ==========================
            # Single-value update
            # ==========================
            if not date:
                return jsonify({"status": "failed", "message": "Date is required for single metric update"}), 400

            # Find or create record
            existing = next((r for r in site_records if r.get("Date") == date), None)
            if not existing:
                existing = {"Date": date, "Views": 0, "Earning": 0.0, "CPM": 0.0}
                site_records.append(existing)

            try:
                if metric == "views":
                    existing["Views"] = int(value)
                elif metric == "earning":
                    existing["Earning"] = float(value)
                elif metric == "cpm":
                    existing["CPM"] = float(value)
            except ValueError:
                return jsonify({"status": "failed", "message": f"Invalid value for {metric}"}), 400

        # Save back to DB
        db["maindb"].update_one({"type": "shortlink_statics"}, {"$set": {site: site_records}})
        return jsonify({"status": "success", "message": f"{site.title()} data updated!"})

    except Exception as e:
        return jsonify({"status": "failed", "message": f"Error: {str(e)}"}), 500


@app.route('/update-farm-db', methods=['POST'])
def update_farm_db():
    try:
        time_period = request.form.get('time')  # alltime / today
        farm_select = request.form.get('farm')  # all / farm1..farm5
        value = request.form.get('value')

        main_data = db["maindb"].find_one({"type": "farmstat"})
        if not main_data:
            return jsonify({"status": "failed", "message": "Farm data not found"}), 404

        farms = ["farm1","farm2","farm3","farm4","farm5"]
        target_farms = farms if farm_select=="all" else [farm_select]

        update_data = {}
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for f in target_farms:
            if time_period=="today":
                update_data[f"views_td_{f}"] = f"{value} | 1"
                #update_data[f"lastres_{f}"] = now_str
            else:
                update_data[f"views_at_{f}"] = value
                #update_data[f"lastres_{f}"] = now_str  # update lastres for consistency

        db["maindb"].update_one({"type": "farmstat"}, {"$set": update_data})
        return jsonify({"status": "success", "message": "Farm data updated!"})

    except Exception as e:
        return jsonify({"status": "failed", "message": f"Error: {str(e)}"}), 500
    

@app.route('/update-shortlink-payments', methods=['POST'])
def update_shortlink_payments():
    try:
        data = request.json
        site = data.get('site')
        status = data.get('status')
        input_text = data.get('input', '').strip()

        # Validate inputs
        if not all([site, status, input_text]):
            return jsonify({
                "status": "failed", 
                "message": "Missing required fields"
            }), 400

        # Get existing payments document
        payments = db["maindb"].find_one({"type": "shortlink_payments"})
        if not payments:
            return jsonify({
                "status": "failed",
                "message": "Payments database not found"
            }), 404

        site_payments = payments.get(site, [])

        if status == "auto":
            # Handle bulk input
            updated_payments = []
            existing_ids = {p["id"]: i for i, p in enumerate(site_payments)}
            
            for line in input_text.splitlines():
                if not line.strip():
                    continue

                try:
                    if site == "site1":
                        # Space-separated format for site1
                        parts = line.strip().split()
                        if len(parts) < 8:  # Minimum required parts
                            continue
                        
                        payment_id = int(parts[0])
                        new_payment = {
                            "id": payment_id,
                            "date": parts[1],
                            "status": parts[2],
                            "amount": parts[3].replace("$", ""),
                            "w_method": parts[7],
                            "w_account": "P1133173015"
                        }
                    elif site == "site2":
                        parts = line.strip().split('\t')  # Use tab delimiter
                        if len(parts) < 8:  # Need at least 8 parts
                            continue
                            
                        payment_id = int(parts[0])
                        new_payment = {
                            "id": payment_id,
                            "date": parts[1],  # Keep original datetime format
                            "status": parts[2],
                            "amount": parts[3].replace("$", ""),
                            "w_method": parts[6],
                            "w_account": parts[7]
                        }
                    else:  # site3 and site4
                        parts = line.strip().split('\t')
                        if len(parts) < 8:
                            continue
                            
                        payment_id = int(parts[0])
                        new_payment = {
                            "id": payment_id,
                            "date": parts[1],
                            "status": parts[2],
                            "amount": parts[3].replace("$", ""),
                            "w_method": parts[6],
                            "w_account": parts[7]
                        }

                    # If ID exists, update the existing payment
                    if payment_id in existing_ids:
                        site_payments[existing_ids[payment_id]] = new_payment
                    else:
                        site_payments.append(new_payment)

                except (IndexError, ValueError) as e:
                    continue  # Skip invalid lines

            if len(site_payments) == 0:
                return jsonify({
                    "status": "failed",
                    "message": "No valid payments found in input"
                }), 400

        else:
            # Handle single ID update
            try:
                payment_id = int(input_text)
            except ValueError:
                return jsonify({
                    "status": "failed",
                    "message": "Invalid ID format"
                }), 400

            # Find and update payment status
            payment_found = False
            for payment in site_payments:
                if payment["id"] == payment_id:
                    payment["status"] = status
                    payment_found = True
                    break

            if not payment_found:
                return jsonify({
                    "status": "failed",
                    "message": f"Payment ID {payment_id} not found in {site}"
                }), 404

        # Update database
        db["maindb"].update_one(
            {"type": "shortlink_payments"},
            {"$set": {site: site_payments}}
        )

        return jsonify({
            "status": "success",
            "message": f"Successfully updated {site} payments"
        })

    except Exception as e:
        return jsonify({
            "status": "failed",
            "message": f"Error: {str(e)}"
        }), 500


@app.route('/clear-farm-activity', methods=['POST'])
def clear_farm_activity():
    try:
        db["maindb"].update_one({"type": "farm_activity"}, {"$set": {"records": []}})
        return jsonify({"status": "success", "message": "Farm activity cleared!"})
    except Exception as e:
        return jsonify({"status": "failed", "message": f"Error: {str(e)}"}), 500


@app.route("/database")
def database():
    return render_template("database.html")   # Database page

@app.route("/analytics")
def payments():
    return render_template("analytics.html")   # payment page

@app.route('/')
def index():
    stats, farmstats = fetch_stats_and_farm()
    return render_template('index.html', stats=stats, farmstats=farmstats)

@app.route('/get-shortlink-payments')
def get_shortlink_payments():
    doc = db["maindb"].find_one({"type": "shortlink_payments"}) or {}
    # Remove _id field if present
    if "_id" in doc:
        del doc["_id"]
    return jsonify(doc)

@app.route('/get-shortlink-stats')
def get_shortlink_stats():
    doc = db["maindb"].find_one({"type": "shortlink_statics"}) or {}
    # Remove _id field if present
    if "_id" in doc:
        del doc["_id"]
    return jsonify(doc)

@app.route('/update-dashboard', methods=['POST'])
def update_dashboard():
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ['earn_at_conf', 'earn_at', 'views_at', 'views_at_conf', 'cpm', 'lastconf_update']
        if not all(field in data for field in required_fields):
            return jsonify({
                "status": "failed",
                "message": "Missing required fields"
            }), 400
        
        # Update main document
        update_data = {
            "earn_at_conf": float(data['earn_at_conf']),
            "earn_at": float(data['earn_at']),
            "views_at": int(data['views_at']),
            "views_at_conf": int(data['views_at_conf']),
            "cpm": data['cpm'],
            "lastconf_update": data['lastconf_update']
        }
        
        result = db["maindb"].update_one(
            {"type": "main"},
            {"$set": update_data},
            upsert=True
        )
        
        return jsonify({
            "status": "success",
            "message": "Dashboard updated successfully"
        })
        
    except Exception as e:
        return jsonify({
            "status": "failed",
            "message": str(e)
        }), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')