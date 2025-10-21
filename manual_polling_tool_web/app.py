# app.py
import traceback
from flask import Flask, render_template, request, jsonify


# ייבוא כל הקליינטים מהקובץ הקיים שלך
from api_clients import (
    AdMobClient,
    AppLovinClient,
    ChartboostClient,
    FacebookClient,
    FyberClient,
    GamClient,
    HyprMXClient,
    InMobiClient
)

# --- התיקון כאן ---
# הוספנו הגדרה מפורשת לתיקיית static
app = Flask(__name__, static_folder='static', template_folder='templates')

# נתיב ראשי שמציג את דף ה-HTML
@app.route('/')
def index():
    return render_template('index.html')

# --- כל שאר קוד ה-API Endpoints נשאר זהה ---
# (הוסף כאן את כל ה-@app.route('/api/poll/...') מהקובץ הקודם)
@app.route('/api/poll/admob_nonsso', methods=['POST'])
def poll_admob_nonsso():
    try:
        params = request.json
        client = AdMobClient(
            client_id=params.get('client_id'),
            client_secret=params.get('client_secret'),
            refresh_token=params.get('refresh_token'),
            publisher_id=params.get('publisher_id')
        )
        data = client.get_report(
            start_date_str=params.get('start_date'),
            end_date_str=params.get('end_date'),
            selected_dimensions=params.get('dimensions', []),
            selected_metrics=params.get('metrics', [])
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route('/api/poll/admob_sso', methods=['POST'])
def poll_admob_sso():
    # Placeholder for future SSO implementation
    return jsonify({"message": "AdMob SSO endpoint is not yet implemented."}), 501

@app.route('/api/poll/gam', methods=['POST'])
def poll_gam():
    try:
        params = request.json
        client = GamClient(
            client_id=params.get('client_id'),
            client_secret=params.get('client_secret'),
            refresh_token=params.get('refresh_token'),
            network_code=params.get('network_code')
        )
        # GAM מחזיר Tuple, נהפוך אותו ל-JSON מסודר
        header, data = client.get_report(
            start_date_str=params.get('start_date'),
            end_date_str=params.get('end_date'),
            selected_dimensions=params.get('dimensions', []),
            selected_metrics=params.get('metrics', []),
            network_code=params.get('network_code')
        )
        return jsonify({"header": header, "data": data})
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route('/api/poll/applovin', methods=['POST'])
def poll_applovin():
    try:
        params = request.json
        client = AppLovinClient(api_key=params.get('api_key'))
        data = client.get_report(
            start_date_str=params.get('start_date'),
            end_date_str=params.get('end_date'),
            selected_dimensions=params.get('dimensions', []),
            selected_metrics=params.get('metrics', [])
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route('/api/poll/chartboost', methods=['POST'])
def poll_chartboost():
    try:
        params = request.json
        client = ChartboostClient(
            app_ids=params.get('app_ids'),
            user_id=params.get('user_id'),
            user_signature=params.get('user_signature')
        )
        data = client.get_report(
            start_date_str=params.get('start_date'),
            end_date_str=params.get('end_date'),
            selected_dimensions=params.get('dimensions', []),
            selected_metrics=params.get('metrics', []),
            ad_type_selection=params.get('ad_type')
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route('/api/poll/facebook', methods=['POST'])
def poll_facebook():
    try:
        params = request.json
        client = FacebookClient(
            app_id=params.get('app_id'),
            access_token=params.get('access_token')
        )
        data = client.get_report(
            start_date_str=params.get('start_date'),
            end_date_str=params.get('end_date'),
            selected_dimensions=params.get('dimensions', []),
            selected_metrics=params.get('metrics', [])
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route('/api/poll/fyber', methods=['POST'])
def poll_fyber():
    try:
        params = request.json
        client = FyberClient(
            consumer_key=params.get('consumer_key'),
            consumer_secret=params.get('consumer_secret'),
            publisher_id=params.get('publisher_id')
        )
        data = client.get_report(
            start_date_str=params.get('start_date'),
            end_date_str=params.get('end_date'),
            selected_dimensions=params.get('dimensions', []),
            selected_metrics=params.get('metrics', []),
            publisher_id=params.get('publisher_id')
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route('/api/poll/inmobi', methods=['POST'])
def poll_inmobi():
    try:
        params = request.json
        client = InMobiClient(
            username=params.get('username'),
            secret_key=params.get('secret_key')
        )
        data = client.get_report(
            start_date_str=params.get('start_date'),
            end_date_str=params.get('end_date'),
            selected_dimensions=params.get('dimensions', []),
            selected_metrics=params.get('metrics', []),
            filter_placement_ids=params.get('filter_placement_ids')
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

# --- 2. הוסף את ה-Endpoint החדש בסוף (לפני __main__) ---
@app.route('/api/poll/hyprmx', methods=['POST'])
def poll_hyprmx():
    try:
        params = request.json
        client = HyprMXClient(
            api_key=params.get('api_key')
        )
        data = client.get_report(
            start_date_str=params.get('start_date'),
            end_date_str=params.get('end_date'),
            app_id=params.get('app_id'),
            selected_dimensions=params.get('dimensions', [])
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

if __name__ == '__main__':
    # מריץ את השרת המקומי לצורכי פיתוח
    app.run(host='0.0.0.0', port=5001, debug=True)