# api_clients.py
import csv
import gzip
import hmac # For HMAC-SHA1
import hashlib # For SHA1
import base64 # For base64 encoding of signature
import io
import time # For timestamp
import uuid # For nonce
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from urllib.parse import urlencode, quote, parse_qs, urlparse # Added parse_qs, urlparse
import requests
import json
from datetime import datetime
import logging
from urllib.parse import urlencode
import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


# --- AdMobClient (No changes from previous version, included for completeness) ---
class AdMobClient:
    TOKEN_URL_BASE = "https://oauth2.googleapis.com/token"
    REPORT_BASE_URL = "https://admob.googleapis.com/v1/accounts/"

    ALL_DIMENSIONS = [
        "DATE", "AD_UNIT", "FORMAT", "COUNTRY", "APP", "PLATFORM", "PLACEMENT",
        "AD_SOURCE", "CREATIVE", "BID_TYPE"
    ]
    ALL_METRICS = [
        "ESTIMATED_EARNINGS", "IMPRESSIONS", "IMPRESSION_RPM", "CLICKS", "ACTIVE_VIEW_IMPRESSIONS",
        "MATCHED_REQUESTS", "SHOW_RATE", "CLICK_THROUGH_RATE", "AD_REQUESTS"
    ]

    def __init__(self, client_id, client_secret, refresh_token, publisher_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token.replace("\\", "")
        self.publisher_id = publisher_id
        self.access_token = None

    def _get_access_token(self):
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }

        token_url = f"{self.TOKEN_URL_BASE}?{urlencode(params)}"

        logging.info(f"AdMobClient: Requesting access token from {token_url}")

        try:
            response = requests.post(token_url, data={})
            response.raise_for_status()
            token_data = response.json()
            logging.info(f"AdMobClient: Access token response received. Status: 200")
            logging.debug(f"AdMobClient: Token response data: {token_data}")

            if "access_token" in token_data:
                self.access_token = token_data["access_token"]
                return self.access_token
            else:
                raise ValueError(f"Access token not found in response. Response: {token_data}")
        except requests.exceptions.RequestException as e:
            error_message = f"Error fetching access token: {e}"
            if response is not None:
                error_message += f". Status Code: {response.status_code}. Response Text: {response.text}"
            logging.error(error_message)
            raise ConnectionError(error_message)
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON response from token endpoint: {response.text}")
            raise ValueError(f"Invalid JSON response from token endpoint: {response.text}")

    def get_report(self, start_date_str, end_date_str, selected_dimensions, selected_metrics):
        if not self.access_token:
            logging.info("AdMobClient: No existing access token, fetching a new one.")
            self._get_access_token()

        report_url = f"{self.REPORT_BASE_URL}{self.publisher_id}/networkReport:generate"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")

        payload = {
            "reportSpec": {
                "dateRange": {
                    "startDate": {
                        "year": start_date_obj.year,
                        "month": start_date_obj.month,
                        "day": start_date_obj.day
                    },
                    "endDate": {
                        "year": end_date_obj.year,
                        "month": end_date_obj.month,
                        "day": end_date_obj.day
                    }
                },
                "dimensions": selected_dimensions,
                "metrics": selected_metrics,
                "localizationSettings": {
                    "currencyCode": "USD"
                }
            }
        }

        logging.info(f"AdMobClient: Requesting report from {report_url}")
        logging.debug(f"AdMobClient: Report request headers: {headers}")
        logging.debug(f"AdMobClient: Report request payload: {json.dumps(payload, indent=2)}")

        try:
            response = requests.post(report_url, headers=headers, json=payload)
            response.raise_for_status()
            report_data_raw = response.json()
            logging.info(f"AdMobClient: Report response received. Status: {response.status_code}")
            logging.debug(f"AdMobClient: Full Raw Report response data: {json.dumps(report_data_raw, indent=2)}")

            processed_data = []

            if not isinstance(report_data_raw, list):
                logging.error(
                    f"AdMobClient: Unexpected top-level report_data format. Expected a list. Type: {type(report_data_raw)}. Data: {json.dumps(report_data_raw, indent=2)}")
                return []

            for item in report_data_raw:
                if "row" in item and isinstance(item["row"], dict):
                    row_content = item["row"]
                elif "header" in item or "footer" in item:
                    logging.debug(f"AdMobClient: Skipping header/footer item: {json.dumps(item, indent=2)}")
                    continue
                else:
                    logging.warning(
                        f"AdMobClient: Unexpected item format in report data. Assuming direct row content. Item: {json.dumps(item, indent=2)}")
                    row_content = item

                dimension_values = row_content.get("dimensionValues", {})
                metric_values = row_content.get("metricValues", {})

                if not dimension_values and not metric_values:
                    logging.debug(
                        f"AdMobClient: Skipping row with no dimension or metric values: {json.dumps(row_content, indent=2)}")
                    continue

                row_output = {}

                for dim_key in selected_dimensions:
                    if dim_key in dimension_values:
                        dim_data = dimension_values[dim_key]
                        if dim_key == "DATE":
                            row_output["Date"] = dim_data.get("value", "N/A")
                        elif dim_key == "AD_UNIT":
                            row_output["Ad Unit Name"] = dim_data.get("displayLabel", "N/A")
                            row_output["Ad Unit ID"] = dim_data.get("value", "N/A")
                        elif dim_key == "APP":
                            row_output["App Name"] = dim_data.get("displayLabel", "N/A")
                            row_output["App ID"] = dim_data.get("value", "N/A")
                        elif dim_key == "FORMAT":
                            row_output["Format"] = dim_data.get("value", "N/A")
                        elif dim_key == "COUNTRY":
                            row_output["Country"] = dim_data.get("value", "N/A")
                        else:
                            row_output[dim_key.replace('_', ' ').title()] = dim_data.get("displayLabel",
                                                                                         dim_data.get("value", "N/A"))
                    else:
                        logging.warning(
                            f"AdMobClient: Requested dimension '{dim_key}' not found in row: {json.dumps(row_content, indent=2)}")

                for met_key in selected_metrics:
                    if met_key in metric_values:
                        met_data = metric_values[met_key]
                        if met_key == "ESTIMATED_EARNINGS":
                            row_output["Estimated Earnings"] = float(met_data.get("microsValue", "0")) / 1_000_000
                        elif met_key == "IMPRESSIONS":
                            row_output["Impressions"] = int(met_data.get("integerValue", "0"))
                        elif met_key == "IMPRESSION_RPM":
                            row_output["Impression RPM"] = float(met_data.get("doubleValue", 0.0))
                        elif "value" in met_data:
                            try:
                                row_output[met_key.replace('_', ' ').title()] = float(met_data["value"])
                            except ValueError:
                                row_output[met_key.replace('_', ' ').title()] = met_data["value"]
                        elif "doubleValue" in met_data:
                            row_output[met_key.replace('_', ' ').title()] = float(met_data["doubleValue"])
                        elif "integerValue" in met_data:
                            row_output[met_key.replace('_', ' ').title()] = int(met_data["integerValue"])
                        else:
                            row_output[met_key.replace('_', ' ').title()] = "N/A"
                    else:
                        logging.warning(
                            f"AdMobClient: Requested metric '{met_key}' not found in row: {json.dumps(row_content, indent=2)}")

                if any(isinstance(v, (int, float)) and v != 0 for k, v in row_output.items() if
                       k in ["Estimated Earnings", "Impressions", "Impression RPM"]) or \
                        any(v != "N/A" for k, v in row_output.items() if
                            k not in ["Estimated Earnings", "Impressions", "Impression RPM"]):
                    processed_data.append(row_output)
                else:
                    logging.debug(
                        f"AdMobClient: Skipping row with all N/A dimensions and zero metrics: {json.dumps(row_content, indent=2)}")

            logging.info(f"AdMobClient: Processed {len(processed_data)} rows.")
            return processed_data

        except requests.exceptions.RequestException as e:
            error_message = f"Error fetching AdMob report: {e}"
            if response is not None:
                error_json = None
                try:
                    error_json = response.json()
                except json.JSONDecodeError:
                    pass

                if response.status_code == 400:
                    error_detail = error_json.get('error', {}).get('message',
                                                                   'Unknown bad request error') if error_json else response.text
                    error_message = f"AdMob API Error (400 - Bad Request): {error_detail}"
                    raise ValueError(error_message)
                elif response.status_code == 401:
                    error_detail = error_json.get('error', {}).get('message',
                                                                   'Unknown authentication error') if error_json else response.text
                    error_message = f"AdMob API Error (401 - Unauthorized): Check your token/credentials. {error_detail}"
                    raise ConnectionRefusedError(error_message)
                elif response.status_code == 403:
                    error_detail = error_json.get('error', {}).get('message',
                                                                   'Unknown permissions error') if error_json else response.text
                    error_message = f"AdMob API Error (403 - Forbidden): Insufficient permissions or wrong publisher ID. {error_detail}"
                    raise PermissionError(error_message)
                else:
                    error_message += f". Status Code: {response.status_code}. Response Text: {response.text}"
            logging.error(error_message)
            raise ConnectionError(error_message)
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON response from report endpoint: {response.text}")
            raise ValueError(f"Invalid JSON response from report endpoint: {response.text}")
        except Exception as e:
            logging.error(
                f"AdMobClient: Critical error during report parsing: {e}. Raw response type: {type(report_data_raw)}. Raw response: {json.dumps(report_data_raw, indent=2) if isinstance(report_data_raw, (dict, list)) else report_data_raw}")
            raise ValueError(f"Failed to parse AdMob report data: {e}")

# api_clients.py

import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# --- AppLovinClient Class (UPDATED with final output key mapping) ---
class AppLovinClient:
    BASE_URL = "https://r.applovin.com/report"

    # Define all possible dimensions and metrics explicitly for AppLovin.
    # These names will also be used as keys in the final JSON output.
    ALL_DIMENSIONS = [
        "day", "application", "country", "platform", "package_name", "size", "zone_id", "ad_type"
    ]
    ALL_METRICS = [
        "revenue", "views", "impressions", "clicks"
    ]

    def __init__(self, api_key):
        self.api_key = api_key

    def get_report(self, start_date_str, end_date_str, selected_dimensions, selected_metrics):
        requested_columns = set()
        for dim in selected_dimensions:
            # For AppLovin, the API's column names are generally what we want in the output.
            # We just need to ensure they are added to the set.
            # For "DATE", "AD_UNIT", "APP", "FORMAT" from a higher-level abstract UI,
            # we map them to AppLovin's specific column names ("day", "zone_id", "application", "size").
            # This is primarily for consistency if `selected_dimensions` comes from a general list.
            if dim == "DATE":
                requested_columns.add("day")
            elif dim == "AD_UNIT":
                requested_columns.add("zone_id")
            elif dim == "APP":
                requested_columns.add("application")
                requested_columns.add("package_name")  # Ensure package_name is also requested
            elif dim == "FORMAT":
                requested_columns.add("size")
            else:
                requested_columns.add(dim.lower())  # Add as is, assuming it's a direct AppLovin dim

        for metric in selected_metrics:
            if metric == "ESTIMATED_EARNINGS":
                requested_columns.add("revenue")  # Map to AppLovin's 'revenue'
            elif metric == "IMPRESSIONS":
                requested_columns.add("impressions")
            elif metric == "IMPRESSION_RPM":
                logging.warning("AppLovin does not provide 'IMPRESSION_RPM' directly. This metric will be 'N/A'.")
                # We won't add 'impression_rpm' to requested_columns as AppLovin won't return it
            elif metric == "CLICKS":
                requested_columns.add("clicks")
            else:
                requested_columns.add(metric.lower())  # Add as is, assuming it's a direct AppLovin metric

        columns_param = ",".join(list(requested_columns))

        if not columns_param:
            columns_param = "day,application,revenue,impressions,country"
            logging.warning("No dimensions or metrics selected for AppLovin. Defaulting to basic columns.")

        params = {
            "api_key": self.api_key,
            "start": start_date_str,
            "end": end_date_str,
            "columns": columns_param,
            "format": "json",
            "report_type": "publisher"
        }

        logging.info(f"AppLovinClient: Requesting report from {self.BASE_URL}")
        logging.debug(f"AppLovinClient: Report request parameters: {params}")

        response = None
        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()
            report_data = response.json()

            logging.info(f"AppLovinClient: Report response received. Status: {response.status_code}")
            logging.debug(f"AppLovinClient: Full Report response data: {json.dumps(report_data, indent=2)}")

            processed_data = []

            rows_to_process = []
            if isinstance(report_data, list):
                rows_to_process = report_data
            elif isinstance(report_data, dict) and "results" in report_data and isinstance(report_data["results"],
                                                                                           list):
                logging.warning("AppLovinClient: Report data wrapped in 'results' key. Extracting.")
                rows_to_process = report_data["results"]
            else:
                logging.error(
                    f"AppLovinClient: Unexpected report data format. Expected a list of dicts or dict with 'results' key. Type: {type(report_data)}. Data: {json.dumps(report_data, indent=2)}")
                return []

            for row_data in rows_to_process:
                if not isinstance(row_data, dict):
                    logging.warning(f"AppLovinClient: Skipping non-dict item in response list: {row_data}")
                    continue

                row_output = {}
                for col_name in requested_columns:  # Iterate through the columns actually requested
                    raw_value = row_data.get(col_name, "N/A")

                    # Directly use AppLovin's column names as output keys
                    # Apply type conversions as needed
                    if col_name == "revenue":
                        row_output["revenue"] = float(raw_value) if isinstance(raw_value, (
                            int, float, str)) and str(raw_value).replace('.', '', 1).isdigit() else 0.0
                    elif col_name == "impressions":
                        row_output["impressions"] = int(raw_value) if isinstance(raw_value, (int, str)) and str(
                            raw_value).isdigit() else 0
                    elif col_name == "clicks":
                        row_output["clicks"] = int(raw_value) if isinstance(raw_value, (int, str)) and str(
                            raw_value).isdigit() else 0
                    elif col_name == "views":
                        row_output["views"] = int(raw_value) if isinstance(raw_value, (int, str)) and str(
                            raw_value).isdigit() else 0
                    else:
                        # For other dimensions (day, application, country, platform, package_name, size, zone_id, ad_type)
                        # just assign the raw value directly.
                        row_output[col_name] = raw_value

                # Only add Impression RPM if it was explicitly selected, but its value will be N/A
                if "IMPRESSION_RPM" in selected_metrics:
                    row_output["IMPRESSION_RPM"] = "N/A"  # AppLovin doesn't provide this directly

                # Condition for filtering rows (checking for any meaningful data)
                if any(isinstance(v, (int, float)) and v != 0 for k, v in row_output.items() if
                       k in ["revenue", "impressions", "clicks", "views"]) or \
                        any(v != "N/A" for k, v in row_output.items() if
                            k not in ["revenue", "impressions", "clicks", "views", "IMPRESSION_RPM"]):
                    processed_data.append(row_output)
                else:
                    logging.debug(
                        f"AppLovinClient: Skipping row with all N/A dimensions and zero metrics: {json.dumps(row_data, indent=2)}")

            logging.info(f"AppLovinClient: Processed {len(processed_data)} rows.")
            return processed_data

        except requests.exceptions.RequestException as e:
            error_message = f"Error fetching AppLovin report: {e}"
            if response is not None:
                error_json = None
                try:
                    error_json = response.json()
                except json.JSONDecodeError:
                    pass

                if response.status_code == 400:
                    error_detail = error_json.get('error', {}).get('message',
                                                                   'Unknown bad request error') if error_json else response.text
                    error_message = f"AppLovin API Error (400 - Bad Request): {error_detail}"
                    raise ValueError(error_message)
                elif response.status_code == 401 or response.status_code == 403:
                    error_detail = error_json.get('message', 'Check your API Key.') if error_json else response.text
                    raise ConnectionRefusedError(f"AppLovin API Error ({response.status_code}): {error_detail}")
                else:
                    error_message += f". Status Code: {response.status_code}. Response Text: {response.text}"
            logging.error(error_message)
            raise ConnectionError(error_message)
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON response from report endpoint: {response.text}")
            raise ValueError(f"Invalid JSON response from report endpoint: {response.text}")
        except Exception as e:
            logging.error(
                f"AppLovinClient: Critical error during report parsing: {e}. Raw response type: {type(report_data)}. Raw response: {json.dumps(report_data, indent=2) if isinstance(report_data, (dict, list)) else report_data}")
            raise ValueError(f"Failed to parse AppLovin report data: {e}")


class ChartboostClient:
    BASE_URL = "https://analytics.chartboost.com/v3/metrics/appcountry"

    # These lists define the MAPPING from Chartboost response keys to our desired output keys.
    # They also define the options presented in the UI.
    # We map Chartboost's internal names to a consistent output name.
    # For example, "dt" from Chartboost becomes "day" in our output.
    DIMENSION_MAPPING = {
        "dt": "day",
        "appId": "app_id",
        "app": "app_name",
        "countryCode": "country_code",
        "platform": "platform",
        "adLocation": "ad_location",
        "adType": "ad_type",
        "campaignType": "campaign_type"
    }

    METRIC_MAPPING = {
        "moneyEarned": "revenue",
        "impressionsDelivered": "impressions",
        "clicksDelivered": "clicks",
        "installsDelivered": "installs",
        "ecpmEarned": "ecpm",
        "cpcvEarned": "cpcv",
        "ctrDelivered": "ctr",
        "installRateDelivered": "install_rate",
        "videoCompletedDelivered": "video_completed"
    }

    # These are for UI display and internal logic.
    # ALL_DIMENSIONS & ALL_METRICS now use the *desired output keys*.
    ALL_DIMENSIONS = list(DIMENSION_MAPPING.values())
    ALL_METRICS = list(METRIC_MAPPING.values())

    AD_TYPE_IDS_MAP = {
        "rewarded_video": "4",
        "interstitial": "1",
        "all": "5",
        "rv_is_all": "4,1,5"
    }

    def __init__(self, app_ids, user_id, user_signature):
        self.app_ids = app_ids
        self.user_id = user_id
        self.user_signature = user_signature

    def get_report(self, start_date_str, end_date_str, selected_dimensions, selected_metrics, ad_type_selection):
        app_ids_param = self.app_ids if isinstance(self.app_ids, str) else ",".join(map(str, self.app_ids))

        ad_type_id_param = ""
        if ad_type_selection == "all":
            ad_type_id_param = self.AD_TYPE_IDS_MAP["all"]
        elif ad_type_selection == "rewarded_video":
            ad_type_id_param = self.AD_TYPE_IDS_MAP["rewarded_video"]
        elif ad_type_selection == "interstitial":
            ad_type_id_param = self.AD_TYPE_IDS_MAP["interstitial"]
        elif ad_type_selection == "rv_is_all":
            ad_type_id_param = self.AD_TYPE_IDS_MAP["rv_is_all"]
        else:
            ad_type_id_param = self.AD_TYPE_IDS_MAP["all"]
            logging.warning(
                f"ChartboostClient: Unexpected ad_type_selection '{ad_type_selection}'. Defaulting to 'all'.")

        params = {
            "appIds": app_ids_param,
            "userId": self.user_id,
            "userSignature": self.user_signature,
            "dateMin": start_date_str,
            "dateMax": end_date_str,
            "adLocation": "all",
            "campaignType": "network,direct_deal",
            "adTypeIds": ad_type_id_param,
        }

        logging.info(f"ChartboostClient: Requesting report from {self.BASE_URL}")
        logging.debug(f"ChartboostClient: Report request parameters: {params}")

        response = None
        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()
            report_data = response.json()

            logging.info(f"ChartboostClient: Report response received. Status: {response.status_code}")
            logging.debug(f"ChartboostClient: Full Report response data: {json.dumps(report_data, indent=2)}")

            processed_data = []

            if not isinstance(report_data, list):
                if isinstance(report_data, dict) and "error" in report_data:
                    raise ValueError(f"Chartboost API Error: {report_data.get('message', json.dumps(report_data))}")
                logging.error(
                    f"ChartboostClient: Unexpected top-level report_data format. Expected a list. Type: {type(report_data)}. Data: {json.dumps(report_data, indent=2)}")
                return []

            # Reverse mappings for easy lookup: from desired output name to Chartboost response key
            reverse_dim_map = {v: k for k, v in self.DIMENSION_MAPPING.items()}
            reverse_metric_map = {v: k for k, v in self.METRIC_MAPPING.items()}

            for row_data in report_data:
                if not isinstance(row_data, dict):
                    logging.warning(f"ChartboostClient: Skipping non-dict item in response list: {row_data}")
                    continue

                temp_row_output = {}  # First populate with all mapped data

                # Process all dimensions and metrics based on mappings
                for chartboost_key, output_key in self.DIMENSION_MAPPING.items():
                    if chartboost_key in row_data:
                        temp_row_output[output_key] = row_data[chartboost_key]

                for chartboost_key, output_key in self.METRIC_MAPPING.items():
                    if chartboost_key in row_data:
                        value = row_data[chartboost_key]
                        try:
                            # Attempt conversion for numeric metrics
                            if output_key in ["impressions", "clicks", "installs", "video_completed"]:
                                temp_row_output[output_key] = int(
                                    float(value))  # Convert to float first, then int for safety
                            else:  # For revenue, ecpm, cpcv, ctr, install_rate
                                temp_row_output[output_key] = float(value)
                        except (ValueError, TypeError):
                            temp_row_output[output_key] = 0.0  # Default numeric to 0 on failure
                            logging.warning(
                                f"ChartboostClient: Failed to convert '{output_key}' value '{value}' from '{chartboost_key}' to number. Setting to 0.")
                    else:
                        temp_row_output[output_key] = 0.0  # Default missing numeric metrics to 0

                # Now, filter temp_row_output based on selected_dimensions and selected_metrics
                final_row_output = {}
                for dim_key in selected_dimensions:
                    if dim_key in temp_row_output:
                        final_row_output[dim_key] = temp_row_output[dim_key]
                    else:
                        final_row_output[dim_key] = "N/A"  # If selected but not found/mapped

                for metric_key in selected_metrics:
                    if metric_key in temp_row_output:
                        final_row_output[metric_key] = temp_row_output[metric_key]
                    else:
                        final_row_output[metric_key] = 0.0  # If selected but not found/mapped, default to 0 for numeric

                # Filter condition (checking for any meaningful data in the FINAL output)
                if any(isinstance(v, (int, float)) and v != 0 for k, v in final_row_output.items() if
                       k in self.ALL_METRICS) or \
                        any(v not in ["N/A", "", None] for k, v in final_row_output.items() if
                            k in self.ALL_DIMENSIONS):
                    processed_data.append(final_row_output)
                else:
                    logging.debug(
                        f"ChartboostClient: Skipping row with all N/A dimensions and zero metrics after filtering: {json.dumps(final_row_output, indent=2)}")

            logging.info(f"ChartboostClient: Processed {len(processed_data)} rows.")
            return processed_data

        except requests.exceptions.RequestException as e:
            error_message = f"Error fetching Chartboost report: {e}"
            if response is not None:
                error_json = None
                try:
                    error_json = response.json()
                except json.JSONDecodeError:
                    pass

                if response.status_code == 400:
                    error_detail = error_json.get('error', {}).get('message',
                                                                   'Unknown bad request error') if error_json else response.text
                    error_message = f"Chartboost API Error (400 - Bad Request): {error_detail}"
                    raise ValueError(error_message)
                elif response.status_code == 401 or response.status_code == 403:
                    error_detail = error_json.get('message',
                                                  'Check your credentials (userId, userSignature, appIds).') if error_json else response.text
                    raise ConnectionRefusedError(f"Chartboost API Error ({response.status_code}): {error_detail}")
                else:
                    error_message += f". Status Code: {response.status_code}. Response Text: {response.text}"
            logging.error(error_message)
            raise ConnectionError(error_message)
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON response from report endpoint: {response.text}")
            raise ValueError(f"Invalid JSON response from report endpoint: {response.text}")
        except Exception as e:
            logging.error(
                f"ChartboostClient: Critical error during report parsing: {e}. Raw response type: {type(report_data)}. Raw response: {json.dumps(report_data, indent=2) if isinstance(report_data, (dict, list)) else report_data}")
            raise ValueError(f"Failed to parse Chartboost report data: {e}")


class FacebookClient:
    API_VERSION = "v13.0"
    BASE_URL = f"https://graph.facebook.com/{API_VERSION}/"

    # Mapping Facebook's API keys to our desired output keys
    DIMENSION_MAPPING = {
        "placement": "placement",
        "country": "country"
        # Add other dimensions if they are returned and relevant
    }

    METRIC_MAPPING = {
        "fb_ad_network_revenue": "revenue"
        # Add other metrics if they are returned
    }

    # These are for UI display and internal logic, using the desired output keys.
    ALL_DIMENSIONS = list(DIMENSION_MAPPING.values()) + ["day"]  # 'day' is derived from 'time'
    ALL_METRICS = list(METRIC_MAPPING.values())

    def __init__(self, app_id, access_token):
        self.app_id = app_id
        self.access_token = access_token.strip()

    def get_report(self, start_date_str, end_date_str, selected_dimensions, selected_metrics):
        report_url_base = f"{self.BASE_URL}{self.app_id}/adnetworkanalytics/"

        fb_metrics_internal = [fb_key for fb_key, output_key in self.METRIC_MAPPING.items() if
                               output_key in selected_metrics]
        fb_breakdowns_internal = [fb_key for fb_key, output_key in self.DIMENSION_MAPPING.items() if
                                  output_key in selected_dimensions]

        if not fb_metrics_internal:
            fb_metrics_internal = ["fb_ad_network_revenue"]
            logging.warning("FacebookClient: No metrics selected. Defaulting to 'fb_ad_network_revenue'.")

        metrics_query_part = f"metrics=[{','.join([f'%27{m}%27' for m in fb_metrics_internal])}]"

        breakdowns_query_parts = []
        for i, b in enumerate(fb_breakdowns_internal):
            breakdowns_query_parts.append(f"breakdowns[{i}]={b}")
        breakdowns_query_string = "&".join(breakdowns_query_parts)

        query_parts = [
            metrics_query_part,
            breakdowns_query_string,
            f"since={start_date_str}",
            f"until={end_date_str}",
            f"access_token={self.access_token}",
            "ordering_column=value"
        ]

        final_query_string = "&".join(query_parts)
        report_url = f"{report_url_base}?{final_query_string}"

        logging.info(f"FacebookClient: Requesting report from {report_url}")
        logging.debug(f"FacebookClient: Full Report request URL: {report_url}")

        response = None
        try:
            response = requests.get(report_url)
            response.raise_for_status()
            report_raw_data = response.json()

            logging.info(f"FacebookClient: Report response received. Status: {response.status_code}")
            logging.debug(f"FacebookClient: Full Report raw data: {json.dumps(report_raw_data, indent=2)}")

            processed_data = []

            # Navigate the nested structure: 'data' -> list[dict{'results': list[dict]}]
            if "data" not in report_raw_data or not isinstance(report_raw_data["data"], list):
                if isinstance(report_raw_data, dict) and "error" in report_raw_data:
                    error_info = report_raw_data["error"]
                    raise ValueError(
                        f"Facebook API Error ({error_info.get('code', 'N/A')}): {error_info.get('message', 'Unknown error')}")
                logging.error(
                    f"FacebookClient: Unexpected top-level report_data format. Expected dict with 'data' list. Type: {type(report_raw_data)}. Data: {json.dumps(report_raw_data, indent=2)}")
                return []

            # Loop through the list under 'data' (each item has a 'results' key)
            for data_item in report_raw_data["data"]:
                if "results" not in data_item or not isinstance(data_item["results"], list):
                    logging.warning(
                        f"FacebookClient: Skipping data_item without 'results' list: {json.dumps(data_item, indent=2)}")
                    continue

                # Now, report_rows is the actual list of data entries
                report_rows = data_item["results"]

                for row_data in report_rows:
                    if not isinstance(row_data, dict):
                        logging.warning(f"FacebookClient: Skipping non-dict item in results list: {row_data}")
                        continue

                    temp_row_output = {}

                    # Extract 'time' and map to 'day'
                    if "time" in row_data:
                        # 'time' format: "YYYY-MM-DDTHH:MM:SS+0000", extract only the date part
                        temp_row_output["day"] = row_data["time"].split('T')[0]

                    # Extract 'value' (main metric, e.g., revenue)
                    if "value" in row_data:
                        try:
                            temp_row_output["revenue"] = float(row_data["value"])
                        except (ValueError, TypeError):
                            temp_row_output["revenue"] = 0.0
                            logging.warning(
                                f"FacebookClient: Failed to convert revenue value '{row_data['value']}' to float. Setting to 0.")
                    else:
                        temp_row_output["revenue"] = 0.0

                    # Process 'breakdowns' (dimensions)
                    if "breakdowns" in row_data and isinstance(row_data["breakdowns"], list):
                        for breakdown_entry in row_data["breakdowns"]:
                            if "key" in breakdown_entry and "value" in breakdown_entry:
                                # Map Facebook's breakdown 'key' to our output key
                                output_key = self.DIMENSION_MAPPING.get(breakdown_entry["key"], breakdown_entry["key"])
                                temp_row_output[output_key] = breakdown_entry["value"]

                    # Filter temp_row_output based on selected_dimensions and selected_metrics
                    final_row_output = {}
                    # Ensure 'day' is considered a dimension for filtering output
                    all_selected_output_keys = selected_dimensions + selected_metrics
                    if "day" not in all_selected_output_keys and "day" in self.ALL_DIMENSIONS:
                        all_selected_output_keys.append(
                            "day")  # Always include 'day' if it's available and not explicitly excluded

                    for output_key in all_selected_output_keys:
                        if output_key in temp_row_output:
                            final_row_output[output_key] = temp_row_output[output_key]
                        else:
                            # Default values for selected but not found keys
                            if output_key in self.ALL_METRICS:
                                final_row_output[output_key] = 0.0
                            elif output_key in self.ALL_DIMENSIONS:
                                final_row_output[output_key] = "N/A"

                    if any(isinstance(v, (int, float)) and v != 0 for k, v in final_row_output.items() if
                           k in self.ALL_METRICS) or \
                            any(v not in ["N/A", "", None] for k, v in final_row_output.items() if
                                k in self.ALL_DIMENSIONS):
                        processed_data.append(final_row_output)
                    else:
                        logging.debug(
                            f"FacebookClient: Skipping row with all N/A dimensions and zero metrics after filtering: {json.dumps(final_row_output, indent=2)}")

            logging.info(f"FacebookClient: Processed {len(processed_data)} rows.")
            return processed_data

        except requests.exceptions.RequestException as e:
            error_message = f"Error fetching Facebook report: {e}"
            if response is not None:
                error_json = None
                try:
                    error_json = response.json()
                except json.JSONDecodeError:
                    pass

                if response.status_code == 400:
                    error_detail = error_json.get('error', {}).get('message',
                                                                   'Unknown bad request error') if error_json else response.text
                    error_message = f"Facebook API Error (400 - Bad Request): {error_detail}"
                    raise ValueError(error_message)
                elif response.status_code == 401 or response.status_code == 403:
                    error_detail = error_json.get('error', {}).get('message',
                                                                   'Check your access token or app ID permissions.') if error_json else response.text
                    raise ConnectionRefusedError(f"Facebook API Error ({response.status_code}): {error_detail}")
                elif response.status_code == 404:
                    error_detail = error_json.get('error', {}).get('message',
                                                                   'Endpoint not found or invalid App ID.') if error_json else response.text
                    error_message = f"Facebook API Error (404 - Not Found): {error_detail}"
                    raise ValueError(error_message)
                else:
                    error_message += f". Status Code: {response.status_code}. Response Text: {response.text}"
            logging.error(error_message)
            raise ConnectionError(error_message)
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON response from report endpoint: {response.text}")
            raise ValueError(f"Invalid JSON response from report endpoint: {response.text}")
        except Exception as e:
            logging.error(
                f"FacebookClient: Critical error during report parsing: {e}. Raw response type: {type(report_raw_data)}. Raw response: {json.dumps(report_raw_data, indent=2) if isinstance(report_raw_data, (dict, list)) else report_raw_data}")
            raise ValueError(f"Failed to parse Facebook report data: {e}")


class FyberClient:
    BASE_URL_TEMPLATE = "https://revenuedesk.fyber.com/iamp/services/performance/{publisher_id}/vamp/{start_epoch}/{end_epoch}"

    DIMENSION_MAPPING = {
        "date": "day", # 'date' from response is epoch, we convert to 'day' string
        "country": "country_code",
        # Removed "appId": "app_id", and "spotId": "spot_id" from DIMENSION_MAPPING here
        # because these are handled explicitly from parent levels and shouldn't be
        # mapped from 'unit_data' which contains empty strings for these keys.
        "contentId": "content_id",
        "contentName": "content_name",
        "applicationName": "app_name",
        "distributorName": "distributor_name",
        "contentCategories": "content_categories"
    }

    METRIC_MAPPING = {
        "adRequests": "ad_requests",
        "impressions": "impressions",
        "fillRate": "fill_rate",
        "clicks": "clicks",
        "ctr": "ctr",
        "ecpm": "ecpm",
        "revenue": "revenue"
    }

    # These are for UI display and internal logic, using the desired output keys.
    # IMPORTANT: Manually add 'app_id' and 'spot_id' here as they come from parent levels, not directly from 'unit_data'
    ALL_DIMENSIONS = ["app_id", "spot_id"] + list(DIMENSION_MAPPING.values())
    ALL_METRICS = list(METRIC_MAPPING.values())

    def __init__(self, consumer_key, consumer_secret, publisher_id):
        self.consumer_key = consumer_key.strip()
        self.consumer_secret = consumer_secret.strip()
        self.publisher_id = publisher_id

    def _generate_oauth_signature(self, http_method, base_url, params, consumer_secret):
        all_params = params.copy()
        all_params.update({
            "oauth_consumer_key": self.consumer_key,
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": str(int(time.time())),
            "oauth_nonce": str(uuid.uuid4()),
            "oauth_version": "1.0"
        })

        param_pairs = []
        for key, value in all_params.items():
            if isinstance(value, list):
                for v in value:
                    param_pairs.append((quote(str(key), safe=''), quote(str(v), safe='')))
            else:
                param_pairs.append((quote(str(key), safe=''), quote(str(value), safe='')))

        param_pairs.sort()

        normalized_params = "&".join([f"{key}={value}" for key, value in param_pairs])

        parsed_url = urlparse(base_url)
        oauth_base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

        signature_base_string = "&".join([
            quote(http_method.upper(), safe=''),
            quote(oauth_base_url, safe=''),
            quote(normalized_params, safe='')
        ])

        signing_key = f"{quote(consumer_secret, safe='')}&"

        hashed = hmac.new(signing_key.encode('utf-8'),
                          signature_base_string.encode('utf-8'),
                          hashlib.sha1)

        signature = base64.b64encode(hashed.digest()).decode('utf-8')

        return {
            "oauth_consumer_key": all_params["oauth_consumer_key"],
            "oauth_signature_method": all_params["oauth_signature_method"],
            "oauth_timestamp": all_params["oauth_timestamp"],
            "oauth_nonce": all_params["oauth_nonce"],
            "oauth_version": all_params["oauth_version"],
            "oauth_signature": signature
        }

    def get_report(self, start_date_str, end_date_str, selected_dimensions, selected_metrics, publisher_id):
        start_epoch = int(datetime.strptime(start_date_str, "%Y-%m-%d").timestamp())
        end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
        end_epoch = int(end_date_obj.timestamp())

        report_url_path = self.BASE_URL_TEMPLATE.format(
            publisher_id=publisher_id,
            start_epoch=start_epoch,
            end_epoch=end_epoch
        )

        oauth_params_to_sign = {}

        oauth_query_params = self._generate_oauth_signature(
            http_method="GET",
            base_url=report_url_path,
            params=oauth_params_to_sign,
            consumer_secret=self.consumer_secret
        )

        final_report_url = f"{report_url_path}?{urlencode(oauth_query_params)}"

        logging.info(f"FyberClient: Requesting report from {final_report_url}")
        logging.debug(f"FyberClient: Full Report request URL: {final_report_url}")

        response = None
        try:
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
            response = requests.get(final_report_url, headers=headers)
            response.raise_for_status()
            report_raw_data = response.json()

            logging.info(f"FyberClient: Report response received. Status: {response.status_code}")
            logging.debug(f"FyberClient: Full Report raw data: {json.dumps(report_raw_data, indent=2)}")

            processed_data = []

            if "apps" not in report_raw_data or not isinstance(report_raw_data["apps"], list):
                if isinstance(report_raw_data, dict) and "error" in report_raw_data:
                    error_info = report_raw_data.get('error', {})
                    raise ValueError(f"Fyber API Error: {report_raw_data.get('message', error_info.get('message', json.dumps(report_raw_data)))}")
                logging.error(
                    f"FyberClient: Unexpected top-level report_data format. Expected dict with 'apps' list. Type: {type(report_raw_data)}. Data: {json.dumps(report_raw_data, indent=2)}")
                return []

            for app_entry in report_raw_data["apps"]:
                current_app_id = app_entry.get("appId") # Use a temporary variable name
                for spot_entry in app_entry.get("spots", []):
                    current_spot_id = spot_entry.get("spotId") # Use a temporary variable name
                    for unit_data in spot_entry.get("units", []):
                        if not isinstance(unit_data, dict):
                            logging.warning(f"FyberClient: Skipping non-dict item in units list: {unit_data}")
                            continue

                        temp_row_output = {}
                        # Explicitly add parent IDs *first* to ensure they are not overwritten by empty child fields
                        if current_app_id: temp_row_output["app_id"] = current_app_id
                        if current_spot_id: temp_row_output["spot_id"] = current_spot_id

                        # Process dimensions from unit_data (excluding spotId which is handled above)
                        for fyber_key, output_key in self.DIMENSION_MAPPING.items():
                            if fyber_key in unit_data: # Make sure the key actually exists in the unit_data
                                if output_key == "day": # Special handling for date_epoch -> day
                                    temp_row_output["day"] = datetime.fromtimestamp(unit_data[fyber_key]).strftime("%Y-%m-%d")
                                elif output_key not in ["app_id", "spot_id"]: # Explicitly skip if already handled from parent
                                    temp_row_output[output_key] = unit_data[fyber_key]
                            # else: No need for else here, filtering will handle missing ones later


                        # Process metrics from unit_data
                        for fyber_key, output_key in self.METRIC_MAPPING.items():
                            if fyber_key in unit_data:
                                value = unit_data[fyber_key]
                                try:
                                    if output_key in ["impressions", "clicks", "ad_requests"]:
                                        temp_row_output[output_key] = int(float(value))
                                    else:
                                        temp_row_output[output_key] = float(value)
                                except (ValueError, TypeError):
                                    temp_row_output[output_key] = 0.0
                                    logging.warning(f"FyberClient: Failed to convert '{output_key}' value '{value}' from '{fyber_key}' to number. Setting to 0.")
                            else:
                                temp_row_output[output_key] = 0.0

                        # Now, filter temp_row_output based on selected_dimensions and selected_metrics
                        final_row_output = {}
                        all_selected_output_keys = selected_dimensions + selected_metrics

                        for output_key in all_selected_output_keys:
                            if output_key in temp_row_output:
                                final_row_output[output_key] = temp_row_output[output_key]
                            else:
                                if output_key in self.ALL_METRICS:
                                    final_row_output[output_key] = 0.0
                                elif output_key in self.ALL_DIMENSIONS:
                                    final_row_output[output_key] = "N/A"

                        if any(isinstance(v, (int, float)) and v != 0 for k, v in final_row_output.items() if k in self.ALL_METRICS) or \
                           any(v not in ["N/A", "", None] for k, v in final_row_output.items() if k in self.ALL_DIMENSIONS):
                            processed_data.append(final_row_output)
                        else:
                            logging.debug(
                                f"FyberClient: Skipping row with all N/A dimensions and zero metrics after filtering: {json.dumps(final_row_output, indent=2)}")

            logging.info(f"FyberClient: Processed {len(processed_data)} rows.")
            return processed_data

        except requests.exceptions.RequestException as e:
            error_message = f"Error fetching Fyber report: {e}"
            if response is not None:
                error_json = None
                try:
                    error_json = response.json()
                except json.JSONDecodeError:
                    pass

                if response.status_code == 400:
                    error_detail = error_json.get('message', 'Unknown bad request error') if error_json else response.text
                    error_message = f"Fyber API Error (400 - Bad Request): {error_detail}"
                    raise ValueError(error_message)
                elif response.status_code == 401 or response.status_code == 403:
                    error_detail = error_json.get('message', 'Check your OAuth credentials or Publisher ID permissions.') if error_json else response.text
                    raise ConnectionRefusedError(f"Fyber API Error ({response.status_code}): {error_detail}")
                else:
                    error_message += f". Status Code: {response.status_code}. Response Text: {response.text}"
            logging.error(error_message)
            raise ConnectionError(error_message)
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON response from report endpoint: {response.text}")
            raise ValueError(f"Invalid JSON response from report endpoint: {response.text}")
        except Exception as e:
            logging.error(
                f"FyberClient: Critical error during report parsing: {e}. Raw response type: {type(report_raw_data)}. Raw response: {json.dumps(report_raw_data, indent=2) if isinstance(report_raw_data, (dict, list)) else report_raw_data}")
            raise ValueError(f"Failed to parse Fyber report data: {e}")



# --- GamClient Class (FIXED: CSV_HEADER_NORMALIZATION_MAP access and helper method) ---
class GamClient:
    API_VERSION = "v202505"
    TOKEN_URL_BASE = "https://oauth2.googleapis.com/token"
    REPORT_SERVICE_URL = f"https://ads.google.com/apis/ads/publisher/{API_VERSION}/ReportService?wsdl"

    DIMENSION_MAPPING = {
        "DATE": "date",
        "AD_UNIT_NAME": "ad_unit_name",
        "AD_UNIT_ID": "ad_unit_id",
        "PARENT_AD_UNIT_ID": "parent_ad_unit_id",
        "COUNTRY_NAME": "country_name",
        "COUNTRY_CRITERIA_ID": "country_criteria_id",
    }

    METRIC_MAPPING = {
        "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "impressions",
        "TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE": "revenue",
    }

    # Helper for CSV column name normalization (e.g., "Dimension.DATE" -> "DATE")
    CSV_HEADER_NORMALIZATION_MAP = {
        "DIMENSION.DATE": "DATE",
        "DIMENSION.AD_UNIT_NAME": "AD_UNIT_NAME",
        "DIMENSION.AD_UNIT_ID": "AD_UNIT_ID",
        "DIMENSION.PARENT_AD_UNIT_ID": "PARENT_AD_UNIT_ID",
        "DIMENSION.COUNTRY_NAME": "COUNTRY_NAME",
        "DIMENSION.COUNTRY_CRITERIA_ID": "COUNTRY_CRITERIA_ID",
        "COLUMN.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
        "COLUMN.TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE": "TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE",

        # Handle hierarchical ad unit flattening: "Ad unit X", "Ad unit ID X"
        "AD UNIT 1": "AD_UNIT_NAME",
        "AD UNIT ID 1": "AD_UNIT_ID",
        # Add "AD UNIT 2", "AD UNIT ID 2" etc. if needed for multi-level hierarchies
        "AD UNIT 2": "AD_UNIT_NAME",  # This maps hierarchical level 2 to general AD_UNIT_NAME
        "AD UNIT ID 2": "AD_UNIT_ID",  # This maps hierarchical level 2 to general AD_UNIT_ID
        # Be aware that this simplified mapping will prioritize '1' if both '1' and '2' are present
        # and both map to the same output key.
    }

    ALL_REQUESTABLE_DIMENSIONS = list(DIMENSION_MAPPING.values())
    ALL_REQUESTABLE_METRICS = list(METRIC_MAPPING.values())

    def __init__(self, client_id, client_secret, refresh_token, network_code):
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.refresh_token = refresh_token.strip()
        self.network_code = network_code.strip()
        self.access_token = None

    def _get_access_token(self):
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }

        token_url = f"{self.TOKEN_URL_BASE}?{urlencode(params)}"
        logging.info(f"GamClient: Requesting access token from {token_url}")

        try:
            response = requests.post(token_url, data={})
            response.raise_for_status()
            token_data = response.json()
            logging.info(f"GamClient: Access token response received. Status: 200")
            logging.debug(f"GamClient: Token response data: {token_data}")

            if "access_token" in token_data:
                self.access_token = token_data["access_token"]
                return self.access_token
            else:
                raise ValueError(f"Access token not found in response. Response: {token_data}")
        except requests.exceptions.RequestException as e:
            error_message = f"Error fetching GAM access token: {e}"
            if response is not None:
                error_message += f". Status Code: {response.status_code}. Response Text: {response.text}"
            logging.error(error_message)
            raise ConnectionError(error_message)
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON response from token endpoint: {response.text}")
            raise ValueError(
                f"Invalid JSON response from token endpoint: {self.TOKEN_URL_BASE}. Response: {response.text}")

    def _create_report_job_xml(self, start_date_obj, end_date_obj, gam_dimensions, gam_columns):
        # Define XML namespaces
        ns = {"x": "http://schemas.xmlsoap.org/soap/envelope/",
              "v": f"https://www.google.com/apis/ads/publisher/{self.API_VERSION}"}

        # Build the XML structure
        root = ET.Element("{%s}Envelope" % ns["x"])

        # Header
        header = ET.SubElement(root, "{%s}Header" % ns["x"])
        request_header = ET.SubElement(header, "{%s}RequestHeader" % ns["v"])
        ET.SubElement(request_header, "{%s}networkCode" % ns["v"]).text = self.network_code
        ET.SubElement(request_header, "{%s}applicationName" % ns["v"]).text = "ironSource-AdManager-reporting"

        # Body
        body = ET.SubElement(root, "{%s}Body" % ns["x"])
        run_report_job = ET.SubElement(body, "{%s}runReportJob" % ns["v"])
        report_job = ET.SubElement(run_report_job, "{%s}reportJob" % ns["v"])
        ET.SubElement(report_job, "{%s}id" % ns["v"]).text = "0"  # ID is 0 for new jobs

        report_query = ET.SubElement(report_job, "{%s}reportQuery" % ns["v"])

        # Dimensions
        for dim in gam_dimensions:
            ET.SubElement(report_query, "{%s}dimensions" % ns["v"]).text = dim

        ET.SubElement(report_query, "{%s}adUnitView" % ns["v"]).text = "HIERARCHICAL"

        # Columns (Metrics)
        for col in gam_columns:
            ET.SubElement(report_query, "{%s}columns" % ns["v"]).text = col

        # Start Date
        start_date_el = ET.SubElement(report_query, "{%s}startDate" % ns["v"])
        ET.SubElement(start_date_el, "{%s}year" % ns["v"]).text = str(start_date_obj.year)
        ET.SubElement(start_date_el, "{%s}month" % ns["v"]).text = str(start_date_obj.month)
        ET.SubElement(start_date_el, "{%s}day" % ns["v"]).text = str(start_date_obj.day)

        # End Date
        end_date_el = ET.SubElement(report_query, "{%s}endDate" % ns["v"])
        ET.SubElement(end_date_el, "{%s}year" % ns["v"]).text = str(end_date_obj.year)
        ET.SubElement(end_date_el, "{%s}month" % ns["v"]).text = str(end_date_obj.month)
        ET.SubElement(end_date_el, "{%s}day" % ns["v"]).text = str(end_date_obj.day)

        ET.SubElement(report_query, "{%s}dateRangeType" % ns["v"]).text = "CUSTOM_DATE"

        return ET.tostring(root, encoding='unicode', xml_declaration=True)

    def _run_report_job(self, start_date_obj, end_date_obj, gam_dimensions, gam_columns):
        if not self.access_token:
            self._get_access_token()

        xml_payload = self._create_report_job_xml(start_date_obj, end_date_obj, gam_dimensions, gam_columns)

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}'
        }

        logging.info(f"GamClient: Running report job at {self.REPORT_SERVICE_URL}")
        logging.debug(f"GamClient: Report Job XML Payload: {xml_payload}")

        response = requests.post(self.REPORT_SERVICE_URL, headers=headers, data=xml_payload.encode('utf-8'))
        response.raise_for_status()

        logging.info(f"GamClient: Report job response received. Status: {response.status_code}")
        logging.debug(f"GamClient: Report Job Raw Response: {response.text}")

        return response.text

    def _extract_report_job_id(self, xml_response):
        root = ET.fromstring(xml_response)
        # Define namespace explicitly for finding elements
        ns = {"soap": "http://schemas.xmlsoap.org/soap/envelope/",
              "v": f"https://www.google.com/apis/ads/publisher/{self.API_VERSION}"}

        # Find the <id> element within <rval> in the SOAP body
        job_id_element = root.find(".//soap:Body/v:runReportJobResponse/v:rval/v:id", ns)
        if job_id_element is not None:
            return job_id_element.text
        else:
            raise ValueError("GAM: Could not find report job ID in the XML response. Response: " + xml_response)

    def _create_download_url_xml(self, report_job_id):
        ns = {"x": "http://schemas.xmlsoap.org/soap/envelope/",
              "v": f"https://www.google.com/apis/ads/publisher/{self.API_VERSION}"}

        root = ET.Element("{%s}Envelope" % ns["x"])

        # Header
        header = ET.SubElement(root, "{%s}Header" % ns["x"])
        request_header = ET.SubElement(header, "{%s}RequestHeader" % ns["v"])
        ET.SubElement(request_header, "{%s}networkCode" % ns["v"]).text = self.network_code
        ET.SubElement(request_header, "{%s}applicationName" % ns["v"]).text = "ironSource-AdManager-reporting"

        # Body
        body = ET.SubElement(root, "{%s}Body" % ns["x"])
        get_report_download_url = ET.SubElement(body, "{%s}getReportDownloadURL" % ns["v"])
        ET.SubElement(get_report_download_url, "{%s}reportJobId" % ns["v"]).text = report_job_id
        ET.SubElement(get_report_download_url, "{%s}exportFormat" % ns["v"]).text = "CSV_DUMP"

        return ET.tostring(root, encoding='unicode', xml_declaration=True)

    def _get_report_download_url(self, report_job_id):
        """Step 4: Get Report Download URL with Retry Logic"""
        if not self.access_token:
            self._get_access_token()

        xml_payload = self._create_download_url_xml(report_job_id)

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}'
        }

        max_retries = 5
        initial_delay = 5  # seconds
        for i in range(max_retries):
            logging.info(
                f"GamClient: Attempt {i + 1}/{max_retries} to get report download URL for job ID: {report_job_id}")
            logging.debug(f"GamClient: Download URL XML Payload: {xml_payload}")

            try:
                response = requests.post(self.REPORT_SERVICE_URL, headers=headers, data=xml_payload.encode('utf-8'))
                response.raise_for_status()

                logging.info(f"GamClient: Download URL response received. Status: {response.status_code}")
                logging.debug(f"GamClient: Download URL Raw Response: {response.text}")

                root = ET.fromstring(response.text)
                ns = {"soap": "http://schemas.xmlsoap.org/soap/envelope/",
                      "v": f"https://www.google.com/apis/ads/publisher/{self.API_VERSION}"}

                download_url_element = root.find(".//soap:Body/v:getReportDownloadURLResponse/v:rval", ns)
                if download_url_element is not None:
                    raw_url = download_url_element.text
                    cleaned_url = raw_url.replace("&amp;", "&")
                    return cleaned_url
                else:
                    raise ValueError(
                        "GAM: Could not find report download URL in the XML response. Response: " + response.text)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code >= 500 and i < max_retries - 1:
                    delay = initial_delay * (2 ** i)
                    logging.warning(
                        f"GAM: Server error ({e.response.status_code}) for download URL. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise e
            except Exception as e:
                logging.error(
                    f"GAM: Error getting download URL: {e}. Raw response: {response.text if response else 'No response'}")
                raise e

        raise ConnectionError(f"GAM: Failed to get report download URL after {max_retries} retries.")

    def _download_and_parse_csv(self, download_url):
        """
        Step 5: Download CSV and Parse
        Returns: (list of header strings, list of dicts with original GAM CSV header names as keys)
        """
        logging.info(f"GamClient: Downloading CSV from: {download_url}")
        response = requests.get(download_url)
        response.raise_for_status()  # Raise for HTTP errors

        logging.info(f"GamClient: CSV download successful. Parsing data.")

        csv_bytes = response.content  # Get raw bytes

        try:
            # Check if content is gzipped by looking at magic bytes (0x1f 0x8b)
            if csv_bytes and len(csv_bytes) >= 2 and csv_bytes[0] == 0x1f and csv_bytes[1] == 0x8b:
                decompressed_content = gzip.decompress(csv_bytes)
                csv_data_string = decompressed_content.decode('utf-8')
                logging.debug("GAM CSV: Content decompressed successfully.")
            else:
                # If not gzipped, just decode directly (assuming UTF-8)
                csv_data_string = csv_bytes.decode('utf-8')
                logging.debug("GAM CSV: Content not gzipped, decoded directly.")
        except Exception as e:
            logging.error(f"GAM CSV: Error during decompression/decoding: {e}. Falling back to response.text.")
            csv_data_string = response.text

            # Remove null bytes, as they cause csv.reader issues
        csv_data_string = csv_data_string.replace('\x00', '')

        logging.debug(f"GAM CSV Raw String (first 500 chars):\n{csv_data_string[:500]}")

        # Use csv.DictReader as it directly gives us dictionaries using the header names.
        # It handles `newline=''` implicitly when given a string via `io.StringIO`.
        # The key is to ensure the first row correctly represents the headers.

        # Gam CSVs can have metadata before the actual header.
        # We need to find the actual header line robustly.

        # Use csv.reader first to find the header row and then csv.DictReader.
        # This approach ensures that the header detection is separate and flexible.

        # Split CSV string into lines to find header manually
        lines = csv_data_string.strip().splitlines()

        header_row = []
        data_rows_for_dictreader = []

        found_header = False
        start_data_idx = 0  # Index from which actual data rows begin

        for row_idx, line in enumerate(lines):
            row_cells = [cell.strip() for cell in next(csv.reader(io.StringIO(line)))]  # Parse line to cells

            if not row_cells or all(not cell for cell in row_cells):  # Skip empty lines
                continue

            is_total_row = (row_cells[0].strip().lower() == "total") if row_cells else False

            if is_total_row:
                logging.debug(f"GAM CSV: Skipping total row during header search: {line}")
                continue

            if not found_header:
                # Normalize potential header cells from CSV to match our CSV_HEADER_NORMALIZATION_MAP keys
                normalized_potential_header_cells = [
                    self._normalize_csv_header_name_for_check(cell.strip()) for cell in row_cells if cell.strip()
                ]

                # Check if this row is a header by matching against our CSV_HEADER_NORMALIZATION_MAP keys
                is_header_candidate = any(
                    cell in self.CSV_HEADER_NORMALIZATION_MAP for cell in normalized_potential_header_cells)

                if is_header_candidate:
                    header_row = [cell.strip() for cell in row_cells]  # Keep raw header names as they appear in CSV
                    found_header = True
                    start_data_idx = row_idx + 1  # Data starts from the next line
                    logging.debug(f"GAM CSV: Identified header at line {row_idx}: {header_row}")
                    # Break here as we found the header, rest of lines are data or post-data totals
                    break  # Break from this for-loop
                else:
                    logging.debug(f"GAM CSV: Skipping pre-header/metadata line (not a header candidate): {line}")
                    continue

        if not found_header or not header_row:
            raise ValueError(
                "GAM CSV: Could not identify header row in the downloaded CSV data. CSV content snippet: " + csv_data_string[
                                                                                                             :500])

        # Now, process actual data rows starting from start_data_idx
        # We will use the identified header_row and remaining lines.

        # Reconstruct the string for DictReader, only with header and data lines
        csv_data_for_dictreader = "\n".join([",".join(header_row)] + lines[start_data_idx:])

        # Use DictReader to get list of dictionaries directly
        reader = csv.DictReader(io.StringIO(csv_data_for_dictreader))

        processed_records_as_dicts = []
        for row_dict_raw in reader:
            # DictReader gives us OrderedDict or dict, keys are raw headers.
            # We need to convert relevant values from string to number and handle "N/A"
            final_row_dict = {}
            for raw_header_name, value in row_dict_raw.items():
                # Skip empty header names if DictReader somehow produced them
                if not raw_header_name.strip():
                    continue

                # Clean and convert value for known metrics
                # This ensures numerical values are numbers, not strings.

                # Check against normalized header names for conversion logic
                normalized_header_name = self._normalize_csv_header_name_for_check(raw_header_name)

                if normalized_header_name == "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS":
                    final_row_dict[raw_header_name] = int(float(value)) if value and value.replace('.', '',
                                                                                                   1).isdigit() else 0
                elif normalized_header_name == "TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE":
                    final_row_dict[raw_header_name] = float(value) if value and value.replace('.', '',
                                                                                              1).isdigit() else 0.0
                elif value.strip().upper() == "N/A" or value.strip() == "-":
                    final_row_dict[raw_header_name] = "N/A"
                else:
                    # For other fields, try to convert to number if possible, else keep as string.
                    try:
                        # Attempt conversion to float/int if it looks like a number
                        if value and value.replace('.', '', 1).isdigit():  # Checks for int or float string
                            if '.' in value:
                                final_row_dict[raw_header_name] = float(value)
                            else:
                                final_row_dict[raw_header_name] = int(value)
                        else:
                            final_row_dict[raw_header_name] = value.strip()  # Keep as string, strip whitespace
                    except ValueError:
                        final_row_dict[raw_header_name] = value.strip()  # Fallback to string if conversion fails

            # Skip rows that are empty or total rows (already handled during initial parsing, but double check)
            if not final_row_dict or (
                    header_row[0] in final_row_dict and final_row_dict[header_row[0]].strip().lower() == "total"):
                logging.debug(f"GAM CSV: Skipping empty or total row after DictReader: {final_row_dict}")
                continue

            processed_records_as_dicts.append(final_row_dict)

        return header_row, processed_records_as_dicts

    # Helper method to normalize CSV header names for CHECKING against map keys
    @staticmethod
    def _normalize_csv_header_name_for_check(header_name):
        # This function aims to convert CSV headers like "Dimension.DATE" to "DATE"
        # or "Ad unit 1" to "AD UNIT 1" to match keys in CSV_HEADER_NORMALIZATION_MAP.

        # Strip "Dimension.", "Column." prefixes
        normalized = header_name.upper().replace("DIMENSION.", "").replace("COLUMN.", "").strip()

        # For "Ad unit X" or "Ad unit ID X" where X is a number, we want to generalize.
        # Return the exact string that is a key in CSV_HEADER_NORMALIZATION_MAP.
        # E.g., "AD UNIT 1", "AD UNIT ID 1" are keys in the map.
        # Check for specific patterns like "AD UNIT" followed by digits, or "AD UNIT ID" followed by digits
        if normalized.startswith("AD UNIT "):
            if " ID " in normalized:  # "AD UNIT ID 1"
                return "AD UNIT ID 1"  # We only have map for '1'
            else:  # "AD UNIT 1"
                return "AD UNIT 1"  # We only have map for '1'

        # For other headers, return the normalized version (uppercase, no prefixes).
        return normalized

    def get_report(self, start_date_str, end_date_str, selected_dimensions, selected_metrics, network_code):
        if not self.access_token:
            self._get_access_token()

        start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")

        # Map selected dimensions/metrics (our output keys) to GAM's API query names (e.g., "DATE", "AD_UNIT_NAME")
        gam_dimensions_for_query = [gam_key for gam_key, output_key in self.DIMENSION_MAPPING.items() if
                                    output_key in selected_dimensions]
        gam_columns_for_query = [gam_key for gam_key, output_key in self.METRIC_MAPPING.items() if
                                 output_key in selected_metrics]

        if not gam_dimensions_for_query or not gam_columns_for_query:
            if not gam_dimensions_for_query:
                gam_dimensions_for_query = ["DATE"]
                logging.warning("GAMClient: No dimensions selected for query. Defaulting to 'DATE'.")
            if not gam_columns_for_query:
                gam_columns_for_query = ["TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
                                         "TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE"]
                logging.warning("GAMClient: No metrics selected for query. Defaulting to Impressions and Revenue.")

        report_job_response_xml = self._run_report_job(start_date_obj, end_date_obj, gam_dimensions_for_query,
                                                       gam_columns_for_query)

        report_job_id = self._extract_report_job_id(report_job_response_xml)
        logging.info(f"GamClient: Report Job ID: {report_job_id}")

        download_url = self._get_report_download_url(report_job_id)
        logging.info(f"GamClient: Report Download URL: {download_url}")

        # This now returns (header_list, data_list_of_dicts_with_original_headers)
        csv_header, csv_data_dicts = self._download_and_parse_csv(download_url)

        # No more filtering here. The output is the raw CSV structure with original headers.
        return csv_header, csv_data_dicts  # Return header and data as list of dicts with raw headers


# --- InMobiClient Class (FINALIZED based on provided InMobi API calls/responses) ---
class InMobiClient:
    SESSION_CREATE_URL = "https://api.inmobi.com/v1.0/generatesession/generate"
    REPORT_API_URL = "https://api.inmobi.com/v3.0/reporting/publisher"

    INMOBI_RESPONSE_DIMENSIONS = [
        "country", "countryId", "placementId", "placementName"
    ]
    INMOBI_RESPONSE_METRICS = [
        "earnings"
    ]

    ALL_REQUESTABLE_DIMENSIONS = ["country", "placement"]  # 'placement' is for groupBy field
    ALL_REQUESTABLE_METRICS = ["earnings"]

    # Removed password from __init__ as it's not in the provided API calls.
    def __init__(self, username, secret_key):
        self.username = username.strip()
        self.secret_key = secret_key.strip()
        self.session_id = None
        self.account_id = None

    def _create_session(self):
        """Step 1: Create Session and get Session ID / Account ID"""
        headers = {
            "userName": self.username,
            "secretKey": self.secret_key,  # API Key goes here
            "Accept": "application/json"
        }

        logging.info(f"InMobiClient: Requesting session from {self.SESSION_CREATE_URL}")
        logging.debug(f"InMobiClient: Session request headers: {headers}")

        response = None
        try:
            response = requests.request("GET", self.SESSION_CREATE_URL, headers=headers)
            response.raise_for_status()
            session_data = response.json()
            logging.info(f"InMobiClient: Session creation response received. Status: {response.status_code}")
            logging.debug(f"InMobiClient: Session data: {session_data}")

            if "respList" in session_data and isinstance(session_data["respList"], list) and session_data["respList"]:
                first_resp_item = session_data["respList"][0]
                self.session_id = first_resp_item.get("sessionId")
                self.account_id = first_resp_item.get("accountId")

            if not self.session_id or not self.account_id:
                raise ValueError(
                    f"InMobi: Session ID or Account ID not found in session response. Response: {json.dumps(session_data)}")

            logging.info(
                f"InMobiClient: Session created. Session ID: {self.session_id[:10]}..., Account ID: {self.account_id}")
            return True

        except requests.exceptions.RequestException as e:
            error_message = f"Error creating InMobi session: {e}"
            if response is not None:
                error_message += f". Status Code: {response.status_code}. Response Text: {response.text}"
            logging.error(error_message)
            raise ConnectionError(error_message)
        except json.JSONDecodeError:
            logging.error(f"InMobiClient: Invalid JSON response from session endpoint: {response.text}")
            raise ValueError(
                f"Invalid JSON response from session endpoint: {self.SESSION_CREATE_URL}. Response: {response.text}")
        except Exception as e:
            logging.error(f"InMobiClient: Unexpected error during session creation: {e}")
            raise

    def get_report(self, start_date_str, end_date_str, selected_dimensions, selected_metrics,
                   filter_placement_ids=None):
        """Step 2: Request Report"""
        if not self.session_id or not self.account_id:
            logging.info("InMobiClient: No active session. Creating a new one.")
            self._create_session()

        inmobi_metrics_for_request = [m for m in selected_metrics if m in self.ALL_REQUESTABLE_METRICS]
        inmobi_group_by_for_request = [d for d in selected_dimensions if d in self.ALL_REQUESTABLE_DIMENSIONS]

        if not inmobi_metrics_for_request:
            inmobi_metrics_for_request = ["earnings"]
            logging.warning("InMobiClient: No metrics selected for request. Defaulting to 'earnings'.")

        # --- FIX: Ensure 'placement' is in groupBy if placementId filter is used ---
        filter_values_for_payload = []
        if filter_placement_ids:
            try:
                filter_values_for_payload = [int(pid) for pid in filter_placement_ids.split(',') if
                                             pid.strip().isdigit()]
                # If filtering by placementId, 'placement' should be in groupBy for the request.
                if "placement" not in inmobi_group_by_for_request:
                    inmobi_group_by_for_request.append("placement")
                    logging.info("InMobiClient: Added 'placement' to groupBy as placementId filter is active.")
            except ValueError:
                logging.error(
                    f"InMobiClient: Invalid placement IDs provided for filtering: {filter_placement_ids}. Will send empty filterValue list.")

        # The core report request payload (will be wrapped by "reportRequest")
        report_request_payload_content = {
            "metrics": inmobi_metrics_for_request,
            "groupBy": inmobi_group_by_for_request,
            "timeFrame": f"{start_date_str}:{end_date_str}"
        }

        # --- FIX: Always include filterBy, populate if IDs are present ---
        # Add the 'filterBy' object with 'placementId' as filterName
        # and the (possibly empty) list of filter values.
        report_request_payload_content["filterBy"] = {
            "filterName": "placementId",  # This name is fixed as per API example
            "filterValue": filter_values_for_payload
        }
        # --- END FIX ---

        # --- FIX: Wrap the payload content under "reportRequest" key ---
        final_payload_json_body = {
            "reportRequest": report_request_payload_content
        }
        # --- END FIX ---

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'accountId': self.account_id,
            'secretKey': self.secret_key,
            'sessionID': self.session_id
        }

        logging.info(f"InMobiClient: Requesting report from {self.REPORT_API_URL}")
        logging.debug(
            f"InMobiClient: Report payload: {json.dumps(final_payload_json_body, indent=2)}")  # Log the final wrapped payload
        logging.debug(f"InMobiClient: Report headers: {headers}")

        response = None
        try:
            response = requests.request("POST", self.REPORT_API_URL, headers=headers,
                                        json=final_payload_json_body)  # Send the wrapped payload
            response.raise_for_status()
            report_raw_data = response.json()

            logging.info(f"InMobiClient: Report response received. Status: {response.status_code}")
            logging.debug(f"InMobiClient: Full Report raw data: {json.dumps(report_raw_data, indent=2)}")

            processed_data = []

            report_rows = report_raw_data.get("respList", [])

            if not report_rows and isinstance(report_raw_data, dict) and report_raw_data.get("error"):
                error_list = report_raw_data.get("errorList", [])
                if error_list and isinstance(error_list, list) and error_list[0]:
                    error_message_from_api = error_list[0].get("message", "Unknown API error from errorList.")
                    raise ValueError(f"InMobi API Error: {error_message_from_api}")
                else:
                    raise ValueError(f"InMobi API Error: Unknown error. Raw response: {json.dumps(report_raw_data)}")
            elif not report_rows:
                logging.warning("InMobiClient: Report data is empty or not in expected 'respList' key.")
                return []

            for row_data in report_rows:
                if not isinstance(row_data, dict):
                    logging.warning(f"InMobiClient: Skipping non-dict item in report_rows: {row_data}")
                    continue

                processed_row = {}

                for key, value in row_data.items():
                    if key == "earnings":
                        try:
                            processed_row[key] = float(value)
                        except (ValueError, TypeError):
                            processed_row[key] = 0.0
                            logging.warning(
                                f"InMobiClient: Failed to convert '{key}' value '{value}' to number. Setting to 0.")
                    elif key in ["countryId", "placementId"]:
                        try:
                            processed_row[key] = int(float(value))
                        except (ValueError, TypeError):
                            processed_row[key] = 0
                            logging.warning(
                                f"InMobiClient: Failed to convert '{key}' value '{value}' to int. Setting to 0.")
                    else:
                        processed_row[key] = value

                processed_data.append(processed_row)

            logging.info(f"InMobiClient: Processed {len(processed_data)} rows.")
            return processed_data

        except requests.exceptions.RequestException as e:
            error_message = f"Error fetching InMobi report: {e}"
            if response is not None:
                error_json = None
                try:
                    error_json = response.json()
                except json.JSONDecodeError:
                    pass
                if isinstance(error_json, dict) and "error" in error_json:  # InMobi's specific error structure
                    error_detail = error_json.get("errorList", [{"message": "Unknown API error"}])[0].get("message",
                                                                                                          "Unknown error")
                    error_message = f"InMobi API Error ({response.status_code}): {error_detail}"
                else:
                    error_message += f". Status Code: {response.status_code}. Response Text: {response.text}"
            logging.error(error_message)
            raise ConnectionError(error_message)
        except json.JSONDecodeError:
            logging.error(f"InMobiClient: Invalid JSON response from report endpoint: {response.text}")
            raise ValueError(
                f"Invalid JSON response from report endpoint: {self.REPORT_API_URL}. Response: {response.text}")
        except Exception as e:
            logging.error(
                f"InMobiClient: Critical error during report parsing: {e}. Raw response: {response.text if response else 'No response'}")
            raise ValueError(f"Failed to parse InMobi report data: {e}")