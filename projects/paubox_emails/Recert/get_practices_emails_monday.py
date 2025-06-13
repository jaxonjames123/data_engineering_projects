import json
import os
import requests

# Jaxon API Key
api_key = os.environ.get('MONDAY_API_KEY')
api_url = os.environ.get('MONDAY_API_URL')
headers = {"Authorization": api_key, "API-Version": "2024-04"}
board_id = "6603518030"
monday_dictionary = {}
# dummy board
# board_id = "8212334932"


def process_items(monday_items, monday_dictionary):
    for item in monday_items:
        item_id = item["id"]
        column_values = item["column_values"]
        item_data = {
            # "TIN": column_values[0]["text"],
            # "Email": column_values[1]["text"],
            # "CAC": column_values[2]["text"],
            # "Active Email?": column_values[3]["text"],
            # "Notes": column_values[4]["text"],
            "TIN": column_values[1]["text"],
            "Email": column_values[2]["text"],
            "CAC": column_values[4]["text"],
            "Active Email?": column_values[3]["text"],
            "Notes": column_values[5]["text"],
        }
        if item_id in monday_dictionary:
            monday_dictionary[item_id][item["name"]].append(item_data)
        else:
            monday_dictionary[item_id] = {item["name"]: [item_data]}


query = f"""
        query {{
            boards(ids: {board_id}) {{
                items_page(limit: 500) {{
                    cursor
                    items {{
                        id
                        name
                        column_values {{
                            text
                        }}
                    }}
                }}
            }}
        }}
        """

while True:
    response = requests.post(
        url=api_url, json={"query": query}, headers=headers)
    response_data = json.loads(response.text)
    if "data" in response_data and "boards" in response_data["data"]:
        board_data = response_data["data"]["boards"][0]
        if "items_page" in board_data:
            monday_items = board_data["items_page"]["items"]
            process_items(monday_items, monday_dictionary)
            cursor = board_data["items_page"]["cursor"]
            if cursor:
                query = f"""
                        query {{
                            boards(ids: {board_id}) {{
                            items_page(
                                limit: 500
                                cursor: "{cursor}") {{
                                    cursor
                                    items {{
                                        id
                                        name
                                        column_values {{
                                            text
                                        }}
                                    }}
                                }}
                            }}
                        }}
                        """
            else:
                break
        else:
            break
    else:
        break
