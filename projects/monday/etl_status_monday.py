import datetime
import json
import os
import sys

import requests

process_name = sys.argv[1]
status = sys.argv[2]
mco = sys.argv[3]
ipa = sys.argv[4]
feed_type = sys.argv[5]
record_count = sys.argv[6]
feed_direction = sys.argv[7]
script_name = sys.argv[8]
for_month = sys.argv[9]

apiKey = os.environ.get("$MONDAY_API_KEY")
apiUrl = os.environ.get("MONDAY_API_URL")
headers_outbound = {"Authorization": apiKey, "API-Version": "2023-10"}
headers = {"Authorization": apiKey, "API-Version": "2023-10"}
column_labels = {
    "text": "process_name",
    "date0": "execution_date",
    "status6": "status",
    "dropdown5": "mco",
    "dropdown53": "ipa",
    "dropdown4": "lob",
    "feed_type3": "feed_type",
    "numbers": "record_count",
    "label": "feed_direction",
    "text6": "script_name",
    "text69": "for_month",
}


def query():
    if feed_direction == "Inbound":
        query1 = (
            'query {items_page_by_column_values (board_id: 4513137397, columns: [{column_id: "text", column_values: ["'
            + process_name
            + '"]}]) { items { id name }}}'
        )
        data = {"query": query1}
        r = requests.post(url=apiUrl, json=data, headers=headers)
        response_data1 = json.loads(r.text)
        if len(response_data1["data"]["items_page_by_column_values"]) > 0:
            for response_row in response_data1["data"]["items_page_by_column_values"][
                "items"
            ]:
                item_id1 = response_row["id"]
                item_name = response_row["name"]
                q2 = (
                    "query { items (ids: "
                    + item_id1
                    + ") { name column_values { id value text } } }"
                )
                data = {"query": q2}
                r = requests.post(url=apiUrl, json=data, headers=headers)
                response_data2 = json.loads(r.text)
                column_values = response_data2["data"]["items"][0]["column_values"]
                item_values = {}
                for column_value in column_values:
                    if column_value["id"] != "item_id":
                        item_values[column_labels[column_value["id"]]] = column_value[
                            "text"
                        ]
                item_values["item_id"] = item_id1
                if (
                    item_values["process_name"] == process_name
                    and item_values["mco"] == mco
                    and item_values["ipa"] == ipa
                    and item_values["feed_direction"] == feed_direction
                    and item_values["script_name"] == script_name
                ):
                    return item_values

    elif feed_direction == "Outbound":
        query = (
            'query { boards (ids: 4513137397) { items_page (limit: 1, query_params: {rules: [{column_id: "text", compare_value: ["'
            + process_name
            + '"]}{column_id: "text6",compare_value: ["'
            + script_name
            + '"]}]}) { items { id name column_values{id text}}}}}'
        )
        data = {"query": query}
        r = requests.post(url=apiUrl, json=data, headers=headers_outbound)
        response_data = json.loads(r.text)
        if response_data["data"]["boards"][0]["items_page"]["items"][0]["id"] != "":
            column_values = response_data["data"]["boards"][0]["items_page"]["items"][
                0
            ]["column_values"]
            item_values = {}
            for column_value in column_values:
                if column_value["id"] != "item_id":
                    item_values[column_labels[column_value["id"]]] = column_value[
                        "text"
                    ]
                if column_value["id"] == "item_id":
                    item_values["item_id"] = column_value["text"]
            if (
                item_values["process_name"] == process_name
                and item_values["mco"] == mco
                and item_values["ipa"] == ipa
                and item_values["feed_direction"] == feed_direction
                and item_values["script_name"] == script_name
            ):
                return item_values


def update(item_id, new_status):
    exec_date = datetime.datetime.now().strftime("%Y-%m-%d")
    if new_status.upper() == "SUCCESS":
        new_status = "Complete"
    elif new_status.upper() == "FAILED" or new_status.upper() == "FAILURE":
        new_status = "Failed"
    upd0 = (
        "mutation { change_multiple_column_values(item_id:"
        + item_id
        + ', board_id: 4513137397, column_values: "{ \\"status6\\":\\"'
        + new_status
        + '\\",\\"date0\\": \\"'
        + exec_date
        + '\\",\\"numbers\\":'
        + record_count
        + ',\\"text69\\":\\"'
        + for_month
        + '\\"}") { id}}'
    )
    data = {"query": upd0}
    r = requests.post(url=apiUrl, json=data, headers=headers)
    response_data = json.loads(r.text)
    return response_data


def main():
    matched_values = query()
    ret_val = update(matched_values["item_id"], status)
    if ret_val:
        print(
            "Return value is: ", ret_val["data"]["change_multiple_column_values"]["id"]
        )


if __name__ == "__main__":
    main()
