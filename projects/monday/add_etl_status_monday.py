import datetime
import json
import os

import requests

items = [
    [
        "Membership",
        "Healthfirst",
        "SOMOS",
        "Inbound",
        "healthfirst_somos_load.sh",
        "Not Received",
    ],
    [
        "Medical Claims",
        "Healthfirst",
        "SOMOS",
        "Inbound",
        "healthfirst_somos_load.sh",
        "Not Received",
    ],
    [
        "Rx Claims",
        "Healthfirst",
        "SOMOS",
        "Inbound",
        "healthfirst_somos_load.sh",
        "Not Received",
    ],
    [
        "Membership",
        "Healthfirst",
        "Corinthian",
        "Inbound",
        "healthfirst_corinthian_load.sh",
        "Not Received",
    ],
    [
        "Medical Claims",
        "Healthfirst",
        "Corinthian",
        "Inbound",
        "healthfirst_corinthian_load.sh",
        "Not Received",
    ],
    [
        "Rx Claims",
        "Healthfirst",
        "Corinthian",
        "Inbound",
        "healthfirst_corinthian_load.sh",
        "Not Received",
    ],
    [
        "Membership",
        "Healthfirst",
        "Excelsior",
        "Inbound",
        "healthfirst_excelsior_load.sh",
        "Not Received",
    ],
    [
        "Medical Claims",
        "Healthfirst",
        "Excelsior",
        "Inbound",
        "healthfirst_excelsior_load.sh",
        "Not Received",
    ],
    [
        "Rx Claims",
        "Healthfirst",
        "Excelsior",
        "Inbound",
        "healthfirst_excelsior_load.sh",
        "Not Received",
    ],
    [
        "Membership",
        "Elderplan",
        "Balance",
        "Inbound",
        "load_elderplan_roster.sh",
        "Not Received",
    ],
    [
        "Membership",
        "Emblem",
        "SOMOS",
        "Inbound",
        "process_evolent_eligibility.sh",
        "Not Received",
    ],
    [
        "Medical Claims",
        "Emblem",
        "SOMOS",
        "Inbound",
        "process_evolent_claim.sh",
        "Not Received",
    ],
    ["Rx Claims", "Emblem", "SOMOS", "Inbound", "evolent_rx_data.sh", "Not Received"],
    [
        "Membership",
        "Empire (Anthem/Elevance)",
        "SOMOS",
        "Inbound",
        "process_evolent_eligibility.sh",
        "Not Received",
    ],
    [
        "Medical Claims",
        "Empire (Anthem/Elevance)",
        "SOMOS",
        "Inbound",
        "process_evolent_claim.sh",
        "Not Received",
    ],
    [
        "Rx Claims",
        "Empire (Anthem/Elevance)",
        "SOMOS",
        "Inbound",
        "evolent_rx_data.sh",
        "Not Received",
    ],
    [
        "Membership",
        "Empire (Anthem/Elevance)",
        "Balance",
        "Inbound",
        "process_anthem_balance.sh",
        "Not Received",
    ],
    [
        "Medical Claims",
        "Empire (Anthem/Elevance)",
        "Balance",
        "Inbound",
        "process_anthem_balance.sh",
        "Not Received",
    ],
    [
        "Rx Claims",
        "Empire (Anthem/Elevance)",
        "Balance",
        "Inbound",
        "process_anthem_balance.sh",
        "Not Received",
    ],
    ["Membership", "Fidelis", "SOMOS", "Inbound", "fidelis_load.sh", "Not Received"],
    [
        "Medical Claims",
        "Fidelis",
        "SOMOS",
        "Inbound",
        "fidelis_load.sh",
        "Not Received",
    ],
    ["Rx Claims", "Fidelis", "SOMOS", "Inbound", "fidelis_load.sh", "Not Received"],
    ["Membership", "Molina", "SOMOS", "Inbound", "molina_load.sh", "Not Received"],
    ["Medical Claims", "Molina", "SOMOS", "Inbound", "molina_load.sh", "Not Received"],
    ["Rx Claims", "Molina", "SOMOS", "Inbound", "molina_load.sh", "Not Received"],
    ["Membership", "UHC", "SOMOS", "Inbound", "uhc_somos_load.sh", "Not Received"],
    ["Medical Claims", "UHC", "SOMOS", "Inbound", "uhc_somos_load.sh", "Not Received"],
    [
        "Membership",
        "Wellcare",
        "Balance",
        "Inbound",
        "wellcare_load.sh",
        "Not Received",
    ],
    [
        "Medical Claims",
        "Wellcare",
        "Balance",
        "Inbound",
        "wellcare_load.sh",
        "Not Received",
    ],
    ["Rx Claims", "Wellcare", "Balance", "Inbound", "wellcare_load.sh", "Not Received"],
    [
        "Risk Adjustment",
        "Empire (Anthem/Elevance)",
        "Balance",
        "Inbound",
        "anthem_risk_ingest.sh",
        "Not Received",
    ],
    [
        "Risk Adjustment",
        "Emblem",
        "SOMOS, Balance, Corinthian",
        "Inbound",
        "emblem_risk_ingest.sh",
        "Not Received",
    ],
    [
        "Risk Adjustment",
        "Healthfirst",
        "SOMOS, Corinthian, Excelsior",
        "Inbound",
        "healthfirst_risk_ingest.sh",
        "Not Received",
    ],
    [
        "Medication Adherence",
        "Empire (Anthem/Elevance)",
        "Balance",
        "Inbound",
        "anthem_medadh_ingest.sh",
        "Not Received",
    ],
    [
        "Medication Adherence",
        "Emblem",
        "Balance, Corinthian",
        "Inbound",
        "emblem_medadh_ingest.sh",
        "Not Received",
    ],
    [
        "Medication Adherence",
        "Elderplan",
        "Balance",
        "Inbound",
        "elderplan_medadh_ingest.sh",
        "Not Received",
    ],
    [
        "Medication Adherence",
        "Healthfirst",
        "SOMOS, Corinthian, Excelsior",
        "Inbound",
        "hf_medadh_ingest.sh",
        "Not Received",
    ],
    [
        "Medication Adherence",
        "Wellcare",
        "Balance",
        "Inbound",
        "wellcare_medadh_ingest.sh",
        "Not Received",
    ],
    [
        "Membership, Medical Claims, Rx Claims",
        "ACO_MSSP",
        "Balance",
        "Outbound",
        "process_from_aco_to_garage.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Emblem",
        "SOMOS",
        "Outbound",
        "process_emblem_to_eimg.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Emblem",
        "SOMOS",
        "Outbound",
        "process_emblem_to_eimg.sh",
        "Not Yet Sent",
    ],
    [
        "Rx Claims",
        "Emblem",
        "SOMOS",
        "Outbound",
        "process_emblem_to_eimg.sh",
        "Not Yet Sent",
    ],
    ["GIC", "Emblem", "SOMOS", "Outbound", "process_emblem_to_eimg.sh", "Not Yet Sent"],
    [
        "Medical Claims",
        "Emblem",
        "SOMOS",
        "Outbound",
        "process_evolent_claim.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Empire (Anthem/Elevance)",
        "SOMOS",
        "Outbound",
        "process_anthem_to_eimg.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Empire (Anthem/Elevance)",
        "SOMOS",
        "Outbound",
        "process_anthem_to_eimg.sh",
        "Not Yet Sent",
    ],
    [
        "Rx Claims",
        "Empire (Anthem/Elevance)",
        "SOMOS",
        "Outbound",
        "process_anthem_to_eimg.sh",
        "Not Yet Sent",
    ],
    [
        "GIC",
        "Empire (Anthem/Elevance)",
        "SOMOS",
        "Outbound",
        "process_anthem_to_eimg.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Empire (Anthem/Elevance)",
        "SOMOS",
        "Outbound",
        "process_evolent_claim.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Empire (Anthem/Elevance), Emblem",
        "SOMOS",
        "Outbound",
        "eligibility_data_gdw.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Empire (Anthem/Elevance), Emblem",
        "SOMOS",
        "Outbound",
        "somos_to_ehs.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Empire (Anthem/Elevance), Emblem",
        "SOMOS",
        "Outbound",
        "somos_to_ehs.sh",
        "Not Yet Sent",
    ],
    [
        "Rx Claims",
        "Empire (Anthem/Elevance), Emblem",
        "SOMOS",
        "Outbound",
        "somos_to_ehs.sh",
        "Not Yet Sent",
    ],
    [
        "GIC",
        "Empire (Anthem/Elevance), Emblem",
        "SOMOS",
        "Outbound",
        "somos_to_ehs.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Empire (Anthem/Elevance), Emblem",
        "SOMOS",
        "Outbound",
        "somos_to_sbh.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Empire (Anthem/Elevance), Emblem",
        "SOMOS",
        "Outbound",
        "somos_to_sbh.sh",
        "Not Yet Sent",
    ],
    [
        "Rx Claims",
        "Empire (Anthem/Elevance), Emblem",
        "SOMOS",
        "Outbound",
        "somos_to_sbh.sh",
        "Not Yet Sent",
    ],
    [
        "GIC",
        "Empire (Anthem/Elevance), Emblem",
        "SOMOS",
        "Outbound",
        "somos_to_sbh.sh",
        "Not Yet Sent",
    ],
    [
        "Alerts",
        "Empire (Anthem/Elevance), Emblem, Fidelis, Healthfirst",
        "SOMOS",
        "Outbound",
        "process_rhio_to_rapidcare.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Fidelis",
        "SOMOS",
        "Outbound",
        "fidelis_roster_to_bxrhio.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Healthfirst",
        "Corinthian",
        "Outbound",
        "process_healthfirst_corinthian_to_evolent.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Healthfirst",
        "Corinthian",
        "Outbound",
        "process_healthfirst_corinthian_to_evolent.sh",
        "Not Yet Sent",
    ],
    [
        "Rx Claims",
        "Healthfirst",
        "Corinthian",
        "Outbound",
        "process_healthfirst_corinthian_to_evolent.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "process_healthfirst_somos_to_evolent.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "process_healthfirst_somos_to_evolent.sh",
        "Not Yet Sent",
    ],
    [
        "Rx Claims",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "process_healthfirst_somos_to_evolent.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Healthfirst",
        "Corinthian, Excelsior",
        "Outbound",
        "process_healthfirst_legacy_files_to_garage.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Healthfirst",
        "Corinthian, Excelsior",
        "Outbound",
        "process_healthfirst_legacy_files_to_garage.sh",
        "Not Yet Sent",
    ],
    [
        "Rx Claims",
        "Healthfirst",
        "Corinthian, Excelsior",
        "Outbound",
        "process_healthfirst_legacy_files_to_garage.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "process_healthfirst_somos_to_garage.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "process_healthfirst_somos_to_garage.sh",
        "Not Yet Sent",
    ],
    [
        "Rx Claims",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "process_healthfirst_somos_to_garage.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "process_hf_to_eimg.sh",
        "Not Yet Sent",
    ],
    [
        "Medical Claims",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "process_hf_to_eimg.sh",
        "Not Yet Sent",
    ],
    [
        "Rx Claims",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "process_hf_to_eimg.sh",
        "Not Yet Sent",
    ],
    [
        "Alerts",
        "NextGen",
        "Hospitals",
        "Outbound",
        "nextgenenter_to_garage.sh",
        "Not Yet Sent",
    ],
    [
        "EMR",
        "NextGen",
        "Hospitals",
        "Outbound",
        "nextgenenter_to_garage.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Empire (Anthem/Elevance)",
        "SOMOS",
        "Outbound",
        "anthem_roster_to_bxrhio.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Emblem",
        "SOMOS",
        "Outbound",
        "emblem_roster_to_bxrhio.sh",
        "Not Yet Sent",
    ],
    [
        "Membership",
        "Healthfirst",
        "SOMOS",
        "Outbound",
        "hf_roster_to_bxrhio.sh",
        "Not Yet Sent",
    ],
    # ['GIC','UHC','SOMOS','Inbound'],
    # ['GIC','VNS','Balance','Inbound'],
    # ['GIC','Wellcare','Balance','Inbound'],
    # ['GIC','UHC','Molina','Inbound'],
    # ['GIC','Humana','SOMOS','Inbound'],
    # ['GIC','Fidelis','SOMOS','Inbound'],
    # ['GIC','Empire (Anthem/Elevance)','Balance','Inbound'],
    # ['GIC','Empire (Anthem/Elevance)','SOMOS','Inbound'],
    # ['GIC','Emblem','Balance','Inbound'],
    # ['GIC','Emblem','SOMOS','Inbound'],
    # ['GIC','Elderplan','Balance','Inbound'],
    # ['GIC','Healthfirst','Excelsior','Inbound'],
    # ['GIC','Healthfirst','Corinthian','Inbound'],
    # ['GIC','Healthfirst','SOMOS','Inbound'],
]


apiKey = os.environ.get("$MONDAY_API_KEY")
apiUrl = os.environ.get("MONDAY_API_URL")
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
curr_month = datetime.datetime.now().strftime("%Y%m")


def query():
    for item in items:
        mcount = 0
        item_name = item[0]
        mco = item[1]
        ipa = item[2]
        feed_direction = item[3]
        script_name = item[4]
        for_month = item[5]
        process_name = item[0].replace(",", "_" + curr_month + ",") + "_" + curr_month
        exec_date = datetime.datetime.now().strftime("%Y-%m-%d")
        status = "Pending"
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
                    print(
                        "Item already exists: "
                        + process_name
                        + ", "
                        + mco
                        + ", "
                        + ipa
                        + ", "
                        + feed_direction
                        + ", "
                        + script_name
                    )
                    mcount += 1

        if mcount == 0:
            print(mcount)
            print(
                "Adding : "
                + process_name
                + " , "
                + mco
                + " , "
                + ipa
                + " , "
                + feed_direction
                + " , "
                + script_name
                + " , "
                + for_month
            )
            add_new(
                item_name,
                process_name,
                mco,
                ipa,
                feed_direction,
                exec_date,
                status,
                script_name,
                for_month,
            )


def add_new(
    item_name,
    process_name,
    mco,
    ipa,
    feed_direction,
    exec_date,
    status,
    script_name,
    for_month,
):
    upd1 = (
        'mutation { create_item (board_id: 4513137397, group_id: "topics", item_name: "'
        + item_name
        + '", column_values: "{ \\"date0\\":\\"'
        + exec_date
        + '\\" ,\\"text\\":\\"'
        + process_name
        + '\\", \\"status6\\":\\"'
        + status
        + '\\",\\"dropdown5\\":\\"'
        + mco
        + '\\",\\"dropdown53\\":\\"'
        + ipa
        + '\\",\\"feed_type3\\":\\"'
        + item_name
        + '\\",\\"label\\":\\"'
        + feed_direction
        + '\\",\\"text6\\":\\"'
        + script_name
        + '\\",\\"text69\\":\\"'
        + for_month
        + '\\"}") {  id   }}'
    )
    data = {"query": upd1}
    r = requests.post(url=apiUrl, json=data, headers=headers)
    response_data = json.loads(r.text)
    return response_data


def main():
    matched_values = query()


if __name__ == "__main__":
    main()
