from collections import Counter, OrderedDict
from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import callback_context, dcc, html
from dash.dependencies import Input, Output
from plotly.colors import qualitative

from app import app
from dashboards.aios.datatable_pagesize import DataTableWithPageSizeDD
from dashboards.aios.datepicker import DatePickerAIO
from dashboards.shared.activities import Activity
from dashboards.shared.constants import TABLE_INT_FORMAT
from dashboards.shared.database import DB
from dashboards.shared.utils import (
    SnapaddyCiColors,
    beautify_datetime,
    format_keys,
    get_date_k_days_ago,
)

PATH = "/"
SNAPADDY_COLORS = SnapaddyCiColors().snapaddy_colormap


@app.callback(
    Output("home-overall-usage-graph", "figure"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "start_date"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "end_date"),
    Input("home-exclude-snapaddy-checkbox", "value"),
)
def update_overall_usage_graph(start_date: str, end_date: str, excluded_orga_id: list[int]):
    """Update the Active organizations graph."""
    # query the database
    orga_usage_by_day = DB.fetch_all(
        """
        WITH days AS (
            SELECT generate_series(:start_date, :end_date, '1 day'::interval) AS day
        )
        SELECT days.day,
            orgas_by_day.total_orga_count,
            orgas_by_day.cs_orga_count,
            orgas_by_day.vr_orga_count,
            orgas_by_day.dq_orga_count
        FROM days
            LEFT JOIN (
                SELECT count(distinct(act.organization_id)) AS total_orga_count,
                    count(distinct(act.organization_id)) FILTER (
                        WHERE act.activity = 'BCS_CARDS_SCAN_PROCESS'
                    ) AS cs_orga_count,
                    count(distinct(act.organization_id)) FILTER (
                        WHERE act.activity = 'VR_REPORT_START'
                    ) AS vr_orga_count,
                    count(distinct(act.organization_id)) FILTER (
                        WHERE act.activity = 'GRABBER_EXPORT_CRM'
                    ) AS dq_orga_count,
                    date_trunc('day', act.created) AS created_date
                FROM replication.analytics_user_activity AS act
                    JOIN replication.analytics_user AS usr
                        ON act.user_id = usr.user_id
                WHERE act.created BETWEEN :start_date AND :end_date
                    AND (act.organization_id = 1 OR usr.username NOT LIKE '%@snapaddy.com')
                    AND act.activity IN (
                        'BCS_CARDS_SCAN_PROCESS',
                        'VR_REPORT_START',
                        'GRABBER_EXPORT_CRM'
                    )
                    AND act.organization_id <> ALL(:excluded_orga_id)
                GROUP BY created_date
            ) AS orgas_by_day
                ON orgas_by_day.created_date = days.day
        ORDER BY days.day;
        """,
        {
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
            "excluded_orga_id": excluded_orga_id,
        },
    )

    # construct figure
    fig = go.Figure(
        layout={
            "hovermode": "x",
            "yaxis": {"title": "active organizations", "rangemode": "nonnegative"},
            "transition_duration": 500,
            "margin": {"t": 30},
            "height": 300,
            "barmode": "stack",
            "legend": {"traceorder": "normal"},
        }
    )

    # draw bars for DQ usage
    fig.add_trace(
        go.Bar(
            x=[act_org["day"] for act_org in orga_usage_by_day],
            y=[act_org["dq_orga_count"] for act_org in orga_usage_by_day],
            name="DataQuality",
            marker={"color": SNAPADDY_COLORS["green"]},
        )
    )

    # draw bars for BC usage
    fig.add_trace(
        go.Bar(
            x=[act_org["day"] for act_org in orga_usage_by_day],
            y=[act_org["cs_orga_count"] for act_org in orga_usage_by_day],
            name="BusinessCards",
            marker={"color": SNAPADDY_COLORS["light_blue"]},
        )
    )

    # draw bars for VR usage
    fig.add_trace(
        go.Bar(
            x=[act_org["day"] for act_org in orga_usage_by_day],
            y=[act_org["vr_orga_count"] for act_org in orga_usage_by_day],
            name="VisitReport",
            marker={"color": SNAPADDY_COLORS["light_orange"]},
        )
    )

    # draw line for total usage
    fig.add_trace(
        go.Scatter(
            x=[act_org["day"] for act_org in orga_usage_by_day],
            y=[act_org["total_orga_count"] for act_org in orga_usage_by_day],
            name="total",
            mode="lines",
            line_shape="spline",
            line={"color": SNAPADDY_COLORS["grey"]},
            visible="legendonly",
        )
    )

    return fig


@app.callback(
    Output("home-new-trials-graph", "figure"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "start_date"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "end_date"),
    Input("home-exclude-snapaddy-checkbox", "value"),
)
def update_new_trials_graph(start_date: str, end_date: str, excluded_orga_id: list[int]):
    """Update the New trials graph."""
    # count days of period between start and end dates
    days_of_period = (
        datetime.fromisoformat(end_date).date()
        - datetime.fromisoformat(start_date).date()
        - timedelta(days=1)
    )

    # calculate start date of the previous period
    interval_start_date = (datetime.fromisoformat(start_date) - days_of_period).date()

    # query the data for the selected interval and the previous interval at once
    new_trials = DB.fetch_all(
        """
        WITH days AS (
            SELECT generate_series(:start_date, :end_date, '1 day'::interval) AS day
        )
        SELECT days.day,
            count(distinct(orgs.organization_id)) AS new_trials
        FROM days
            LEFT JOIN (
                SELECT organization_id,
                    valid_from
                FROM replication.analytics_organization_subscription_type
                WHERE subscription_type_id = 16
                    AND valid_from BETWEEN :start_date AND :end_date
                    AND organization_id <> ALL(:excluded_orga_id)
            ) AS orgs
                ON date_trunc('day', orgs.valid_from) = days.day
        GROUP BY 1
        ORDER BY days.day;
        """,
        {
            "start_date": interval_start_date,
            "end_date": datetime.fromisoformat(end_date),
            "excluded_orga_id": excluded_orga_id,
        },
    )

    # separate the trial information referring to the previous and the current period
    previous_period = new_trials[: ((len(new_trials) // 2) + 1)]
    current_period = new_trials[(len(new_trials) // 2) :]

    # construct figure
    fig = go.Figure(
        layout={
            "hovermode": "x",
            "yaxis": {"title": "number of new trials", "rangemode": "nonnegative"},
            "transition_duration": 500,
            "margin": {"t": 30},
            "height": 300,
            "legend": {"traceorder": "reversed"},
        }
    )
    # draw line for previous period
    fig.add_trace(
        go.Scatter(
            x=[act_org["day"] for act_org in current_period],
            y=[act_org["new_trials"] for act_org in previous_period],
            text=[act_org["day"].date() for act_org in previous_period],
            hovertemplate="%{text}:<br>%{y} new trials",
            mode="lines",
            line_shape="spline",
            name="previous period",
            fill="tonexty",
            marker_color="lightgray",
        )
    )
    # draw line for current period
    fig.add_trace(
        go.Scatter(
            x=[act_usr["day"] for act_usr in current_period],
            y=[act_usr["new_trials"] for act_usr in current_period],
            text=[act_org["day"].date() for act_org in current_period],
            hovertemplate="%{text}:<br>%{y} new trials",
            mode="lines",
            line_shape="spline",
            name="current period",
        )
    )

    return fig


@app.callback(
    Output({"index": "home-exceed-concurrent-table"}, "data"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "start_date"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "end_date"),
    Input("home-exclude-snapaddy-checkbox", "value"),
)
def update_exceed_concurrent_table(
    start_date: str, end_date: str, excluded_orga_id: list[int]
) -> list[dict]:
    """Update the Organizations exceeding VR concurrent licenses table."""
    concurrent = DB.fetch_all(
        """
        SELECT o.organization_id AS org_id,
            o.name AS org_name,
            ost.max_users,
            count(distinct usl.user_id) AS distinct_users,
            date_trunc('day', act.created)::date AS created_date,
            coalesce(bw.id, substring(hs.billwerkcustomerid, 1, 24)) AS bw_org_id,
            coalesce(bw.customfields ->> 'HubSpotID'::text, hs.id) AS hs_org_id
        FROM replication.analytics_organization_subscription_type AS ost
            JOIN replication.analytics_organization AS o
                ON ost.organization_id = o.organization_id
            JOIN replication.analytics_user_license AS usl
                ON ost.organization_subscription_type_id = usl.organization_subscription_type_id
            JOIN replication.analytics_user AS usr
                ON usl.user_id = usr.user_id
            JOIN replication.analytics_user_activity AS act
                ON usl.user_id = act.user_id
                AND act.organization_id = ost.organization_id
            LEFT JOIN billwerk_customer_profile AS bw
                ON o.billwerk_customer_id = bw.id
            LEFT JOIN hubspot_company_profile AS hs
                ON o.hubspot_company_id = hs.id
        WHERE ost.subscription_type_id = 32
            AND act.created BETWEEN :start_date AND :end_date
            AND act.activity = 'VR_REPORT_START'
            AND usl.unassigned_at IS NULL
            AND (act.organization_id = 1 OR usr.username NOT LIKE '%@snapaddy.com')
            AND act.organization_id <> ALL(:excluded_orga_id)
        GROUP BY date_trunc(
            'day',
            act.created
        ), o.organization_id, o.name, ost.max_users, hs_org_id, bw_org_id
        HAVING count(distinct usl.user_id) > ost.max_users
        ORDER BY created_date DESC;
        """,
        {
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
            "excluded_orga_id": excluded_orga_id,
        },
    )

    # format names into links (via markdown)
    format_keys(concurrent)

    return concurrent


@app.callback(
    Output({"index": "home-most-active-touchless-orgas-table"}, "data"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "start_date"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "end_date"),
    Input("home-exclude-snapaddy-checkbox", "value"),
)
def update_most_active_touchless_orgas_table(
    start_date: str, end_date: str, excluded_orga_id: list[int]
) -> list[dict]:
    """Update the Most active touchless organizations by target activities table."""
    # here SQL Query from BI Data:

    most_active_orgas = DB.fetch_all(
        """
        SELECT o.organization_id AS org_id,
            o.name AS org_name,
            count(*) FILTER (
                WHERE act.activity = 'GRABBER_EXPORT_CRM'
            ) AS dq_act,
            count(*) FILTER (
                WHERE act.activity = 'VR_REPORT_START'
            ) AS vr_act,
            count(*) FILTER (
                WHERE act.activity = 'BCS_CARDS_SCAN_PROCESS'
            ) AS bcs_act,
            count(*) FILTER (
                WHERE act.activity = 'GRABBER_EXPORT_CRM'
            ) + count(*) FILTER (
                WHERE act.activity = 'VR_REPORT_START'
            ) + count(*) FILTER (
                WHERE act.activity = 'BCS_CARDS_SCAN_PROCESS'
            ) AS all_activities_count,
            count(distinct(act.user_id)) AS overall_uniq_users,
            max(act.created) AS last_activity,
            coalesce(bw.customfields ->> 'HubSpotID'::text, hs.id) AS hs_org_id,
            coalesce(bw.id, substring(hs.billwerkcustomerid, 1, 24)) AS bw_org_id
        FROM replication.analytics_user_activity AS act
            JOIN replication.analytics_organization AS o
                ON act.organization_id = o.organization_id
            JOIN replication.analytics_user AS usr
                ON act.user_id = usr.user_id
            JOIN volatile.billwerk_customer_planvariant AS customer
                ON customer.customerid = o.billwerk_customer_id
            LEFT JOIN billwerk_customer_profile AS bw
                ON o.billwerk_customer_id = bw.id
            LEFT JOIN hubspot_company_profile AS hs
                ON o.hubspot_company_id = hs.id
        WHERE act.created BETWEEN :start_date AND :end_date
            AND (act.organization_id = 1 OR usr.username NOT LIKE '%@snapaddy.com')
            AND act.organization_id <> ALL(:excluded_orga_id)
            AND (customer.touchless IS TRUE AND customer.enterprise IS FALSE)
        GROUP BY o.organization_id, o.name, hs_org_id, bw_org_id
        ORDER BY all_activities_count DESC;
        """,
        {
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
            "excluded_orga_id": excluded_orga_id,
        },
    )

    # format names into links (via markdown)
    format_keys(most_active_orgas)
    beautify_datetime(most_active_orgas)

    return most_active_orgas


@app.callback(
    Output({"index": "home-slipping-away-table"}, "data"),
    Input("activity-constants", "data"),
    Input("home-exclude-snapaddy-checkbox", "value"),
    Input("home-pageload", "value"),
)
def update_slipping_away_table(
    activity_constants: dict[str, list[str]], excluded_orga_id: list[int], *_
) -> list[dict[str, str]]:
    """Update the Organizations with DataQuality users slipping away table."""
    activity_constants = Activity(activity_constants)

    slipping_away = DB.fetch_all(
        """
        WITH slipping AS (
            SELECT act.organization_id,
                act.user_id
            FROM replication.analytics_user_activity AS act
                JOIN replication.analytics_user AS usr
                    ON act.user_id = usr.user_id
            WHERE created > :act_date_limit
                AND act.activity = ANY(:grabber_all)
                AND usr.is_deleted = FALSE
                AND (act.organization_id = 1 OR usr.username NOT LIKE '%@snapaddy.com')
                AND act.organization_id <> ALL(:excluded_orga_id)
            GROUP BY act.user_id, act.organization_id
            HAVING max(created) < :no_act_date_limit AND count(*) > 50
        )
        SELECT org.organization_id AS org_id,
            org.name AS org_name,
            count(distinct(slipping.user_id)) AS user_count,
            count(distinct(usr.user_id)) AS total_user_count,
            (
                count(distinct(slipping.user_id))::float / count(distinct(usr.user_id))
            ) * 100 AS slip_dist,
            coalesce(bw.id, substring(hs.billwerkcustomerid, 1, 24)) AS bw_org_id,
            coalesce(bw.customfields ->> 'HubSpotID'::text, hs.id) AS hs_org_id
        FROM replication.analytics_organization AS org
            JOIN slipping
                ON slipping.organization_id = org.organization_id
            JOIN replication.analytics_user AS usr
                ON usr.organization_id = org.organization_id
            LEFT JOIN billwerk_customer_profile AS bw
                ON org.billwerk_customer_id = bw.id
            LEFT JOIN hubspot_company_profile AS hs
                ON org.hubspot_company_id = hs.id
        GROUP BY org.organization_id, org.name, hs_org_id, bw_org_id
        ORDER BY user_count DESC, slip_dist DESC, org_name ASC;
        """,
        {
            "act_date_limit": get_date_k_days_ago(120),
            "no_act_date_limit": get_date_k_days_ago(30),
            "grabber_all": activity_constants["grabber_all"],
            "excluded_orga_id": excluded_orga_id,
        },
    )

    # format names into links (via markdown)
    format_keys(slipping_away)

    return slipping_away


@app.callback(
    Output("home-daily-assigned-licenses-graph", "figure"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "start_date"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "end_date"),
)
def update_daily_assigned_licenses_graph(start_date: str, end_date: str):
    """Update the Recently assigned licenses graph."""
    # query the database
    assigned_licenses_by_day = DB.fetch_all(
        """
        WITH days AS (
            SELECT generate_series(:start_date, :end_date, '1 day'::interval) AS day
        )
        SELECT days.day,
            orgas_by_day.count_all,
            orgas_by_day.count_dq,
            orgas_by_day.count_bcs,
            orgas_by_day.count_vr,
            orgas_by_day.count_enrichment
        FROM days
            LEFT JOIN (
                SELECT date_trunc('day', sul.assigned_at) AS assigned,
                    count(sst.name) AS count_all,
                    count(sst.name) FILTER (
                        WHERE sst.name LIKE 'VISIT%'
                    ) AS count_vr,
                    count(sst.name) FILTER (
                        WHERE sst.name LIKE 'GRABBER%'
                            OR sst.name = 'DATA_QUALITY'
                    ) AS count_dq,
                    count(sst.name) FILTER (
                        WHERE sst.name LIKE 'BUSINESS%'
                            OR sst.name LIKE 'CARD%'
                    ) AS count_bcs,
                    count(sst.name) FILTER (
                        WHERE sst.name = 'DATA_ENRICHMENT'
                    ) AS count_enrichment
                FROM replication.analytics_user_license AS sul
                    LEFT JOIN replication.analytics_organization_subscription_type AS ost
                        ON sul.organization_subscription_type_id = ost.organization_subscription_type_id
                    JOIN replication.analytics_subscription_type AS sst
                        ON ost.subscription_type_id = sst.subscription_type_id
                GROUP BY assigned
            ) AS orgas_by_day
                ON orgas_by_day.assigned = days.day
        ORDER BY days.day;
        """,  # noqa: E501
        {
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
        },
    )

    # construct figure
    fig = go.Figure(
        layout={
            "hovermode": "x",
            "yaxis": {"title": "assigned licenses", "rangemode": "nonnegative"},
            "transition_duration": 500,
            "margin": {"t": 30},
            "height": 300,
            "barmode": "stack",
            "legend": {"traceorder": "normal"},
        }
    )

    # draw bars for assigned DQ licenses
    fig.add_trace(
        go.Bar(
            x=[act_org["day"] for act_org in assigned_licenses_by_day],
            y=[act_org["count_dq"] for act_org in assigned_licenses_by_day],
            name="DataQuality",
            marker={"color": SNAPADDY_COLORS["green"]},
        )
    )

    # draw bars for assigned BC licenses
    fig.add_trace(
        go.Bar(
            x=[act_org["day"] for act_org in assigned_licenses_by_day],
            y=[act_org["count_bcs"] for act_org in assigned_licenses_by_day],
            name="BusinessCards",
            marker={"color": SNAPADDY_COLORS["light_blue"]},
        )
    )

    # draw bars for assigned VR licenses
    fig.add_trace(
        go.Bar(
            x=[act_org["day"] for act_org in assigned_licenses_by_day],
            y=[act_org["count_vr"] for act_org in assigned_licenses_by_day],
            name="VisitReport",
            marker={"color": SNAPADDY_COLORS["light_orange"]},
        )
    )

    # draw bars for assigned Data Enrichment licenses
    fig.add_trace(
        go.Bar(
            x=[act_org["day"] for act_org in assigned_licenses_by_day],
            y=[act_org["count_enrichment"] for act_org in assigned_licenses_by_day],
            name="Data Enrichment",
        )
    )

    # draw line for total assigned licenses
    fig.add_trace(
        go.Scatter(
            x=[act_org["day"] for act_org in assigned_licenses_by_day],
            y=[act_org["count_all"] for act_org in assigned_licenses_by_day],
            name="total",
            mode="lines",
            line_shape="spline",
            line={"color": SNAPADDY_COLORS["grey"]},
            visible="legendonly",
        )
    )

    return fig


@app.callback(
    Output({"index": "home-top-errors-organizations-table"}, "data"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "start_date"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "end_date"),
    Input("activity-constants", "data"),
    Input("home-exclude-snapaddy-checkbox", "value"),
)
def update_latest_activity_table(
    start_date: str,
    end_date: str,
    activity_constants: dict[str, list[str]],
    excluded_orga_id: list[int],
) -> list[dict[str, str]]:
    """Update the Most affected organization for each error type table."""
    activity_constants = Activity(activity_constants)

    org_activities = DB.fetch_all(
        """
        SELECT t.activity,
            t.organization_id AS org_id,
            t.act_count,
            t.last_act,
            t.org_name,
            t.hs_org_id,
            t.bw_org_id
        FROM (
            SELECT act.activity,
                act.organization_id,
                org.name AS org_name,
                count(act.activity) AS act_count,
                max(act.created) AS last_act,
                row_number() OVER (
                    PARTITION BY activity
                    ORDER BY count(act.activity) DESC
                ) AS rank,
                coalesce(bw.id, substring(hs.billwerkcustomerid, 1, 24)) AS bw_org_id,
                coalesce(bw.customfields ->> 'HubSpotID'::text, hs.id) AS hs_org_id
            FROM replication.analytics_user_activity AS act
                JOIN replication.analytics_organization AS org
                    ON act.organization_id = org.organization_id
                JOIN replication.analytics_user AS usr
                    ON act.user_id = usr.user_id
                LEFT JOIN billwerk_customer_profile AS bw
                    ON org.billwerk_customer_id = bw.id
                LEFT JOIN hubspot_company_profile AS hs
                    ON org.hubspot_company_id = hs.id
            WHERE act.created BETWEEN :start_date AND :end_date
                AND act.activity = ANY(:error_all)
                AND act.organization_id <> ALL(:excluded_orga_id)
                AND (act.organization_id = 1 OR usr.username NOT LIKE '%@snapaddy.com')
            GROUP BY activity, act.organization_id, org_name, hs_org_id, bw_org_id
            ORDER BY last_act DESC
        ) t
        WHERE rank = 1
        ORDER BY t.act_count DESC;
        """,
        {
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
            "error_all": activity_constants["error_all"],
            "excluded_orga_id": excluded_orga_id,
        },
    )

    # format names into links (via markdown)
    format_keys(org_activities)
    beautify_datetime(org_activities)

    return org_activities


@app.callback(
    Output("home-errors-graph", "figure"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "start_date"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "end_date"),
    Input("activity-constants", "data"),
    Input("home-exclude-snapaddy-checkbox", "value"),
)
def update_errors_graph(
    start_date: str,
    end_date: str,
    activity_constants: dict[str, list[str]],
    excluded_orga_id: list[int],
):
    """Update the Error overview graph."""
    # count days of period between start and end dates
    activity_constants = Activity(activity_constants)

    days_of_period = (
        datetime.fromisoformat(end_date).date()
        - datetime.fromisoformat(start_date).date()
        - timedelta(days=1)
    )

    # calculate start date of the previous period
    interval_start_date = (datetime.fromisoformat(start_date) - days_of_period).date()

    errors_activities = DB.fetch_all(
        """
        WITH days AS (
            SELECT generate_series(:start_date_int, :end_date, '1 day'::interval) AS day
        )
        SELECT days.day,
            act_usr.user_id,
            act_usr.username,
            act_usr.org_id,
            act_usr.org_name,
            coalesce(act_usr.activity, '') AS activity,
            act_usr.meta,
            act_usr.interval_var,
            act_usr.date,
            act_usr.hs_org_id,
            act_usr.bw_org_id
        FROM days
            LEFT JOIN (
                SELECT usr.user_id,
                    usr.username,
                    org.organization_id AS org_id,
                    org.name AS org_name,
                    act.activity,
                    act.payload ->> 'meta' AS meta,
                    (CASE WHEN act.created > :start_date
                        THEN 1
                        WHEN act.created <= :start_date
                        THEN 0
                    END) AS interval_var,
                    date_trunc('day', act.created) AS "created_date",
                    to_char(act.created, 'YYYY-MM-DD, HH24:MI:SS UTC') AS date,
                    coalesce(bw.id, substring(hs.billwerkcustomerid, 1, 24)) AS bw_org_id,
                    coalesce(bw.customfields ->> 'HubSpotID'::text, hs.id) AS hs_org_id
                FROM replication.analytics_user_activity AS act
                    LEFT JOIN replication.analytics_user AS usr
                        ON act.user_id = usr.user_id
                    LEFT JOIN replication.analytics_organization AS org
                        ON act.organization_id = org.organization_id
                    LEFT JOIN billwerk_customer_profile AS bw
                        ON org.billwerk_customer_id = bw.id
                    LEFT JOIN hubspot_company_profile AS hs
                        ON org.hubspot_company_id = hs.id
                WHERE activity = ANY(:error_all)
                    AND created BETWEEN :start_date_int AND :end_date
                    AND act.organization_id <> ALL(:excluded_orga_id)
                    AND (act.organization_id = 1 OR usr.username NOT LIKE '%@snapaddy.com')
            ) AS act_usr
                ON act_usr.created_date = days.day
        ORDER BY days.day;
        """,
        {
            "start_date_int": interval_start_date,
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
            "error_all": activity_constants["error_all"],
            "excluded_orga_id": excluded_orga_id,
        },
    )

    format_keys(errors_activities)

    # if it is current period, then interval_var is 1, else 0
    counter = Counter([act["activity"] for act in errors_activities if act["interval_var"] == 1])
    counts = dict(sorted(counter.items(), key=lambda kv: kv[1], reverse=True))

    return go.Figure(
        data=go.Bar(
            name="failures",
            x=[count for count in counts.values()],
            y=[name for name in counts],
            text=[count for count in counts.values()],
            customdata=[
                [act for act in errors_activities if act["activity"] == activity_name]
                for activity_name in counts
            ],
            textposition="auto",
            orientation="h",
        ),
        layout={
            "yaxis": {"autorange": "reversed"},
            "xaxis": {"title": "number of errors"},
            "margin": {"t": 35},
            "height": 650,
        },
    )


@app.callback(
    Output({"index": "home-errors-table"}, "data"),
    Output("home-errors-table-heading", "children"),
    Input("home-errors-graph", "clickData"),
)
def display_errors_click_data(fail_click_data):
    """Update the table under the Error overview graph."""
    if fail_click_data and "home-errors-graph.clickData" in callback_context.triggered_prop_ids:
        activities = fail_click_data["points"][0]["customdata"]
        activity_name = activities[0]["activity"]
        activities = [act for act in activities if act["interval_var"] == 1]
        return activities, f"Error: {activity_name}"

    return [], ""


@app.callback(
    Output("home-errors-period-graph", "figure"),
    Output("home-errors-period-graph-heading", "children"),
    Input("home-errors-graph", "clickData"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "start_date"),
    Input(DatePickerAIO.IDS.datepicker("home-global-datepicker"), "end_date"),
)
def display_errors_click_data_graph(fail_click_data, start_date, end_date):
    """Update the graph under the Error overview graph."""
    if fail_click_data and callback_context.triggered_id == "home-errors-graph":
        activities = fail_click_data["points"][0]["customdata"]
        error_name = activities[0]["activity"]

        counts_prev = dict(Counter([act["day"] for act in activities if act["interval_var"] == 0]))
        counts_prev = dict(OrderedDict(sorted(counts_prev.items())))

        counts_current = dict(
            Counter([act["day"] for act in activities if act["interval_var"] == 1])
        )
        counts_current = dict(OrderedDict(sorted(counts_current.items())))

        days_of_period = (
            datetime.fromisoformat(end_date).date()
            - datetime.fromisoformat(start_date).date()
            - timedelta(days=1)
        )

        # make new list with dates from query, including start date and end date for cur period
        d = []
        d.append(datetime.fromisoformat(start_date).date() - timedelta(days=1))

        for key in counts_current:
            d.append(datetime.fromisoformat(key).date())
        d.append(datetime.fromisoformat(end_date).date())

        # calculate missing dates between start and end date
        date_set = set(d[0] + timedelta(x) for x in range((d[-1] - d[0]).days))
        missing = sorted(date_set - set(d))
        missing = [x.strftime("%Y-%m-%d") for x in missing]

        # add them into our dict with 0 value
        missing = dict.fromkeys(missing, 0)
        counts_current = counts_current | missing
        counts_current = {k: counts_current[k] for k in sorted(counts_current)}

        # make new list with dates from query, including start date with unterval and end date
        d = []
        d.append((datetime.fromisoformat(start_date) - days_of_period).date())

        for key in counts_prev:
            d.append(datetime.fromisoformat(key).date())
        d.append(datetime.fromisoformat(start_date).date())

        # calculate missing dates between start and end date
        date_set = set(d[0] + timedelta(x) for x in range((d[-1] - d[0]).days))
        missing = sorted(date_set - set(d))
        missing = [x.strftime("%Y-%m-%d") for x in missing]

        # add them into our dict with 0 value
        missing = dict.fromkeys(missing, 0)
        counts_prev = counts_prev | missing
        counts_prev = {k: counts_prev[k] for k in sorted(counts_prev)}

        # delete first date from current dict and add it into prev dict
        # so length of cur and prev will be similar
        first_date_from_cur = next(iter(counts_current.items()))
        first_date_from_cur = {first_date_from_cur[0]: first_date_from_cur[1]}
        counts_prev = counts_prev | first_date_from_cur
        (k := next(iter(counts_current)), counts_current.pop(k))

        fig = go.Figure(
            layout={
                "hovermode": "x",
                "yaxis": {"title": "number of errors", "rangemode": "nonnegative"},
                "transition_duration": 500,
                "margin": {"t": 30},
                "height": 400,
                "legend": {"traceorder": "reversed"},
            }
        )
        # draw line for previous period
        fig.add_trace(
            go.Scatter(
                y=[count for count in counts_prev.values()],
                x=[name for name in counts_current],
                text=[datetime.fromisoformat(name).date() for name in counts_prev],
                mode="lines",
                line_shape="spline",
                name="previous period",
                fill="tonexty",
                marker_color="#DDA0DD",
                hovertemplate="%{text}:<br>%{y} errors",
            )
        )
        # draw line for current period
        fig.add_trace(
            go.Scatter(
                y=[count for count in counts_current.values()],
                x=[name for name in counts_current],
                text=[datetime.fromisoformat(name).date() for name in counts_current],
                mode="lines",
                line_shape="spline",
                name="current period",
                marker_color="purple",
                hovertemplate="%{text}:<br>%{y} errors",
            )
        )

        return fig, f"Error: {error_name}"

    return go.Figure(), ""


def get_layout() -> html.Div:
    """Construct the basic layout of the Home dashboard.

    Returns
    -------
    html.Div
        container element that contains the Home view
    """
    return html.Div(
        [
            html.H2("Overview"),
            html.Hr(),
            dcc.Input(type="hidden", id="home-pageload"),
            dbc.Row(
                dbc.Col(
                    [
                        DatePickerAIO(
                            aio_id="home-global-datepicker",
                            show_7_days_button=True,
                            show_30_days_button=True,
                            show_90_days_button=True,
                            show_365_days_button=False,
                            default_interval=30,
                        ),
                        dbc.Card(
                            dbc.CardBody(
                                dbc.Checklist(
                                    options=[{"label": "exclude snapADDY", "value": 1}],
                                    value=[],
                                    className="text-muted",
                                    id="home-exclude-snapaddy-checkbox",
                                ),
                                style={"padding": "6px 12px"},
                            ),
                            style={"display": "inline-block", "float": "right"},
                        ),
                    ],
                ),
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("Active organizations"),
                        html.P(
                            """Overview of active organizations by day. An organization is
                               considered to be actively using a product if they have at least on
                               user who exported to the CRM (DataQuality), scanned a card
                               (BusinessCards) or started a report (VisitReport)."""
                        ),
                        dcc.Loading(dcc.Graph(id="home-overall-usage-graph")),
                    ]
                ),
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("New trials"),
                        html.P(
                            """Number of free trials started in the selected time period (with the
                               previous time period before for comparison)."""
                        ),
                        dcc.Loading(dcc.Graph(id="home-new-trials-graph")),
                    ]
                ),
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("Most active touchless organizations by target activities"),
                        html.P(
                            """List of all active touchless organizations in the selected time
                                period by number of events for each target activities.
                                Overall distinct users are not only target activities users.
                                The target activities for each product are defined as follows:"""
                        ),
                        dcc.Markdown(
                            "* DataQuality: `GRABBER_EXPORT_CRM`\n"
                            "* BusinessCards: `BCS_CARDS_SCAN_PROCESS`\n"
                            "* VisitReport: `VR_REPORT_START`"
                        ),
                        DataTableWithPageSizeDD(
                            id={"index": "home-most-active-touchless-orgas-table"},
                            columns=[
                                {
                                    "name": "organization",
                                    "id": "org_name",
                                    "type": "text",
                                    "presentation": "markdown",
                                },
                                {
                                    "name": "vr count",
                                    "id": "vr_act",
                                    "type": "numeric",
                                    "format": TABLE_INT_FORMAT,
                                },
                                {
                                    "name": "dq count",
                                    "id": "dq_act",
                                    "type": "numeric",
                                    "format": TABLE_INT_FORMAT,
                                },
                                {
                                    "name": "bcs count",
                                    "id": "bcs_act",
                                    "type": "numeric",
                                    "format": TABLE_INT_FORMAT,
                                },
                                {
                                    "name": "count target activities",
                                    "id": "all_activities_count",
                                    "type": "numeric",
                                    "format": TABLE_INT_FORMAT,
                                },
                                {
                                    "name": "overall distinct users",
                                    "id": "overall_uniq_users",
                                    "type": "numeric",
                                    "format": TABLE_INT_FORMAT,
                                },
                                {
                                    "name": "last activity",
                                    "id": "last_activity",
                                    "type": "datetime",
                                },
                            ],
                        ),
                    ]
                )
            ),
            html.Br(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H4("Organizations exceeding VR concurrent licenses"),
                            html.P(
                                """Table with organizations that have been exceeding their
                                   VisitReport concurrent licenses in the selected time period."""
                            ),
                            DataTableWithPageSizeDD(
                                id={"index": "home-exceed-concurrent-table"},
                                columns=[
                                    {
                                        "name": "organization",
                                        "id": "org_name",
                                        "type": "text",
                                        "presentation": "markdown",
                                    },
                                    {
                                        "name": "licensed users",
                                        "id": "max_users",
                                        "type": "numeric",
                                        "format": TABLE_INT_FORMAT,
                                    },
                                    {
                                        "name": "actual users",
                                        "id": "distinct_users",
                                        "type": "numeric",
                                        "format": TABLE_INT_FORMAT,
                                    },
                                    {"name": "date", "id": "created_date"},
                                ],
                                style_data_conditional=[
                                    {
                                        "if": {"column_id": "distinct_users"},
                                        "backgroundColor": qualitative.Plotly[9],
                                    },
                                ],
                            ),
                        ]
                    ),
                    dbc.Col(
                        [
                            html.H4("Organizations with DataQuality users slipping away"),
                            html.P(
                                """Organizations with at least one DataQuality user slipping away.
                                   The severity column represents the ratio between number of
                                   slipping-away users and number of total users. It is divided
                                   into 3 groups by different colors:"""
                            ),
                            dcc.Markdown(
                                "* 🔴: More than **10%**\n"
                                "* 🟡: Between **5% - 10%**\n"
                                "* 🟢: Less than **5%**"
                            ),
                            DataTableWithPageSizeDD(
                                id={"index": "home-slipping-away-table"},
                                columns=[
                                    {
                                        "name": "organization",
                                        "id": "org_name",
                                        "type": "text",
                                        "presentation": "markdown",
                                    },
                                    {"name": "slipping-away users", "id": "user_count"},
                                    {"name": "total users", "id": "total_user_count"},
                                    {
                                        "name": "severity",
                                        "id": "slip_dist",
                                        "type": "numeric",
                                        "format": {
                                            "specifier": "$,.1f",
                                            "locale": {"symbol": ["", "%"]},
                                        },
                                    },
                                ],
                                style_data_conditional=[
                                    {
                                        "if": {
                                            "column_id": "slip_dist",
                                            "filter_query": "{slip_dist} >= 10",
                                        },
                                        "backgroundColor": qualitative.Set2[1],
                                    },
                                    {
                                        "if": {
                                            "column_id": "slip_dist",
                                            "filter_query": "{slip_dist} >= 5 && {slip_dist} < 10",
                                        },
                                        "backgroundColor": qualitative.Plotly[9],
                                    },
                                    {
                                        "if": {
                                            "column_id": "slip_dist",
                                            "filter_query": "{slip_dist} < 5",
                                        },
                                        "backgroundColor": qualitative.Set2[4],
                                    },
                                ],
                            ),
                        ]
                    ),
                ],
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("Recently assigned licenses"),
                        html.P("Number of licenses newly assigned to users by day."),
                        dcc.Loading(dcc.Graph(id="home-daily-assigned-licenses-graph")),
                    ]
                ),
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("Most affected organization for each error type"),
                        html.P(
                            """List of all errors that occurred in the selected period of time,
                               each together with the organization that encountered it the most."""
                        ),
                        DataTableWithPageSizeDD(
                            id={"index": "home-top-errors-organizations-table"},
                            columns=[
                                {
                                    "name": "error",
                                    "id": "activity",
                                    "type": "text",
                                },
                                {
                                    "name": "organization",
                                    "id": "org_name",
                                    "type": "text",
                                    "presentation": "markdown",
                                },
                                {
                                    "name": "error count",
                                    "id": "act_count",
                                    "type": "numeric",
                                    "format": TABLE_INT_FORMAT,
                                },
                                {
                                    "name": "last error",
                                    "id": "last_act",
                                    "type": "datetime",
                                },
                            ],
                        ),
                    ]
                )
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("Error overview"),
                        html.P(
                            """All errors that occurred during the selected time period (with
                               detail information in the table below)."""
                        ),
                        dcc.Loading(dcc.Graph(id="home-errors-graph")),
                    ],
                ),
            ),
            html.Br(),
            html.H5(
                id="home-errors-table-heading",
                className="table-heading",
            ),
            DataTableWithPageSizeDD(
                id={"index": "home-errors-table"},
                columns=[
                    {
                        "name": "organization",
                        "id": "org_name",
                        "presentation": "markdown",
                    },
                    {"name": "username", "id": "username", "presentation": "markdown"},
                    {"name": "error", "id": "activity"},
                    {"name": "meta information", "id": "meta"},
                    {"name": "timestamp", "id": "date"},
                ],
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H5(
                            id="home-errors-period-graph-heading",
                            className="table-heading",
                        ),
                        html.P(
                            """Number of errors occurred during the selected time period with the
                               previous time period before for comparison (click on any
                               bar in error overview to see graph)."""
                        ),
                        dcc.Loading(dcc.Graph(id="home-errors-period-graph")),
                    ]
                ),
            ),
        ]
    )
