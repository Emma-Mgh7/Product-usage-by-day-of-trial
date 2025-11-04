from collections import Counter
from datetime import datetime

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, callback_context, dcc, html

from app import app
from dashboards.aios.datatable_pagesize import DataTableWithPageSizeDD
from dashboards.aios.datepicker import DatePickerAIO
from dashboards.shared.activities import Activity
from dashboards.shared.constants import (
    RELEVANT_TRIAL_ACTIVITIES,
    TABLE_INT_FORMAT,
)
from dashboards.shared.database import DB

PATH = "/dashboards/trials_product_usage"
PRODUCT_LIST = ["BCS", "GRABBER", "EXPORT", "VR"]
DEFAULT_SELECTION_MESSAGE = "Click a bar segment to show the selected product and trial day."


@app.callback(
    Output("free-trials-products-products-graph", "figure"),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "start_date",
    ),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "end_date",
    ),
)
def update_product_graph(start_date: str, end_date: str):
    """Update the Product usage by day of trial graph."""
    # query the database
    # TODO: current logic is not exactly correct; however it is impossible currently 2023-02-13
    # We considers users who are assigned to a free trial, but it is still possible that the users
    # are assigned to other roles, and the activities are created by those roles, but not from
    # free trial. However activites can be only refered by user but not subscription.
    trial_products = DB.fetch_all(
        """
        SELECT date_part(
            'day',
            date_trunc('day', act.created) - date_trunc('day', ost.valid_from)
        )::int AS day_nr,
            split_part(act.activity, '_', 1) AS product_name,
            count(distinct(usr.user_id)) AS act_count
        FROM replication.analytics_organization_subscription_type AS ost
            JOIN replication.analytics_user_license AS ul
                ON ul.organization_subscription_type_id = ost.organization_subscription_type_id
            JOIN replication.analytics_user AS usr
                ON ul.user_id = usr.user_id
            JOIN replication.analytics_user_activity AS act
                ON act.user_id = ul.user_id
        WHERE ost.valid_until > :end_date
            AND ost.valid_from > :start_date
            AND ost.subscription_type_id = 16
            AND act.created BETWEEN :start_date AND :end_date
            AND act.activity = ANY(:relevant_trial_activities)
            AND usr.username NOT LIKE '%@snapaddy%'
        GROUP BY day_nr, product_name
        ORDER BY day_nr;
        """,
        {
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
            "relevant_trial_activities": RELEVANT_TRIAL_ACTIVITIES,
        },
    )

    trial_products = [product for product in trial_products if product["day_nr"] < 14]

    # offset days by one (1 to 14 instead of 0 to 13)
    for product in trial_products:
        product["day_nr"] += 1

    # get all products with at least one activity
    products_with_activities = sorted(set(product["product_name"] for product in trial_products))

    # set up figure
    fig = go.Figure(
        layout={
            "barmode": "stack",
            "hovermode": "x",
            "margin": {"t": 30},
            "transition_duration": 500,
            "xaxis": {"title": "day of the trial", "tickmode": "linear"},
            "yaxis": {"title": "number of active users", "rangemode": "nonnegative"},
        },
    )

    # draw bars of each product (for the 14 days of the trial)
    days = list(range(1, 15))
    for product in products_with_activities:
        # get the number of activities for this product on each day
        day_to_count = {
            item["day_nr"]: item["act_count"]
            for item in trial_products
            if item["product_name"] == product
        }

        # add the trace for this product
        fig.add_trace(
            go.Bar(
                x=days,
                y=[day_to_count.get(day, 0) for day in days],  # fall back to 0 on missing days
                name=product,
            )
        )

    return fig


@app.callback(
    Output("free-trials-products-selection-summary", "children"),
    Input("free-trials-products-products-graph", "clickData"),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "start_date",
    ),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "end_date",
    ),
)
def update_product_selection(click_data: dict | None, _start_date: str | None, _end_date: str | None):
    """Display the selected product/day underneath the product usage chart."""

    triggered_prop = (
        callback_context.triggered[0]["prop_id"] if callback_context.triggered else None
    )
    if triggered_prop != "free-trials-products-products-graph.clickData":
        return DEFAULT_SELECTION_MESSAGE

    if not click_data or "points" not in click_data or not click_data["points"]:
        return DEFAULT_SELECTION_MESSAGE

    point = click_data["points"][0]

    product_name = (
        point.get("data", {}).get("name")
        or point.get("fullData", {}).get("name")
        or "Unknown"
    )

    day_value = point.get("x")
    if isinstance(day_value, (int, float)) and float(day_value).is_integer():
        day_label = f"Day {int(day_value)}"
    elif isinstance(day_value, str):
        try:
            numeric_day = float(day_value)
        except ValueError:
            day_label = f"Day {day_value}"
        else:
            day_label = (
                f"Day {int(numeric_day)}" if numeric_day.is_integer() else f"Day {numeric_day}"
            )
    else:
        day_label = f"Day {day_value}" if day_value is not None else "Day ?"

    return f"Product: {product_name} - {day_label}"


@app.callback(
    Output("free-trials-products-activities-graph", "figure"),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "start_date",
    ),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "end_date",
    ),
)
def update_activity_graph(start_date: str, end_date: str):
    """Update the User activities by day of trial graph."""
    trial_activities = DB.fetch_all(
        """
        SELECT date_part(
            'day',
            date_trunc('day', act.created) - date_trunc('day', ost.valid_from)
        )::int AS day_nr,
            act.activity,
            count(*) AS act_count
        FROM replication.analytics_organization_subscription_type AS ost
            JOIN replication.analytics_user_license AS ul
                ON ul.organization_subscription_type_id = ost.organization_subscription_type_id
            JOIN replication.analytics_user AS usr
                ON ul.user_id = usr.user_id
            JOIN replication.analytics_user_activity AS act
                ON act.user_id = ul.user_id
        WHERE ost.valid_until > :end_date
            AND ost.valid_from > :start_date
            AND ost.subscription_type_id = 16
            AND act.created BETWEEN :start_date AND :end_date
            AND act.activity = ANY(:relevant_trial_activities)
            AND usr.username NOT LIKE '%@snapaddy%'
        GROUP BY day_nr, act.activity
        ORDER BY day_nr;
        """,
        {
            "relevant_trial_activities": RELEVANT_TRIAL_ACTIVITIES,
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
        },
    )

    trial_activities = [activity for activity in trial_activities if activity["day_nr"] < 14]

    # offset days by one (1 to 14 instead of 0 to 13)
    for activity in trial_activities:
        activity["day_nr"] += 1

    # get all activities that occured at least once
    distinct_activities = sorted(set(act["activity"] for act in trial_activities))

    # set up figure
    fig = go.Figure(
        layout={
            "barmode": "stack",
            "hovermode": "x",
            "margin": {"t": 30},
            "transition_duration": 500,
            "xaxis": {"title": "day of the trial", "tickmode": "linear"},
            "yaxis": {"title": "number of activities", "rangemode": "nonnegative"},
        },
    )

    # draw a line for each kind of activity (for the 14 days of the trial)
    days = list(range(1, 15))
    for activity in distinct_activities:
        # get the number of activities on each day
        day_to_count = {
            act["day_nr"]: act["act_count"]
            for act in trial_activities
            if act["activity"] == activity
        }

        # add the trace for this kind of activity
        fig.add_trace(
            go.Scatter(
                x=days,
                y=[day_to_count.get(day, 0) for day in days],  # fall back to 0 on missing days
                name=activity,
            )
        )

    return fig


@app.callback(
    Output("free-trials-products-activities-by-products", "figure"),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "start_date",
    ),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "end_date",
    ),
    Input("activity-constants", "data"),
)
def update_activities_by_products_graph(
    start_date: str, end_date: str, activity_constants: dict[str, list[str]]
):
    """Update the Activities by product graph."""
    activity_constants = Activity(activity_constants)

    condition = (
        activity_constants["assistant_all"]
        + activity_constants["suggestions_all"]
        + activity_constants["social_all"]
    )

    product_counts = DB.fetch_all(
        """
        SELECT split_part(act.activity, '_', 1) AS product,
            count(split_part(act.activity, '_', 1)) AS product_count
        FROM replication.analytics_user_activity AS act
            JOIN replication.analytics_user AS usr
                ON usr.user_id = act.user_id
            JOIN replication.analytics_organization AS org
                ON (act.organization_id = org.organization_id)
            JOIN replication.analytics_organization_subscription_type AS org_sub_type
                ON (org_sub_type.organization_id = org.organization_id)
            JOIN replication.analytics_subscription_type AS sub_type
                ON (org_sub_type.subscription_type_id = sub_type.subscription_type_id)
        WHERE sub_type.name = 'FREE_TRIAL'
            AND NOT EXISTS (
                SELECT tmp
                FROM unnest((:condition)::text[]) AS tmp
                WHERE tmp = act.activity
            )
            AND created BETWEEN :start_date AND :end_date
            AND (act.organization_id = 1 OR usr.username NOT LIKE '%@snapaddy.com')
        GROUP BY product
        ORDER BY product_count DESC;
        """,
        {
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
            "condition": condition,
        },
    )

    # only consider activities that belong to products in our list
    product_counts_filtered = [item for item in product_counts if item["product"] in PRODUCT_LIST]

    # append 'OTHER' category for all remaining activities, summing up their counts
    other_act_sum = sum(
        item["product_count"] for item in product_counts if item["product"] not in PRODUCT_LIST
    )
    product_counts_filtered.append({"product": "OTHER", "product_count": other_act_sum})

    # sort by product (i.e. activity) count
    product_counts_filtered.sort(key=lambda x: x["product_count"])

    # construct figure
    return go.Figure(
        go.Bar(
            x=[act["product_count"] for act in product_counts_filtered],
            y=[act["product"] for act in product_counts_filtered],
            orientation="h",
            textposition="outside",
        ),
        layout={
            "xaxis": {
                "rangemode": "nonnegative",
                "title": "number of activities",
            },
        },
    )


@app.callback(
    Output({"index": "free-trials-products-top-activities-table"}, "data"),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "start_date",
    ),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "end_date",
    ),
    Input("activity-constants", "data"),
)
def update_frequent_act_table(
    start_date: str, end_date: str, activity_constants: dict[str, list[str]]
) -> list[dict[str, str]]:
    """Update the Most frequent trial activities table."""
    activity_constants = Activity(activity_constants)
    # query database
    condition = (
        activity_constants["assistant_all"]
        + activity_constants["suggestions_all"]
        + activity_constants["social_all"]
    )

    return DB.fetch_all(
        """
        SELECT act.activity,
            count(act.activity) AS activity_count,
            count(distinct(act.user_id)) AS user_count
        FROM replication.analytics_user_activity AS act
            JOIN replication.analytics_user AS usr
                ON usr.user_id = act.user_id
            JOIN replication.analytics_organization AS org
                ON (act.organization_id = org.organization_id)
            JOIN replication.analytics_organization_subscription_type AS org_sub_type
                ON (org_sub_type.organization_id = org.organization_id)
            JOIN replication.analytics_subscription_type AS sub_type
                ON (org_sub_type.subscription_type_id = sub_type.subscription_type_id)
        WHERE sub_type.name = 'FREE_TRIAL'
            AND NOT EXISTS (
                SELECT tmp
                FROM unnest((:condition)::text[]) AS tmp
                WHERE tmp = act.activity
            )
            AND act.created BETWEEN :start_date AND :end_date
            AND (act.organization_id = 1 OR usr.username NOT LIKE '%@snapaddy.com')
        GROUP BY act.activity
        ORDER BY activity_count DESC;
        """,
        {
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
            "condition": condition,
        },
    )


@app.callback(
    Output("free-trials-products-product-mix", "figure"),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "start_date",
    ),
    Input(
        DatePickerAIO.IDS.datepicker("free-trials-products-global-datepicker"),
        "end_date",
    ),
    Input("activity-constants", "data"),
)
def update_product_mix_graph(
    start_date: str, end_date: str, activity_constants: dict[str, list[str]]
):
    """Update the Product mix per organization graph."""
    activity_constants = Activity(activity_constants)
    # query database
    condition = (
        activity_constants["assistant_all"]
        + activity_constants["suggestions_all"]
        + activity_constants["social_all"]
    )

    product_mixes = DB.fetch_all(
        """
        SELECT array_agg(distinct(split_part(act.activity, '_', 1))) AS products
        FROM replication.analytics_user_activity AS act
            JOIN replication.analytics_user AS usr
                ON usr.user_id = act.user_id
            JOIN replication.analytics_organization AS org
                ON (act.organization_id = org.organization_id)
            JOIN replication.analytics_organization_subscription_type AS org_sub_type
                ON (org_sub_type.organization_id = org.organization_id)
            JOIN replication.analytics_subscription_type AS sub_type
                ON (org_sub_type.subscription_type_id = sub_type.subscription_type_id)
        WHERE sub_type.name = 'FREE_TRIAL'
            AND NOT EXISTS (
                SELECT tmp
                FROM unnest((:condition)::text[]) AS tmp
                WHERE tmp = act.activity
            )
            AND created BETWEEN :start_date AND :end_date
            AND (act.organization_id = 1 OR usr.username NOT LIKE '%@snapaddy.com')
        GROUP BY act.organization_id;
        """,
        {
            "start_date": datetime.fromisoformat(start_date),
            "end_date": datetime.fromisoformat(end_date),
            "condition": condition,
        },
    )

    # construct a '+'-separated list of all relevant products for each organization
    pre_selection = [
        "+".join(sorted(list(set([product for product in products if product in PRODUCT_LIST]))))
        for products in (mix["products"] for mix in product_mixes)
    ]

    # count the different mixes
    counter = Counter(pre_selection)
    counter.pop("")  # remove empty entries, formerly `None`
    mixes, counts = zip(*counter.most_common(), strict=False)

    # construct figure
    return go.Figure(
        data=go.Bar(x=counts, y=mixes, orientation="h", textposition="outside"),
        layout={
            "margin": {"t": 30},
            "xaxis": {"rangemode": "nonnegative", "title": "number of organizations"},
            "yaxis": {"autorange": "reversed"},
        },
    )


def get_layout() -> html.Div:
    """Construct the basic layout of the Free Trials dashboard.

    Returns
    -------
    html.Div
        container element that contains the Free Trials view
    """
    return html.Div(
        [
            html.H2("Free Trials Product Usage"),
            html.Hr(),
            dbc.Row(
                dbc.Col(
                    DatePickerAIO(
                        aio_id="free-trials-products-global-datepicker",
                        show_7_days_button=False,
                        show_30_days_button=True,
                        show_90_days_button=True,
                        default_interval=30,
                    )
                )
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("Product usage by day of trial"),
                        html.P(
                            "Number of unique active users (by product and day of their trial)."
                        ),
                        dcc.Loading(dcc.Graph(id="free-trials-products-products-graph")),
                        html.P(
                            DEFAULT_SELECTION_MESSAGE,
                            id="free-trials-products-selection-summary",
                            className="text-muted mt-2",
                        ),
                    ],
                ),
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("User activities by day of trial"),
                        html.P(
                            "Number of activities by trial users (at each day of their trial)."
                        ),
                        dcc.Loading(dcc.Graph(id="free-trials-products-activities-graph")),
                    ]
                ),
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("Activities by product"),
                        html.P(
                            """Number of activities by trial users, grouped by products (excluding
                               activities related to email or social network suggestions).
                               Activities that are not clearly related to a specific product are
                               displayed as 'OTHER'."""
                        ),
                        dcc.Loading(dcc.Graph(id="free-trials-products-activities-by-products")),
                    ]
                )
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("Most frequent trial activities"),
                        html.P(
                            """The most frequent activities of free trial organizations (excluding
                               activities related to email or social network suggestions)."""
                        ),
                        DataTableWithPageSizeDD(
                            id={"index": "free-trials-products-top-activities-table"},
                            columns=[
                                {
                                    "name": "Activity",
                                    "id": "activity",
                                    "type": "text",
                                    "presentation": "markdown",
                                },
                                {
                                    "name": "User Count",
                                    "id": "user_count",
                                    "type": "numeric",
                                    "format": TABLE_INT_FORMAT,
                                },
                                {
                                    "name": "Activity Count",
                                    "id": "activity_count",
                                    "type": "numeric",
                                    "format": TABLE_INT_FORMAT,
                                },
                            ],
                        ),
                    ]
                ),
            ),
            html.Br(),
            dbc.Row(
                dbc.Col(
                    [
                        html.H4("Product mix per organization"),
                        html.P(
                            """Number of trial organizations with certain product mixes in use
                               (excluding activities related to email or social network
                               suggestions)."""
                        ),
                        dcc.Loading(dcc.Graph(id="free-trials-products-product-mix")),
                    ]
                )
            ),
        ]
    )
